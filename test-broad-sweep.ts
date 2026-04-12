import {
  getBoundsAndBalance,
  fetchFull,
  parallelMap,
  getCallCount,
  resetCallCount,
  detectTier,
} from "./src/rpc.js";
import type { SlotRange, BalanceTx, FullTransaction } from "./src/types.js";
import { computeSolBalanceOverTime } from "./src/algorithm.js";
import { reconstructBalance, validateBalance } from "./src/balance.js";

const RPC_URL = process.env.HELIUS_RPC_URL!;
if (!RPC_URL) { console.error("HELIUS_RPC_URL required"); process.exit(1); }

const FULL_LIMIT = 100;

function splitRange(range: SlotRange, factor: number): SlotRange[] {
  const span = range.lte - range.gte + 1;
  if (span <= 1 || factor <= 1) return [range];
  const effectiveFactor = Math.min(factor, span);
  const chunkSize = Math.floor(span / effectiveFactor);
  const ranges: SlotRange[] = [];
  let lo = range.gte;
  for (let i = 0; i < effectiveFactor; i++) {
    const hi = i === effectiveFactor - 1 ? range.lte : lo + chunkSize - 1;
    ranges.push({ gte: lo, lte: hi });
    lo = hi + 1;
  }
  return ranges;
}

function findAddressIndex(tx: FullTransaction, address: string): number {
  const keys = tx.transaction.message.accountKeys;
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if ((typeof key === "string" ? key : key.pubkey) === address) return i;
  }
  const loaded = tx.meta?.loadedAddresses;
  if (!loaded) return -1;
  const staticLen = keys.length;
  const writable = loaded.writable ?? [];
  for (let i = 0; i < writable.length; i++) {
    if (writable[i] === address) return staticLen + i;
  }
  const readonly = loaded.readonly ?? [];
  for (let i = 0; i < readonly.length; i++) {
    if (readonly[i] === address) return staticLen + writable.length + i;
  }
  return -1;
}

function extractBalanceTxs(txs: FullTransaction[], address: string): BalanceTx[] {
  const slim: BalanceTx[] = [];
  for (const tx of txs) {
    if (!tx.meta || !Array.isArray(tx.meta.preBalances) || !Array.isArray(tx.meta.postBalances)) continue;
    const idx = findAddressIndex(tx, address);
    if (idx === -1 || idx >= tx.meta.preBalances.length || idx >= tx.meta.postBalances.length) continue;
    const pre = tx.meta.preBalances[idx];
    const post = tx.meta.postBalances[idx];
    if (pre === post) continue;
    const signature = tx.transaction.signatures[0];
    if (!signature) continue;
    slim.push({
      signature, slot: tx.slot, blockTime: tx.blockTime,
      transactionIndex: Number.isFinite(tx.transactionIndex) ? tx.transactionIndex : 0,
      preBalance: pre, postBalance: post,
    });
  }
  return slim;
}

async function directFetch(
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  chunks: number,
): Promise<{ txs: BalanceTx[]; calls: number; durationMs: number }> {
  resetCallCount();
  const start = performance.now();
  const allTxs: BalanceTx[] = [];
  const ranges = splitRange(fullRange, chunks);

  const results = await parallelMap(ranges, concurrency, async (range) => {
    const rangeTxs: BalanceTx[] = [];
    let token: string | undefined;
    let pages = 0;
    while (pages < 50) {
      const result = await fetchFull(RPC_URL, address, range, token);
      rangeTxs.push(...extractBalanceTxs(result.data, address));
      pages++;
      if (result.data.length < FULL_LIMIT || !result.paginationToken) break;
      token = result.paginationToken;
    }
    return rangeTxs;
  });

  for (const txs of results) allTxs.push(...txs);

  return { txs: allTxs, calls: getCallCount(), durationMs: performance.now() - start };
}

const WALLETS: Record<string, string[]> = {
  xs: [
    'ERKj6Kq7nU5LPgxknzXFAwG1RzUrQ1tmxA89KgZTy5Xn',
    '49zbLGHRSUmL9HXjLjodBZTA4tZT2vgBRSX2jsE55Re1',
  ],
  sm: [
    '7RXwsoQD6MzsUbwGR2fptPCpi9CFnSLdCYJckb4327as',
    'BFXdmgRTSPaxH9nsa7PYFXz6YFu1MEBps6d962svZKxk',
  ],
  md: [
    'Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR',
    '3FpKm9iSie6ug3xcbSPAeTEDGuizEz9CVUyuHn63inXa',
  ],
  lg: [
    'CRqRsrFrEsm4BxU5XP2boWoLds9u91KjkhKSQqdwmUh9',
    '6gwTAyKVg1zH77JWhZceuHFXH3bJz8jaRyrYrHYaXuqz',
  ],
};

async function main() {
  const tier = detectTier();
  const c = tier.concurrency;
  const multipliers = [3, 5, 7, 10];

  console.log(`Tier: ${tier.tier} (${c} concurrency)`);
  console.log(`Testing multipliers: ${multipliers.map(m => `c*${m}=${c*m}`).join(', ')}\n`);

  // Only test wallets with 1000+ txs (where this matters)
  const testWallets: { label: string; address: string }[] = [];
  for (const [size, addrs] of Object.entries(WALLETS)) {
    for (const addr of addrs) {
      testWallets.push({ label: `${size}:${addr.slice(0,8)}`, address: addr });
    }
  }

  console.log("wallet          | txs     | span      | current_ms | " +
    multipliers.map(m => `c*${m}`.padStart(8)).join(" | ") + " | best");
  console.log("-".repeat(120));

  for (const { label, address } of testWallets) {
    const bounds = await getBoundsAndBalance(RPC_URL, address);
    if (!bounds.firstSlot || !bounds.lastSlot) {
      console.log(`${label.padEnd(15)} | no transactions`);
      continue;
    }
    const fullRange: SlotRange = { gte: bounds.firstSlot, lte: bounds.lastSlot };
    const span = fullRange.lte - fullRange.gte;

    // Current algorithm
    resetCallCount();
    const t0 = performance.now();
    const v1 = await computeSolBalanceOverTime({
      address, rpcUrl: RPC_URL,
      initialChunks: c, splitFactor: 6,
      concurrencyLimit: c,
      maxTransactions: tier.maxTransactions,
    });
    const currentMs = Math.round(performance.now() - t0);
    const txCount = v1.transactions.length;

    await new Promise(r => setTimeout(r, 3000));

    // Sweep multipliers
    const results: { mult: number; ms: number; calls: number }[] = [];
    for (const mult of multipliers) {
      const chunks = c * mult;
      const df = await directFetch(address, fullRange, c, chunks);
      results.push({ mult, ms: Math.round(df.durationMs), calls: df.calls });
      await new Promise(r => setTimeout(r, 3000));
    }

    const best = results.reduce((a, b) => a.ms < b.ms ? a : b);
    const bestLabel = best.ms < currentMs ? `c*${best.mult}` : "current";

    console.log(
      `${label.padEnd(15)} | ${String(txCount).padStart(7)} | ${String(span).padStart(9)} | ${String(currentMs).padStart(10)} | ` +
      results.map(r => String(r.ms).padStart(8)).join(" | ") +
      ` | ${bestLabel} (${best.ms < currentMs ? `-${Math.round((1 - best.ms/currentMs) * 100)}%` : "0%"})`
    );
  }
}

main().catch(console.error);

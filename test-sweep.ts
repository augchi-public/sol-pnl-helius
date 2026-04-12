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

  return {
    txs: allTxs,
    calls: getCallCount(),
    durationMs: performance.now() - start,
  };
}

async function main() {
  const address = process.argv[2] || "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR";
  const tier = detectTier();
  const concurrency = tier.concurrency;

  console.log(`Wallet: ${address}`);
  console.log(`Tier: ${tier.tier} (${concurrency} concurrency)\n`);

  const bounds = await getBoundsAndBalance(RPC_URL, address);
  if (!bounds.firstSlot || !bounds.lastSlot) {
    console.log("No transactions found"); return;
  }
  const fullRange: SlotRange = { gte: bounds.firstSlot, lte: bounds.lastSlot };
  console.log(`Slot range: ${fullRange.gte} - ${fullRange.lte} (span: ${fullRange.lte - fullRange.gte})`);
  console.log(`Quick sig count: ${bounds.totalSigCount}\n`);

  // ── Baseline: Current algorithm ──
  console.log("── Current algorithm ──");
  resetCallCount();
  const t1 = performance.now();
  const v1 = await computeSolBalanceOverTime({
    address, rpcUrl: RPC_URL,
    initialChunks: concurrency, splitFactor: 6,
    concurrencyLimit: concurrency,
    maxTransactions: tier.maxTransactions,
  });
  const v1Time = performance.now() - t1;
  console.log(`  ${Math.round(v1Time)}ms | ${v1.totalCalls} calls | ${v1.transactions.length} txs\n`);

  // ── Sweep chunk counts ──
  const chunkCounts = [
    concurrency,           // 185
    concurrency * 2,       // 370
    concurrency * 3,       // 555
    concurrency * 5,       // 925
    concurrency * 7,       // 1295
    concurrency * 10,      // 1850
  ];

  console.log("── Direct fetch sweep ──");
  console.log("chunks | time_ms | calls | txs");
  console.log("-------|---------|-------|----");

  for (const chunks of chunkCounts) {
    await new Promise(r => setTimeout(r, 3000));
    const result = await directFetch(address, fullRange, concurrency, chunks);
    const timeline = reconstructBalance(result.txs, bounds.balance, address);
    const valid = validateBalance(timeline, bounds.balance);
    console.log(
      `${String(chunks).padStart(6)} | ${String(Math.round(result.durationMs)).padStart(7)} | ${String(result.calls).padStart(5)} | ${result.txs.length} ${valid.valid ? "" : "FAIL"}`
    );
  }
}

main().catch(console.error);

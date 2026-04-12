import {
  getBoundsAndBalance,
  fetchFull,
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

// Drain a single range by narrowing lte after each full page
async function drainRange(address: string, range: SlotRange): Promise<{ txs: BalanceTx[]; pages: number }> {
  const txs: BalanceTx[] = [];
  let currentLte = range.lte;
  let pages = 0;

  while (currentLte >= range.gte && pages < 500) {
    const result = await fetchFull(RPC_URL, address, { gte: range.gte, lte: currentLte });
    txs.push(...extractBalanceTxs(result.data, address));
    pages++;

    if (result.data.length < FULL_LIMIT) break;

    const minSlot = Math.min(...result.data.map(tx => tx.slot));
    if (minSlot <= range.gte) break;
    currentLte = minSlot - 1;
  }

  return { txs, pages };
}

// Work-stealing parallel drain: N ranges, C concurrent workers pulling from shared queue
async function parallelDrainWorkStealing(
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  numRanges: number,
): Promise<{ txs: BalanceTx[]; calls: number; durationMs: number; maxPages: number; totalRanges: number }> {
  resetCallCount();
  const start = performance.now();

  const ranges = splitRange(fullRange, numRanges);
  let queueIdx = 0;
  let maxPages = 0;

  const workers = Array.from({ length: Math.min(concurrency, ranges.length) }, async () => {
    const workerTxs: BalanceTx[] = [];
    while (true) {
      const idx = queueIdx++;
      if (idx >= ranges.length) break;
      const { txs, pages } = await drainRange(address, ranges[idx]);
      workerTxs.push(...txs);
      if (pages > maxPages) maxPages = pages;
    }
    return workerTxs;
  });

  const results = await Promise.all(workers);
  const allTxs: BalanceTx[] = [];
  for (const txs of results) allTxs.push(...txs);

  return {
    txs: allTxs,
    calls: getCallCount(),
    durationMs: performance.now() - start,
    maxPages,
    totalRanges: ranges.length,
  };
}

async function runBenchmark(
  label: string,
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  numRanges: number,
  expectedBalance: number,
) {
  const result = await parallelDrainWorkStealing(address, fullRange, concurrency, numRanges);
  const timeline = reconstructBalance(result.txs, expectedBalance, address);
  const valid = validateBalance(timeline, expectedBalance);
  console.log(
    `  ${label.padEnd(20)} | ${String(Math.round(result.durationMs)).padStart(7)}ms | ` +
    `${String(result.calls).padStart(5)} calls | ${String(result.txs.length).padStart(7)} txs | ` +
    `maxPages=${String(result.maxPages).padStart(3)} | ${valid.valid ? "PASS" : "FAIL"}`
  );
  return result;
}

async function main() {
  const address = process.argv[2] || "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR";
  const tier = detectTier();
  const c = tier.concurrency;

  console.log(`Wallet: ${address}`);
  console.log(`Tier: ${tier.tier} (${c} concurrency)\n`);

  const bounds = await getBoundsAndBalance(RPC_URL, address);
  if (!bounds.firstSlot || !bounds.lastSlot) {
    console.log("No transactions found"); return;
  }
  const fullRange: SlotRange = { gte: bounds.firstSlot, lte: bounds.lastSlot };
  console.log(`Slot range: ${fullRange.gte} - ${fullRange.lte} (span: ${fullRange.lte - fullRange.gte})`);
  console.log(`Quick sig count: ${bounds.totalSigCount}\n`);

  // ── Current algorithm ──
  console.log("── Current algorithm ──");
  resetCallCount();
  const t1 = performance.now();
  const v1 = await computeSolBalanceOverTime({
    address, rpcUrl: RPC_URL,
    initialChunks: c, splitFactor: 6,
    concurrencyLimit: c,
    maxTransactions: tier.maxTransactions,
  });
  const v1Time = Math.round(performance.now() - t1);
  console.log(`  ${"probe+fetch".padEnd(20)} | ${String(v1Time).padStart(7)}ms | ${String(v1.totalCalls).padStart(5)} calls | ${String(v1.transactions.length).padStart(7)} txs |            | PASS\n`);

  // ── Parallel drain with work-stealing ──
  console.log("── Parallel drain (work-stealing) ──");

  const configs = [
    { label: `${c} ranges (c*1)`, ranges: c },
    { label: `${c * 2} ranges (c*2)`, ranges: c * 2 },
    { label: `${c * 3} ranges (c*3)`, ranges: c * 3 },
    { label: `${c * 5} ranges (c*5)`, ranges: c * 5 },
    { label: `${c * 7} ranges (c*7)`, ranges: c * 7 },
  ];

  for (const cfg of configs) {
    await new Promise(r => setTimeout(r, 3000));
    await runBenchmark(cfg.label, address, fullRange, c, cfg.ranges, bounds.balance);
  }
}

main().catch(console.error);

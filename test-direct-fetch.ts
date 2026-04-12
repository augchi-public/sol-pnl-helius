import {
  getBoundsAndBalance,
  accountExists,
  fetchFull,
  parallelMap,
  getCallCount,
  resetCallCount,
  detectTier,
} from "./src/rpc.js";
import { debugLog } from "./src/logger.js";
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

// Direct fetch: no probing, just split and fetch
async function directFetch(
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  currentBalance: number,
): Promise<{ txs: BalanceTx[]; calls: number; durationMs: number }> {
  resetCallCount();
  const start = performance.now();
  const allTxs: BalanceTx[] = [];

  const ranges = splitRange(fullRange, concurrency);

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

// Direct fetch v2: more chunks than concurrency to reduce per-range density
async function directFetchOverSplit(
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  chunks: number,
  currentBalance: number,
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

// Density-informed direct fetch: uses Round 0's sigSlots to estimate ideal chunk count
async function densityInformedFetch(
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  currentBalance: number,
  sigSlots: number[],
  quickCount: number,
): Promise<{ txs: BalanceTx[]; calls: number; durationMs: number; chunks: number }> {
  resetCallCount();
  const start = performance.now();
  const allTxs: BalanceTx[] = [];

  // Estimate total txs from density
  let estTotal = quickCount;
  if (quickCount >= 1000 && sigSlots.length >= 1000) {
    const newestSlot = sigSlots[0];
    const oldestSampleSlot = sigSlots[sigSlots.length - 1];
    const sampleSpan = newestSlot - oldestSampleSlot;
    const fullSpan = fullRange.lte - fullRange.gte;
    if (sampleSpan > 0) {
      const density = 1000 / sampleSpan;
      estTotal = Math.round(density * fullSpan);
    }
  }

  // Target ~60 txs per range to guarantee single-page fetch with headroom
  // 2.5x safety factor accounts for non-uniform density distribution
  const idealChunks = Math.ceil((estTotal * 2.5) / 60);
  const chunks = Math.max(idealChunks, concurrency * 5); // floor: concurrency * 5

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
    chunks,
  };
}

// Work-queue adaptive fetch: continuous workers pull from a shared queue.
// When a fetch returns a full page, the remaining range is split and pushed
// back into the queue — no iteration barriers.
async function workQueueFetch(
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  initialChunks: number,
  currentBalance: number,
  maxTxs: number,
): Promise<{ txs: BalanceTx[]; calls: number; durationMs: number; chunks: number }> {
  resetCallCount();
  const start = performance.now();
  const allTxs: BalanceTx[] = [];
  let totalFetched = 0;
  let requeued = 0;

  const queue: SlotRange[] = splitRange(fullRange, initialChunks);
  let queueIdx = 0;
  let activeWorkers = 0;
  let done = false;

  function getNext(): SlotRange | null {
    if (queueIdx < queue.length) return queue[queueIdx++];
    return null;
  }

  await new Promise<void>((resolve) => {
    function tryResolve() {
      if (activeWorkers === 0 && queueIdx >= queue.length) {
        done = true;
        resolve();
      }
    }

    async function worker() {
      while (!done) {
        const range = getNext();
        if (!range) {
          tryResolve();
          return;
        }
        activeWorkers++;
        try {
          const result = await fetchFull(RPC_URL, address, range);
          const txs = extractBalanceTxs(result.data, address);
          allTxs.push(...txs);
          totalFetched += txs.length;

          if (result.data.length >= FULL_LIMIT && result.paginationToken) {
            const rawSlots = result.data.map(tx => tx.slot);
            const minSlot = Math.min(...rawSlots);
            const remainingRange: SlotRange = { gte: range.gte, lte: minSlot };
            if (remainingRange.lte >= remainingRange.gte) {
              const span = remainingRange.lte - remainingRange.gte;
              const splitFactor = Math.min(10, Math.max(2, Math.ceil(span / 1000)));
              const subRanges = splitRange(remainingRange, splitFactor);
              queue.push(...subRanges);
              requeued += subRanges.length;
            }
          }
        } catch (err) {
          console.error("Worker error:", err);
        }
        activeWorkers--;
        // Don't resolve yet — try to pick up more work first
        if (queueIdx >= queue.length && activeWorkers === 0) {
          tryResolve();
          return;
        }
      }
    }

    // Launch workers
    for (let i = 0; i < concurrency; i++) {
      worker();
    }
  });

  const seen = new Set<string>();
  const uniqueTxs = allTxs.filter(tx => {
    if (seen.has(tx.signature)) return false;
    seen.add(tx.signature);
    return true;
  });

  console.log(`    initial ${initialChunks} ranges, ${requeued} re-queued, total ${queueIdx} processed`);

  return {
    txs: uniqueTxs,
    calls: getCallCount(),
    durationMs: performance.now() - start,
    chunks: initialChunks,
  };
}

async function main() {
  const address = process.argv[2] || "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR";
  const tier = detectTier();
  const concurrency = tier.concurrency;

  console.log(`Wallet: ${address}`);
  console.log(`Tier: ${tier.tier} (${concurrency} concurrency)\n`);

  // Get bounds
  const bounds = await getBoundsAndBalance(RPC_URL, address);
  if (!bounds.firstSlot || !bounds.lastSlot) {
    console.log("No transactions found"); return;
  }
  const fullRange: SlotRange = { gte: bounds.firstSlot, lte: bounds.lastSlot };
  console.log(`Slot range: ${fullRange.gte} - ${fullRange.lte} (span: ${fullRange.lte - fullRange.gte})`);
  console.log(`Quick sig count: ${bounds.totalSigCount}`);

  // Estimate total from density
  let estTotal = bounds.totalSigCount;
  if (bounds.totalSigCount >= 1000 && bounds.sigSlots.length >= 1000) {
    const newestSlot = bounds.sigSlots[0];
    const oldestSampleSlot = bounds.sigSlots[bounds.sigSlots.length - 1];
    const sampleSpan = newestSlot - oldestSampleSlot;
    const fullSpan = fullRange.lte - fullRange.gte;
    if (sampleSpan > 0) {
      const density = 1000 / sampleSpan;
      estTotal = Math.round(density * fullSpan);
    }
  }
  console.log(`Estimated total txs: ${estTotal}\n`);

  // ── Benchmark 1: Current algorithm ──
  console.log("── Current algorithm (probe + fetch) ──");
  resetCallCount();
  const t1 = performance.now();
  const v1Result = await computeSolBalanceOverTime({
    address, rpcUrl: RPC_URL,
    initialChunks: concurrency, splitFactor: 6,
    concurrencyLimit: concurrency,
    maxTransactions: tier.maxTransactions,
  });
  const v1Time = performance.now() - t1;
  const v1Timeline = reconstructBalance(v1Result.transactions, v1Result.currentBalance, address);
  const v1Valid = validateBalance(v1Timeline, v1Result.currentBalance);
  console.log(`  Time: ${Math.round(v1Time)}ms`);
  console.log(`  Calls: ${v1Result.totalCalls}`);
  console.log(`  Txs: ${v1Result.transactions.length}`);
  console.log(`  Valid: ${v1Valid.valid ? "PASS" : "FAIL"}`);

  await new Promise(r => setTimeout(r, 5000));

  // ── Benchmark 2: Fixed over-split (concurrency * 5) ──
  const chunks2 = concurrency * 5;
  console.log(`\n── Fixed over-split (${chunks2} ranges) ──`);
  const df2 = await directFetchOverSplit(address, fullRange, concurrency, chunks2, bounds.balance);
  const df2Timeline = reconstructBalance(df2.txs, bounds.balance, address);
  const df2Valid = validateBalance(df2Timeline, bounds.balance);
  console.log(`  Time: ${Math.round(df2.durationMs)}ms`);
  console.log(`  Calls: ${df2.calls}`);
  console.log(`  Txs: ${df2.txs.length}`);
  console.log(`  Valid: ${df2Valid.valid ? "PASS" : "FAIL"}`);

  await new Promise(r => setTimeout(r, 5000));

  // ── Benchmark 3: Work-queue fetch (start = concurrency) ──
  console.log(`\n── Work-queue fetch (start ${concurrency} ranges) ──`);
  const wq1 = await workQueueFetch(
    address, fullRange, concurrency, concurrency, bounds.balance, tier.maxTransactions,
  );
  const wq1Timeline = reconstructBalance(wq1.txs, bounds.balance, address);
  const wq1Valid = validateBalance(wq1Timeline, bounds.balance);
  console.log(`  Time: ${Math.round(wq1.durationMs)}ms`);
  console.log(`  Calls: ${wq1.calls}`);
  console.log(`  Txs: ${wq1.txs.length}`);
  console.log(`  Valid: ${wq1Valid.valid ? "PASS" : "FAIL"}`);

  await new Promise(r => setTimeout(r, 5000));

  // ── Benchmark 4: Work-queue fetch (start = concurrency * 3) ──
  const initChunks4 = concurrency * 3;
  console.log(`\n── Work-queue fetch (start ${initChunks4} ranges) ──`);
  const wq2 = await workQueueFetch(
    address, fullRange, concurrency, initChunks4, bounds.balance, tier.maxTransactions,
  );
  const wq2Timeline = reconstructBalance(wq2.txs, bounds.balance, address);
  const wq2Valid = validateBalance(wq2Timeline, bounds.balance);
  console.log(`  Time: ${Math.round(wq2.durationMs)}ms`);
  console.log(`  Calls: ${wq2.calls}`);
  console.log(`  Txs: ${wq2.txs.length}`);
  console.log(`  Valid: ${wq2Valid.valid ? "PASS" : "FAIL"}`);
}

main().catch(console.error);

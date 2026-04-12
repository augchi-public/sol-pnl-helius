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

// ── Memory map entry ──
interface RangeEntry {
  gte: number;
  lte: number;
  status: "queued" | "running" | "done";
  ownerId: number;       // worker id, -1 if unowned
  currentLte: number;    // tracks progress within the range
}

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

async function memoryMapDrain(
  address: string,
  fullRange: SlotRange,
  concurrency: number,
  initialChunks: number,
): Promise<{
  txs: BalanceTx[]; calls: number; durationMs: number;
  maxPagesPerWorker: number; totalEntries: number;
  queuedClaims: number; steals: number;
}> {
  resetCallCount();
  const start = performance.now();

  // Shared memory map
  const map: RangeEntry[] = splitRange(fullRange, initialChunks).map(r => ({
    gte: r.gte, lte: r.lte, status: "queued" as const,
    ownerId: -1, currentLte: r.lte,
  }));

  const allTxs: BalanceTx[] = [];
  const seen = new Set<string>();
  let maxPagesPerWorker = 0;
  let queuedClaims = 0;
  let steals = 0;

  function addTxs(txs: BalanceTx[]) {
    for (const tx of txs) {
      if (!seen.has(tx.signature)) {
        seen.add(tx.signature);
        allTxs.push(tx);
      }
    }
  }

  // 1) Claim a queued entry from the map
  function claimQueued(workerId: number): RangeEntry | null {
    for (const entry of map) {
      if (entry.status === "queued") {
        entry.status = "running";
        entry.ownerId = workerId;
        queuedClaims++;
        return entry;
      }
    }
    return null;
  }

  // 2) Steal from a running entry with the largest remaining span
  //    Split: take the lower half, victim keeps upper half
  function stealFromRunning(workerId: number): RangeEntry | null {
    let bestEntry: RangeEntry | null = null;
    let bestRemaining = 0;

    for (const entry of map) {
      if (entry.status !== "running" || entry.ownerId === workerId) continue;
      const remaining = entry.currentLte - entry.gte;
      if (remaining > bestRemaining && remaining >= 100) {
        bestRemaining = remaining;
        bestEntry = entry;
      }
    }

    if (!bestEntry) return null;

    // Split: new entry gets the lower portion, victim keeps upper
    const splitPoint = bestEntry.gte + Math.floor((bestEntry.currentLte - bestEntry.gte) / 2);
    const newEntry: RangeEntry = {
      gte: bestEntry.gte,
      lte: splitPoint,
      status: "running",
      ownerId: workerId,
      currentLte: splitPoint,
    };
    bestEntry.gte = splitPoint + 1;
    map.push(newEntry);
    steals++;
    return newEntry;
  }

  // Get work: queue only, no stealing
  function getWork(workerId: number): RangeEntry | null {
    return claimQueued(workerId);
  }

  const workers = Array.from({ length: concurrency }, async (_, workerId) => {
    let workerPages = 0;

    while (true) {
      const entry = getWork(workerId);
      if (!entry) break;

      // Drain this entry: fetch one page at a time, push remainder to queue
      while (entry.currentLte >= entry.gte) {
        const range: SlotRange = { gte: entry.gte, lte: entry.currentLte };
        const result = await fetchFull(RPC_URL, address, range);
        const txs = extractBalanceTxs(result.data, address);
        addTxs(txs);
        workerPages++;

        if (result.data.length < FULL_LIMIT) break; // range drained

        // Full page: find min slot, push remaining as new queued entry
        const minSlot = Math.min(...result.data.map(tx => tx.slot));
        if (minSlot <= entry.gte) break;

        // Push the remaining [gte, minSlot] back to the queue for ANY worker to claim
        // (using minSlot inclusive to avoid missing boundary txs, dedup via `seen`)
        const remaining: RangeEntry = {
          gte: entry.gte,
          lte: minSlot,
          status: "queued",
          ownerId: -1,
          currentLte: minSlot,
        };
        map.push(remaining);

        // This worker is done with this entry — go grab next work
        break;
      }

      entry.status = "done";
    }

    if (workerPages > maxPagesPerWorker) maxPagesPerWorker = workerPages;
  });

  await Promise.all(workers);

  return {
    txs: allTxs,
    calls: getCallCount(),
    durationMs: performance.now() - start,
    maxPagesPerWorker,
    totalEntries: map.length,
    queuedClaims,
    steals,
  };
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
  const span = fullRange.lte - fullRange.gte;
  console.log(`Slot range: ${fullRange.gte} - ${fullRange.lte} (span: ${span})`);
  console.log(`Quick sig count: ${bounds.totalSigCount}\n`);

  // ── Current algorithm ──
  console.log("── Current algorithm (probe + fetch) ──");
  resetCallCount();
  const t1 = performance.now();
  const v1 = await computeSolBalanceOverTime({
    address, rpcUrl: RPC_URL,
    initialChunks: c, splitFactor: 6,
    concurrencyLimit: c,
    maxTransactions: tier.maxTransactions,
  });
  const v1Time = Math.round(performance.now() - t1);
  console.log(`  Time: ${v1Time}ms | Calls: ${v1.totalCalls} | Txs: ${v1.transactions.length}\n`);

  // ── Memory-map drain benchmarks ──
  const configs = [
    { label: `memmap c*1`, chunks: c },
    { label: `memmap c*2`, chunks: c * 2 },
    { label: `memmap c*3`, chunks: c * 3 },
    { label: `memmap c*5`, chunks: c * 5 },
    { label: `memmap c*7`, chunks: c * 7 },
  ];

  for (const cfg of configs) {
    await new Promise(r => setTimeout(r, 4000));
    console.log(`── ${cfg.label} (${cfg.chunks} initial ranges) ──`);
    const result = await memoryMapDrain(address, fullRange, c, cfg.chunks);
    const timeline = reconstructBalance(result.txs, bounds.balance, address);
    const valid = validateBalance(timeline, bounds.balance);
    console.log(
      `  Time: ${Math.round(result.durationMs)}ms | Calls: ${result.calls} | Txs: ${result.txs.length} | ` +
      `MaxPages/W: ${result.maxPagesPerWorker} | Entries: ${result.totalEntries} | ` +
      `Queued: ${result.queuedClaims} | Steals: ${result.steals} | ` +
      `Valid: ${valid.valid ? "PASS" : "FAIL"}\n`
    );
  }
}

main().catch(console.error);

import type {
  SlotRange,
  FullTransaction,
  BalanceTx,
  AlgorithmConfig,
  RoundStats,
} from "./types.js";
import {
  getBoundsAndBalance,
  probeSignatures,
  fetchFull,
  fetchFullUnbounded,
  parallelMap,
  getCallCount,
  resetCallCount,
  estimateDensity,
  getRetryStats,
} from "./rpc.js";

export { computeSolBalanceOverTimeV3v2 };
import { debugLog } from "./logger.js";

const PROBE_LIMIT = 1000;
const FULL_LIMIT = 100;
const MAX_PAGES_PER_CHUNK = 50;

// ── Slot-range splitting ──

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

// ── Build balanced ranges from known signature slots ──

function buildBalancedRanges(
  sigSlots: number[],
  fullRange: SlotRange,
  maxPerRange: number,
): SlotRange[] {
  const inRange = sigSlots.filter((s) => s >= fullRange.gte && s <= fullRange.lte);
  if (inRange.length === 0) return [fullRange];
  if (inRange.length <= maxPerRange) return [fullRange];

  const sorted = inRange.sort((a, b) => a - b);
  const ranges: SlotRange[] = [];
  let rangeStart = fullRange.gte;

  for (let i = 0; i < sorted.length; i += maxPerRange) {
    const chunkEnd = i + maxPerRange;
    if (chunkEnd >= sorted.length) {
      ranges.push({ gte: rangeStart, lte: fullRange.lte });
    } else {
      const lastSlotInChunk = sorted[chunkEnd - 1];
      const firstSlotNextChunk = sorted[chunkEnd];
      const boundary = lastSlotInChunk === firstSlotNextChunk
        ? lastSlotInChunk
        : Math.floor((lastSlotInChunk + firstSlotNextChunk) / 2);
      if (boundary >= rangeStart) {
        ranges.push({ gte: rangeStart, lte: boundary });
        rangeStart = boundary + 1;
      }
    }
  }

  if (ranges.length === 0) return [fullRange];
  ranges[ranges.length - 1].lte = fullRange.lte;

  return ranges;
}

// ── Extract only what's needed for balance reconstruction ──

function findAddressIndex(
  tx: FullTransaction,
  address: string,
): number {
  const keys = tx.transaction.message.accountKeys;
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if ((typeof key === "string" ? key : key.pubkey) === address) {
      return i;
    }
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

function extractBalanceTxs(
  txs: FullTransaction[],
  address: string,
): BalanceTx[] {
  const slim: BalanceTx[] = [];
  for (const tx of txs) {
    if (!tx.meta) continue;
    if (!Array.isArray(tx.meta.preBalances) || !Array.isArray(tx.meta.postBalances)) {
      continue;
    }
    const idx = findAddressIndex(tx, address);
    if (idx === -1) continue;
    if (idx >= tx.meta.preBalances.length || idx >= tx.meta.postBalances.length) {
      continue;
    }
    const pre = tx.meta.preBalances[idx];
    const post = tx.meta.postBalances[idx];
    if (pre === post) continue;
    const signature = tx.transaction.signatures[0];
    if (!signature) continue;
    slim.push({
      signature,
      slot: tx.slot,
      blockTime: tx.blockTime,
      transactionIndex: Number.isFinite(tx.transactionIndex) ? tx.transactionIndex : 0,
      preBalance: pre,
      postBalance: post,
    });
  }
  return slim;
}

// ── Paginate through a range with shared cap ──

async function paginateFullCapped(
  rpcUrl: string,
  address: string,
  range: SlotRange,
  collected: { count: number; rawPages: number },
  cap: number,
  rawPagesCap: number,
  signal?: AbortSignal,
): Promise<BalanceTx[]> {
  const allTxs: BalanceTx[] = [];
  let token: string | undefined = undefined;
  let pages = 0;
  while (pages < MAX_PAGES_PER_CHUNK && collected.count < cap && collected.rawPages < rawPagesCap) {
    const result = await fetchFull(rpcUrl, address, range, token, signal);
    const slim = extractBalanceTxs(result.data, address);
    allTxs.push(...slim);
    collected.count += slim.length;
    collected.rawPages++;
    pages++;
    if (result.data.length < FULL_LIMIT || !result.paginationToken) break;
    token = result.paginationToken;
  }
  return allTxs;
}

// ── Async work queue for Phase 2 ──

type FetchWorkItem = {
  range: SlotRange;
  paginationToken?: string;
  tokenDepth: number;
};

class AsyncQueue<T> {
  private items: T[] = [];
  private waiters: Array<(item: T | null) => void> = [];
  private closed = false;

  push(item: T): void {
    if (this.closed) return;
    const waiter = this.waiters.shift();
    if (waiter) { waiter(item); } else { this.items.push(item); }
  }

  async shift(): Promise<T | null> {
    if (this.items.length > 0) return this.items.shift()!;
    if (this.closed) return null;
    return new Promise((resolve) => this.waiters.push(resolve));
  }

  close(): void {
    this.closed = true;
    while (this.waiters.length > 0) this.waiters.shift()!(null);
  }
}

function buildChildWorkItem(
  item: FetchWorkItem,
  page: FullTransaction[],
  paginationToken: string,
): FetchWorkItem {
  const minSlot = Math.min(...page.map((tx) => tx.slot));
  const noSlotProgress = minSlot === item.range.lte || minSlot === item.range.gte;

  if (item.paginationToken || noSlotProgress) {
    return { range: item.range, paginationToken, tokenDepth: item.tokenDepth + 1 };
  }
  return { range: { gte: item.range.gte, lte: minSlot }, tokenDepth: 0 };
}

async function queueFetchPhase2(
  rpcUrl: string,
  address: string,
  ranges: SlotRange[],
  concurrency: number,
  signal?: AbortSignal,
): Promise<BalanceTx[]> {
  const queue = new AsyncQueue<FetchWorkItem>();
  const allTxs: BalanceTx[] = [];
  const seen = new Set<string>();
  let outstanding = 0;

  const enqueue = (item: FetchWorkItem) => { outstanding++; queue.push(item); };
  const finishItem = () => { outstanding--; if (outstanding === 0) queue.close(); };

  signal?.addEventListener("abort", () => queue.close(), { once: true });

  for (const range of ranges) {
    enqueue({ range, tokenDepth: 0 });
  }

  const workers = Array.from({ length: Math.min(concurrency, ranges.length) }, async () => {
    while (true) {
      if (signal?.aborted) return;
      const item = await queue.shift();
      if (!item) return;
      try {
        const result = await fetchFull(rpcUrl, address, item.range, item.paginationToken, signal);
        const slim = extractBalanceTxs(result.data, address);
        for (const tx of slim) {
          if (!seen.has(tx.signature)) { seen.add(tx.signature); allTxs.push(tx); }
        }
        if (result.data.length === FULL_LIMIT && result.paginationToken) {
          enqueue(buildChildWorkItem(item, result.data, result.paginationToken));
        }
      } finally {
        finishItem();
      }
    }
  });

  await Promise.all(workers);
  return allTxs;
}

// ── Main algorithm ──

export interface AlgorithmResult {
  currentBalance: number;
  transactions: BalanceTx[];
  roundStats: RoundStats[];
  totalCalls: number;
  totalDurationMs: number;
  capped: boolean;
  retries: number;
  retryTimeMs: number;
}

async function computeSolBalanceOverTimeV3v2(
  config: AlgorithmConfig,
): Promise<AlgorithmResult> {
  const overallStart = performance.now();
  const roundStats: RoundStats[] = [];
  resetCallCount();
  debugLog("algo", "start", {
    address: config.address,
    concurrencyLimit: config.concurrencyLimit,
    maxTransactions: config.maxTransactions,
  });

  const signal = config.signal;

  // ── Round 0: Bounds + anchor ──
  const round0Start = performance.now();
  const callsBefore0 = getCallCount();
  const bounds = await getBoundsAndBalance(config.rpcUrl, config.address, signal);
  roundStats.push({
    round: 0,
    durationMs: performance.now() - round0Start,
    callCount: getCallCount() - callsBefore0,
    probes: 0, leafFetches: 0, splits: 0, empties: 0,
  });

  if (bounds.firstSlot === null || bounds.lastSlot === null) {
    debugLog("algo", "no transactions found, returning balance anchor only");
    return {
      currentBalance: bounds.balance,
      transactions: [],
      roundStats,
      totalCalls: getCallCount(),
      totalDurationMs: performance.now() - overallStart,
      capped: false,
      ...getRetryStats(),
    };
  }

  const allTransactions: BalanceTx[] = [];
  const fullRange: SlotRange = {
    gte: config.fromSlot != null ? Math.max(config.fromSlot, bounds.firstSlot) : bounds.firstSlot,
    lte: config.toSlot != null ? Math.min(config.toSlot, bounds.lastSlot) : bounds.lastSlot,
  };

  if (fullRange.gte > fullRange.lte) {
    debugLog("algo", "slot filter excludes all transactions");
    return {
      currentBalance: bounds.balance,
      transactions: [],
      roundStats,
      totalCalls: getCallCount(),
      totalDurationMs: performance.now() - overallStart,
      capped: false,
      ...getRetryStats(),
    };
  }

  debugLog("algo", "bounds established", { fullRange, currentBalance: bounds.balance });

  // ── Quick size estimate ──
  const hasTimeFilter = config.fromSlot != null || config.toSlot != null;
  let quickCount: number;
  let quickSigSlots: number[];
  if (hasTimeFilter) {
    const quickResult = await probeSignatures(config.rpcUrl, config.address, {
      range: fullRange,
      limit: PROBE_LIMIT,
    }, signal);
    quickCount = quickResult.data.length;
    quickSigSlots = quickResult.data.map((s) => s.slot);
  } else {
    quickCount = bounds.totalSigCount;
    quickSigSlots = bounds.sigSlots;
  }
  debugLog("algo", "quick count", { quickCount, fromBounds: !hasTimeFilter });

  if (quickCount <= FULL_LIMIT) {
    debugLog("algo", "tiny wallet fast path", { count: quickCount });
    const fetchStart = performance.now();
    const callsBeforeFetch = getCallCount();
    const result = await fetchFull(config.rpcUrl, config.address, fullRange, undefined, signal);
    const txs = extractBalanceTxs(result.data, config.address);
    allTransactions.push(...txs);

    roundStats.push({
      round: 1,
      durationMs: performance.now() - fetchStart,
      callCount: getCallCount() - callsBeforeFetch,
      probes: 0, leafFetches: 1, splits: 0, empties: 0,
    });

    return {
      currentBalance: bounds.balance,
      transactions: allTransactions,
      roundStats,
      totalCalls: getCallCount(),
      totalDurationMs: performance.now() - overallStart,
      capped: false,
      ...getRetryStats(),
    };
  }

  if (quickCount < PROBE_LIMIT) {
    debugLog("algo", "small wallet fast path", { count: quickCount });

    const fetchRanges = buildBalancedRanges(quickSigSlots, fullRange, FULL_LIMIT);

    const fetchStart = performance.now();
    const callsBeforeFetch = getCallCount();
    const fetchResults = await parallelMap(
      fetchRanges,
      config.concurrencyLimit,
      async (range) => {
        const allTxs: BalanceTx[] = [];
        let token: string | undefined = undefined;
        let pages = 0;
        while (pages < MAX_PAGES_PER_CHUNK) {
          const result = await fetchFull(config.rpcUrl, config.address, range, token, signal);
          allTxs.push(...extractBalanceTxs(result.data, config.address));
          pages++;
          if (result.data.length < FULL_LIMIT || !result.paginationToken) break;
          token = result.paginationToken;
        }
        return allTxs;
      },
      signal,
    );
    for (const txs of fetchResults) allTransactions.push(...txs);

    roundStats.push({
      round: 1,
      durationMs: performance.now() - fetchStart,
      callCount: getCallCount() - callsBeforeFetch,
      probes: 0, leafFetches: fetchRanges.length, splits: 0, empties: 0,
    });

    return {
      currentBalance: bounds.balance,
      transactions: allTransactions,
      roundStats,
      totalCalls: getCallCount(),
      totalDurationMs: performance.now() - overallStart,
      capped: false,
      ...getRetryStats(),
    };
  }

  // Large wallet (1000+): sig-sweep → queue-based full fetch
  debugLog("algo", "large wallet — sig-sweep + queue fetch", { quickCount });

  const maxTx = config.maxTransactions;
  let capped = false;

  // ── Multi-point density-based cap prediction ──
  if (quickCount >= PROBE_LIMIT && !hasTimeFilter) {
    const densityEst = await estimateDensity(
      config.rpcUrl, config.address, fullRange, bounds.sigSlots, signal,
    );
    if (densityEst.estimatedTotalTxs > maxTx * 1.5) {
      capped = true;
      debugLog("algo", "capped by density prediction", {
        estTotal: densityEst.estimatedTotalTxs, maxTx,
      });
    }
  }

  // ── Fetch phase ──
  const fetchStart = performance.now();
  const callsBeforeFetch = getCallCount();

  if (capped) {
    const cappedRangeCount = Math.ceil(maxTx / FULL_LIMIT);
    const cappedRanges = splitRange(fullRange, cappedRangeCount);
    const rawPagesCap = cappedRangeCount + 500;
    const collected = { count: 0, rawPages: 0 };

    const sortedRanges = cappedRanges.sort((a, b) => b.lte - a.lte);
    debugLog("algo", "capped fetch", { ranges: sortedRanges.length, rawPagesCap });

    const fetchResults = await parallelMap(
      sortedRanges,
      config.concurrencyLimit,
      async (range) => {
        if (collected.count >= maxTx || collected.rawPages >= rawPagesCap) return [];
        return paginateFullCapped(
          config.rpcUrl, config.address, range,
          collected, maxTx, rawPagesCap, signal,
        );
      },
      signal,
    );
    for (const txs of fetchResults) allTransactions.push(...txs);

    let sweepToken: string | undefined;
    for (let sweepPage = 0; sweepPage < 10; sweepPage++) {
      const page = await fetchFullUnbounded(config.rpcUrl, config.address, sweepToken, signal);
      const slim = extractBalanceTxs(page.data, config.address);
      allTransactions.push(...slim);
      if (slim.length > 0 || page.data.length < FULL_LIMIT || !page.paginationToken) break;
      sweepToken = page.paginationToken;
    }

    if (allTransactions.length > maxTx) {
      allTransactions.sort((a, b) => b.slot - a.slot || b.transactionIndex - a.transactionIndex);
      allTransactions.length = maxTx;
    }
  } else {
    // ── Phase 1: Parallel sig sweep (1000 sigs/call) ──
    const SIG_SWEEP_CHUNKS = config.concurrencyLimit;
    const sigSweepRanges = splitRange(fullRange, SIG_SWEEP_CHUNKS);
    debugLog("algo", "sig sweep", { chunks: sigSweepRanges.length, concurrency: config.concurrencyLimit });

    const sigSweepResults = await parallelMap(
      sigSweepRanges,
      config.concurrencyLimit,
      async (range) => {
        const allSlots: number[] = [];
        let token: string | undefined;
        let pages = 0;
        while (pages < 200) {
          const result = await probeSignatures(config.rpcUrl, config.address, {
            range,
            sortOrder: "asc",
            limit: PROBE_LIMIT,
            paginationToken: token,
          }, signal);
          for (const entry of result.data) allSlots.push(entry.slot);
          pages++;
          if (result.data.length < PROBE_LIMIT || !result.paginationToken) break;
          token = result.paginationToken;
        }
        return allSlots;
      },
      signal,
    );

    const allSigSlots: number[] = [];
    for (const slots of sigSweepResults) allSigSlots.push(...slots);
    allSigSlots.sort((a, b) => a - b);

    const totalSigs = allSigSlots.length;
    debugLog("algo", "sig sweep complete", {
      totalSigs,
      uniqueSlots: new Set(allSigSlots).size,
      sigSweepCalls: getCallCount() - callsBeforeFetch,
    });

    // ── Phase 2: Queue-based full fetch ──
    // Use sig slots to build initial ranges (~97 sigs each), then let
    // workers dynamically handle overflow via the async queue.
    const TARGET_PER_CHUNK = 97;
    const fetchRanges = buildBalancedRanges(allSigSlots, fullRange, TARGET_PER_CHUNK);
    debugLog("algo", "queue-based full fetch", {
      chunks: fetchRanges.length,
      avgSigsPerChunk: Math.round(totalSigs / fetchRanges.length),
    });

    const queueTxs = await queueFetchPhase2(
      config.rpcUrl, config.address, fetchRanges,
      config.concurrencyLimit, signal,
    );
    for (const tx of queueTxs) allTransactions.push(tx);
  }

  roundStats.push({
    round: 1,
    durationMs: performance.now() - fetchStart,
    callCount: getCallCount() - callsBeforeFetch,
    probes: 0, leafFetches: 0, splits: 0, empties: 0,
  });

  debugLog("algo", "complete", {
    transactions: allTransactions.length,
    capped,
    totalCalls: getCallCount(),
    totalDurationMs: Math.round(performance.now() - overallStart),
    ...getRetryStats(),
  });

  return {
    currentBalance: bounds.balance,
    transactions: allTransactions,
    roundStats,
    totalCalls: getCallCount(),
    totalDurationMs: performance.now() - overallStart,
    capped,
    ...getRetryStats(),
  };
}

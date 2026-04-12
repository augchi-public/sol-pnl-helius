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
} from "./rpc.js";

export { computeSolBalanceOverTimeV2 };
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
  // Filter to slots actually within the target range
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

  // If the loop produced no ranges (edge case), fall back to full range
  if (ranges.length === 0) return [fullRange];
  // Ensure the last range extends to fullRange.lte
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

  // For v0 transactions, the address may be loaded via Address Lookup Tables.
  // preBalances/postBalances are indexed: [static keys..., writable loaded..., readonly loaded...]
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

async function paginateFull(
  rpcUrl: string,
  address: string,
  range: SlotRange,
  signal?: AbortSignal,
): Promise<BalanceTx[]> {
  const allTxs: BalanceTx[] = [];
  let token: string | undefined = undefined;
  let pages = 0;
  while (pages < MAX_PAGES_PER_CHUNK) {
    const result = await fetchFull(rpcUrl, address, range, token, signal);
    allTxs.push(...extractBalanceTxs(result.data, address));
    pages++;
    if (result.data.length < FULL_LIMIT || !result.paginationToken) break;
    token = result.paginationToken;
  }
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
}

async function computeSolBalanceOverTimeV2(
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
    probes: 0,
    leafFetches: 0,
    splits: 0,
    empties: 0,
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
    };
  }

  debugLog("algo", "bounds established", { fullRange, currentBalance: bounds.balance });

  // ── Quick size estimate ──
  // If no time filters are active, bounds.totalSigCount already gives us
  // a free count (the desc query fetched up to 1000 sigs). If time filters
  // narrow the range, we need a separate probe on the filtered range.
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
    // Tiny wallet: fits in a single fetch call — skip straight to fetch
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

    debugLog("algo", "complete", {
      transactions: allTransactions.length,
      capped: false,
      totalCalls: getCallCount(),
      totalDurationMs: Math.round(performance.now() - overallStart),
    });

    return {
      currentBalance: bounds.balance,
      transactions: allTransactions,
      roundStats,
      totalCalls: getCallCount(),
      totalDurationMs: performance.now() - overallStart,
      capped: false,
    };
  }

  if (quickCount < PROBE_LIMIT) {
    // Small wallet (101–999 txs): known count, split precisely and fetch
    debugLog("algo", "small wallet fast path", { count: quickCount });

    // Build balanced ranges using actual signature slot data so each
    // sub-range has ≤ FULL_LIMIT txs and needs exactly one fetch page.
    const fetchRanges = buildBalancedRanges(quickSigSlots, fullRange, FULL_LIMIT);

    const fetchStart = performance.now();
    const callsBeforeFetch = getCallCount();
    const fetchResults = await parallelMap(
      fetchRanges,
      config.concurrencyLimit,
      (range) => paginateFull(config.rpcUrl, config.address, range, signal),
      signal,
    );
    for (const txs of fetchResults) allTransactions.push(...txs);

    roundStats.push({
      round: 1,
      durationMs: performance.now() - fetchStart,
      callCount: getCallCount() - callsBeforeFetch,
      probes: 0, leafFetches: fetchRanges.length, splits: 0, empties: 0,
    });

    debugLog("algo", "complete", {
      transactions: allTransactions.length,
      capped: false,
      totalCalls: getCallCount(),
      totalDurationMs: Math.round(performance.now() - overallStart),
    });

    return {
      currentBalance: bounds.balance,
      transactions: allTransactions,
      roundStats,
      totalCalls: getCallCount(),
      totalDurationMs: performance.now() - overallStart,
      capped: false,
    };
  }

  // Large wallet (1000+): direct parallel fetch, no probing
  debugLog("algo", "large wallet — direct fetch", { quickCount });

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
    // ── c*7 simple queue drain: split into concurrency*7 ranges,
    //    each worker drains its range page by page, then grabs the next ──
    const CHUNK_MULTIPLIER = 7;
    const numChunks = config.concurrencyLimit * CHUNK_MULTIPLIER;
    const fetchRanges = splitRange(fullRange, numChunks);
    debugLog("algo", "direct fetch", { chunks: fetchRanges.length, concurrency: config.concurrencyLimit });

    const fetchResults = await parallelMap(
      fetchRanges,
      config.concurrencyLimit,
      (range) => paginateFull(config.rpcUrl, config.address, range, signal),
      signal,
    );
    for (const txs of fetchResults) allTransactions.push(...txs);
  }

  roundStats.push({
    round: 1,
    durationMs: performance.now() - fetchStart,
    callCount: getCallCount() - callsBeforeFetch,
    probes: 0,
    leafFetches: capped ? 0 : config.concurrencyLimit * 7,
    splits: 0,
    empties: 0,
  });

  debugLog("algo", "complete", {
    transactions: allTransactions.length,
    capped,
    totalCalls: getCallCount(),
    totalDurationMs: Math.round(performance.now() - overallStart),
  });

  return {
    currentBalance: bounds.balance,
    transactions: allTransactions,
    roundStats,
    totalCalls: getCallCount(),
    totalDurationMs: performance.now() - overallStart,
    capped,
  };
}

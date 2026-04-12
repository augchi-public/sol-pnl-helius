import type {
  SlotRange,
  FullTransaction,
  BalanceTx,
  AlgorithmConfig,
  RoundStats,
} from "./types.js";
import { RangeClass } from "./types.js";
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

// ── Probe + classify ──

interface ProbeResult {
  range: SlotRange;
  classification: RangeClass;
  count: number;
}

async function probeAndClassify(
  config: AlgorithmConfig,
  ranges: SlotRange[],
): Promise<ProbeResult[]> {
  debugLog("probe", "classifying probe ranges", { ranges: ranges.length });
  return parallelMap(ranges, config.concurrencyLimit, async (range) => {
    const result = await probeSignatures(config.rpcUrl, config.address, {
      range,
      limit: PROBE_LIMIT,
    }, config.signal);
    const count = result.data.length;
    let classification: RangeClass;
    if (count === 0) {
      classification = RangeClass.EMPTY;
    } else if (count <= FULL_LIMIT) {
      classification = RangeClass.LEAF;
    } else {
      classification = RangeClass.SPLIT;
    }
    return { range, classification, count };
  }, config.signal);
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

export async function computeSolBalanceOverTime(
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

  // Large wallet (1000+): check if we can skip probing entirely
  debugLog("algo", "large wallet", { quickCount });

  // ── Density-based cap prediction ──
  // ── Multi-point density-based cap prediction ──
  let skipProbing = false;
  if (quickCount >= PROBE_LIMIT && !hasTimeFilter) {
    const densityEst = await estimateDensity(
      config.rpcUrl, config.address, fullRange, bounds.sigSlots, signal,
    );
    if (densityEst.estimatedTotalTxs > config.maxTransactions * 1.5) {
      skipProbing = true;
      debugLog("algo", "skipping probing, going direct capped", {
        estTotal: densityEst.estimatedTotalTxs, maxTx: config.maxTransactions,
      });
    }
  }

  // ── Iterative probing: keep splitting SPLIT ranges until all are LEAFs ──
  const leafRanges: SlotRange[] = [];
  let pendingRanges = skipProbing ? [] : splitRange(fullRange, config.initialChunks);
  let estimatedTotal = 0;
  let totalEmpties = 0;
  let probeIteration = 0;
  const MAX_PROBE_ITERATIONS = 8;
  let capped = false;
  const maxTx = config.maxTransactions;

  while (pendingRanges.length > 0 && probeIteration < MAX_PROBE_ITERATIONS) {
    probeIteration++;
    const iterStart = performance.now();
    const callsBefore = getCallCount();

    const classified = await probeAndClassify(config, pendingRanges);

    const nextPending: SlotRange[] = [];
    let iterLeaves = 0;
    let iterSplits = 0;
    let iterEmpties = 0;
    let iterSaturated = 0;

    for (const c of classified) {
      if (c.classification === RangeClass.EMPTY) {
        iterEmpties++;
      } else if (c.classification === RangeClass.LEAF) {
        leafRanges.push(c.range);
        estimatedTotal += c.count;
        iterLeaves++;
      } else {
        iterSplits++;
        if (c.count < PROBE_LIMIT) {
          // Known count (101–999): split precisely to target ~100 txs per
          // sub-range. These sub-ranges are very likely LEAFs, so add them
          // directly — no need to re-probe.
          const factor = Math.ceil(c.count / FULL_LIMIT);
          const subs = splitRange(c.range, factor);
          leafRanges.push(...subs);
          estimatedTotal += c.count;
        } else {
          // Saturated (1000+): actual count unknown. Split by 10 and
          // re-probe in the next iteration to discover the real density.
          iterSaturated++;
          estimatedTotal += c.count;
          nextPending.push(...splitRange(c.range, 10));
        }
      }
    }

    totalEmpties += iterEmpties;
    roundStats.push({
      round: probeIteration,
      durationMs: performance.now() - iterStart,
      callCount: getCallCount() - callsBefore,
      probes: pendingRanges.length,
      leafFetches: 0,
      splits: iterSplits,
      empties: iterEmpties,
    });
    debugLog("algo", `probe iteration ${probeIteration}`, {
      probed: pendingRanges.length,
      leaves: iterLeaves,
      knownSplits: iterSplits - iterSaturated,
      saturated: iterSaturated,
      empties: iterEmpties,
      totalLeaves: leafRanges.length,
      nextPending: nextPending.length,
      estimatedTotal,
    });

    // Early exit: if the estimated total already exceeds the cap, stop
    // probing and switch to capped pagination. Each saturated range has
    // at least 1000 txs (likely more), so add a conservative multiplier.
    const conservativeTotal = estimatedTotal + iterSaturated * PROBE_LIMIT * 3;
    if (conservativeTotal > maxTx) {
      capped = true;
      leafRanges.push(...nextPending);
      debugLog("algo", "cap detected, stopping probe iterations", {
        conservativeTotal,
        maxTx,
        pendingAbsorbed: nextPending.length,
      });
      pendingRanges = [];
      break;
    }

    // Density-based early exit: if most ranges are splits (dense wallet),
    // further probing has diminishing returns — the extra probe calls cost
    // more than just letting paginateFull handle multi-page ranges within
    // the concurrent worker pool.
    const probed = classified.length;
    const densityRatio = probed > 0 ? iterSplits / probed : 0;
    if (probeIteration >= 1 && densityRatio > 0.5 && nextPending.length > config.concurrencyLimit) {
      leafRanges.push(...nextPending);
      debugLog("algo", "dense wallet, skipping further probing", {
        densityRatio: densityRatio.toFixed(2),
        saturated: iterSaturated,
        nextPending: nextPending.length,
      });
      pendingRanges = [];
      break;
    }

    // Safety: if nextPending is very large, the wallet is extremely dense
    // and further probing will generate even more ranges. Cap it.
    if (nextPending.length > config.concurrencyLimit * 20) {
      capped = true;
      leafRanges.push(...nextPending);
      debugLog("algo", "probe fan-out too large, capping", {
        nextPending: nextPending.length,
        limit: config.concurrencyLimit * 20,
      });
      pendingRanges = [];
      break;
    }

    pendingRanges = nextPending;
  }

  // If we still have unresolved ranges after max iterations,
  // add them as fetch targets (paginateFull will handle multi-page fetching)
  if (pendingRanges.length > 0) {
    debugLog("algo", "max probe iterations reached, adding remaining as fetch targets", {
      remaining: pendingRanges.length,
    });
    leafRanges.push(...pendingRanges);
  }

  debugLog("algo", "probing complete", {
    totalLeaves: leafRanges.length,
    totalEmpties,
    estimatedTotal,
    probeIterations: probeIteration,
  });

  // ── Cap check ──
  if (skipProbing || estimatedTotal > maxTx) {
    capped = true;
    debugLog("algo", "capped", { estimatedTotal, maxTx, skipProbing });
  }

  // ── Fetch: single parallel blast of all leaf ranges ──
  const fetchStart = performance.now();
  const callsBeforeFetch = getCallCount();

  if (capped) {
    // For capped wallets, ignore probed leaf ranges and split the full
    // range into exactly cap/100 uniform sub-ranges. Each sub-range
    // targets ~100 txs (one fetch page), maximising parallelism and
    // avoiding multi-page pagination within workers.
    const cappedRangeCount = Math.ceil(maxTx / FULL_LIMIT);
    const cappedRanges = splitRange(fullRange, cappedRangeCount);
    const rawPagesCap = cappedRangeCount + 500;
    const collected = { count: 0, rawPages: 0 };

    // Sort newest-first so we capture the most recent transactions
    const sortedRanges = cappedRanges.sort((a, b) => b.lte - a.lte);
    debugLog("algo", "capped fetch", { ranges: sortedRanges.length, rawPagesCap });

    const fetchResults = await parallelMap(
      sortedRanges,
      config.concurrencyLimit,
      async (range) => {
        if (collected.count >= maxTx || collected.rawPages >= rawPagesCap) return [];
        return paginateFullCapped(
          config.rpcUrl,
          config.address,
          range,
          collected,
          maxTx,
          rawPagesCap,
          signal,
        );
      },
      signal,
    );

    for (const txs of fetchResults) allTransactions.push(...txs);

    // Sweep most recent transactions to anchor the timeline
    let sweepToken: string | undefined;
    for (let sweepPage = 0; sweepPage < 10; sweepPage++) {
      const page = await fetchFullUnbounded(
        config.rpcUrl,
        config.address,
        sweepToken,
        signal,
      );
      const slim = extractBalanceTxs(page.data, config.address);
      allTransactions.push(...slim);
      if (slim.length > 0 || page.data.length < FULL_LIMIT || !page.paginationToken) break;
      sweepToken = page.paginationToken;
    }

    if (allTransactions.length > maxTx) {
      allTransactions.sort(
        (a, b) => b.slot - a.slot || b.transactionIndex - a.transactionIndex,
      );
      allTransactions.length = maxTx;
    }
  } else {
    // Normal: fetch all leaf ranges in one parallel blast
    const fetchResults = await parallelMap(
      leafRanges,
      config.concurrencyLimit,
      (range) => paginateFull(config.rpcUrl, config.address, range, signal),
      signal,
    );

    for (const txs of fetchResults) allTransactions.push(...txs);
  }

  roundStats.push({
    round: probeIteration + 1,
    durationMs: performance.now() - fetchStart,
    callCount: getCallCount() - callsBeforeFetch,
    probes: 0,
    leafFetches: leafRanges.length,
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

import type {
  JsonRpcRequest,
  JsonRpcResponse,
  GetBalanceResult,
  SignaturesResult,
  FullTransactionsResult,
  SlotRange,
} from "./types.js";
import { debugLog } from "./logger.js";

let globalCallCount = 0;
let globalRetryCount = 0;
let globalRetryTimeMs = 0;

export function resetCallCount(): void {
  globalCallCount = 0;
  globalRetryCount = 0;
  globalRetryTimeMs = 0;
}

export function getCallCount(): number {
  return globalCallCount;
}

export function getRetryStats(): { retries: number; retryTimeMs: number } {
  return { retries: globalRetryCount, retryTimeMs: Math.round(globalRetryTimeMs) };
}

// ── Low-level JSON-RPC transport ──

class RateLimitError extends Error {
  constructor(
    message: string,
    public retryAfterMs: number,
  ) {
    super(message);
  }
}

class CancelledError extends Error {
  constructor() { super("Cancelled"); this.name = "CancelledError"; }
}

export { CancelledError };

function checkSignal(signal?: AbortSignal): void {
  if (signal?.aborted) throw new CancelledError();
}

async function rpcPost<T>(
  rpcUrl: string,
  request: JsonRpcRequest,
  signal?: AbortSignal,
): Promise<T> {
  if (globalBucket) await globalBucket.acquire();
  checkSignal(signal);
  globalCallCount++;
  const started = performance.now();
  debugLog("rpc", "dispatch request", { method: request.method });

  const res = await fetch(rpcUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request),
    signal,
  });

  if (res.status === 429) {
    const retryAfter = res.headers.get("retry-after");
    const delayMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : 2000;
    globalCallCount--;
    debugLog("rpc", "rate limited", {
      retryAfterMs: delayMs,
      durationMs: Math.round(performance.now() - started),
    });
    throw new RateLimitError(`Rate limited`, delayMs);
  }

  if (!res.ok) {
    const text = await res.text();
    debugLog("rpc", "http failure", {
      status: res.status,
      durationMs: Math.round(performance.now() - started),
      body: text.slice(0, 500),
    });
    throw new Error(`RPC HTTP ${res.status}: ${text}`);
  }

  const json = await res.json();
  debugLog("rpc", "response received", {
    status: res.status,
    durationMs: Math.round(performance.now() - started),
  });
  return json as T;
}

async function rpcCall<T>(
  rpcUrl: string,
  method: string,
  params: unknown[],
  signal?: AbortSignal,
): Promise<T> {
  const request: JsonRpcRequest = {
    jsonrpc: "2.0",
    id: 1,
    method,
    params,
  };
  const response = await rpcPost<JsonRpcResponse<T>>(rpcUrl, request, signal);
  if (response.error) {
    throw new Error(
      `RPC error ${response.error.code}: ${response.error.message}`,
    );
  }
  return response.result as T;
}

// ── Token-bucket rate limiter (FIFO-serialized) ──

class TokenBucket {
  private tokens: number;
  private readonly maxTokens: number;
  private readonly refillRatePerMs: number;
  private lastRefill: number;
  private readonly queue: Array<() => void> = [];
  private draining = false;

  constructor(rps: number) {
    this.maxTokens = rps;
    this.tokens = rps;
    this.refillRatePerMs = rps / 1000;
    this.lastRefill = performance.now();
  }

  private refill(): void {
    const now = performance.now();
    const elapsed = now - this.lastRefill;
    this.tokens = Math.min(this.maxTokens, this.tokens + elapsed * this.refillRatePerMs);
    this.lastRefill = now;
  }

  async acquire(): Promise<void> {
    this.refill();
    if (this.tokens >= 1 && this.queue.length === 0) {
      this.tokens -= 1;
      return;
    }
    return new Promise<void>((resolve) => {
      this.queue.push(resolve);
      this.scheduleDrain();
    });
  }

  private scheduleDrain(): void {
    if (this.draining) return;
    this.draining = true;
    const tick = () => {
      this.refill();
      while (this.queue.length > 0 && this.tokens >= 1) {
        this.tokens -= 1;
        this.queue.shift()!();
      }
      if (this.queue.length > 0) {
        const waitMs = Math.max(1, Math.ceil((1 - this.tokens) / this.refillRatePerMs));
        setTimeout(tick, waitMs);
      } else {
        this.draining = false;
      }
    };
    setTimeout(tick, 0);
  }
}

let globalBucket: TokenBucket | null = null;

export function initRateLimiter(rps: number): void {
  globalBucket = new TokenBucket(rps);
  debugLog("rpc", "rate limiter initialized", { rps });
}

// ── Parallel dispatch with concurrency control ──

export async function parallelMap<A, B>(
  items: A[],
  concurrency: number,
  fn: (item: A) => Promise<B>,
  signal?: AbortSignal,
): Promise<B[]> {
  const results: B[] = new Array(items.length);
  let nextIdx = 0;

  async function worker(): Promise<void> {
    while (nextIdx < items.length) {
      checkSignal(signal);
      const idx = nextIdx++;
      results[idx] = await fn(items[idx]);
    }
  }

  const workers = Array.from(
    { length: Math.min(concurrency, items.length) },
    () => worker(),
  );
  await Promise.all(workers);
  return results;
}

// ── Retry wrapper ──

async function withRetry<T>(
  fn: () => Promise<T>,
  maxRetries = 5,
  baseDelayMs = 300,
  signal?: AbortSignal,
): Promise<T> {
  let lastErr: unknown;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    checkSignal(signal);
    try {
      if (attempt > 0) {
        debugLog("rpc", "retry attempt", { attempt, maxRetries });
      }
      return await fn();
    } catch (err) {
      if (err instanceof CancelledError || (err instanceof Error && err.name === "AbortError")) throw err;
      lastErr = err;
      if (attempt < maxRetries) {
        let delay: number;
        if (err instanceof RateLimitError) {
          delay = err.retryAfterMs + Math.random() * 500;
        } else {
          delay = baseDelayMs * 2 ** attempt + Math.random() * 100;
        }
        globalRetryCount++;
        globalRetryTimeMs += delay;
        debugLog("rpc", "retry scheduled", {
          attempt,
          delayMs: Math.round(delay),
          reason:
            err instanceof Error
              ? `${err.name}: ${err.message}`
              : "unknown error",
        });
        await new Promise((r) => setTimeout(r, delay));
      }
    }
  }
  throw lastErr;
}

// ── Rate-limit tier detection ──

export interface TierConfig {
  tier: "free" | "developer" | "business" | "professional";
  rps: number;
  concurrency: number;
  maxTransactions: number;
}

const TIERS: TierConfig[] = [
  { tier: "free",         rps: 10,  concurrency: 8,   maxTransactions: 10_000 },
  { tier: "developer",    rps: 50,  concurrency: 40,  maxTransactions: 50_000 },
  { tier: "business",     rps: 200, concurrency: 197, maxTransactions: 250_000 },
  { tier: "professional", rps: 500, concurrency: 497, maxTransactions: 500_000 },
];

export function detectTier(): TierConfig {
  const override = process.env.HELIUS_TIER?.trim().toLowerCase();
  if (override) {
    const match = TIERS.find((t) => t.tier === override);
    if (match) {
      debugLog("tier", "tier configured", { ...match });
      return match;
    }
  }
  debugLog("tier", "defaulting to developer tier");
  return TIERS[1];
}

// ── Public RPC methods ──

export async function accountExists(
  rpcUrl: string,
  address: string,
  signal?: AbortSignal,
): Promise<boolean> {
  const result = await withRetry(() =>
    rpcCall<{ value: unknown | null }>(rpcUrl, "getAccountInfo", [
      address,
      { commitment: "finalized", encoding: "base64", dataSlice: { offset: 0, length: 0 } },
    ], signal),
    5, 300, signal,
  );
  return result.value !== null;
}

export async function getBalance(
  rpcUrl: string,
  address: string,
  signal?: AbortSignal,
): Promise<GetBalanceResult> {
  return withRetry(() =>
    rpcCall<GetBalanceResult>(rpcUrl, "getBalance", [
      address,
      { commitment: "finalized" },
    ], signal),
    5, 300, signal,
  );
}

export interface ProbeOptions {
  range: SlotRange;
  sortOrder?: "asc" | "desc";
  limit?: number;
  paginationToken?: string;
}

export async function probeSignatures(
  rpcUrl: string,
  address: string,
  opts: ProbeOptions,
  signal?: AbortSignal,
): Promise<SignaturesResult> {
  const params: Record<string, unknown> = {
    transactionDetails: "signatures",
    sortOrder: opts.sortOrder ?? "desc",
    limit: opts.limit ?? 101,
    commitment: "finalized",
    filters: {
      status: "succeeded",
      slot: { gte: opts.range.gte, lte: opts.range.lte },
    },
  };
  if (opts.paginationToken) {
    params.paginationToken = opts.paginationToken;
  }
  return withRetry(() =>
    rpcCall<SignaturesResult>(rpcUrl, "getTransactionsForAddress", [
      address,
      params,
    ], signal),
    5, 300, signal,
  );
}

export async function fetchFull(
  rpcUrl: string,
  address: string,
  range: SlotRange,
  paginationToken?: string,
  signal?: AbortSignal,
): Promise<FullTransactionsResult> {
  const params: Record<string, unknown> = {
    transactionDetails: "full",
    limit: 100,
    commitment: "finalized",
    maxSupportedTransactionVersion: 0,
    encoding: "json",
    filters: {
      status: "succeeded",
      slot: { gte: range.gte, lte: range.lte },
    },
  };
  if (paginationToken) {
    params.paginationToken = paginationToken;
  }
  return withRetry(() =>
    rpcCall<FullTransactionsResult>(rpcUrl, "getTransactionsForAddress", [
      address,
      params,
    ], signal),
    5, 300, signal,
  );
}

export async function fetchFullUnbounded(
  rpcUrl: string,
  address: string,
  paginationToken?: string,
  signal?: AbortSignal,
): Promise<FullTransactionsResult> {
  const params: Record<string, unknown> = {
    transactionDetails: "full",
    sortOrder: "desc",
    limit: 100,
    commitment: "finalized",
    maxSupportedTransactionVersion: 0,
    encoding: "json",
    filters: { status: "succeeded" },
  };
  if (paginationToken) {
    params.paginationToken = paginationToken;
  }
  return withRetry(() =>
    rpcCall<FullTransactionsResult>(rpcUrl, "getTransactionsForAddress", [
      address,
      params,
    ], signal),
    5, 300, signal,
  );
}

// ── Batch getTransaction (JSON-RPC array batching) ──

export interface GetTransactionResult {
  slot: number;
  blockTime: number | null;
  transaction: {
    message: {
      accountKeys: (string | { pubkey: string })[];
    };
    signatures: string[];
  };
  meta: {
    err: unknown | null;
    fee: number;
    preBalances: number[];
    postBalances: number[];
    loadedAddresses?: {
      readonly: string[];
      writable: string[];
    };
  } | null;
}

const BATCH_LIMIT = 240;

export { BATCH_LIMIT };

export async function batchGetTransaction(
  rpcUrl: string,
  signatures: string[],
): Promise<(GetTransactionResult | null)[]> {
  const batch = signatures.map((sig, i) => ({
    jsonrpc: "2.0" as const,
    id: i,
    method: "getTransaction",
    params: [
      sig,
      {
        encoding: "json",
        maxSupportedTransactionVersion: 0,
        commitment: "finalized",
      },
    ],
  }));

  return withRetry(async () => {
    if (globalBucket) await globalBucket.acquire();
    globalCallCount++;
    const started = performance.now();
    debugLog("rpc", "dispatch batch getTransaction", { count: batch.length });

    const res = await fetch(rpcUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(batch),
    });

    if (res.status === 429) {
      const retryAfter = res.headers.get("retry-after");
      const delayMs = retryAfter ? parseInt(retryAfter, 10) * 1000 : 2000;
      globalCallCount--;
      throw new RateLimitError("Rate limited", delayMs);
    }

    if (!res.ok) {
      const text = await res.text();
      globalCallCount--;
      throw new Error(`RPC HTTP ${res.status}: ${text}`);
    }

    const json = (await res.json()) as JsonRpcResponse<GetTransactionResult>[];
    debugLog("rpc", "batch response received", {
      count: json.length,
      durationMs: Math.round(performance.now() - started),
    });

    json.sort((a, b) => a.id - b.id);
    return json.map((r) => r.result ?? null);
  }, 10, 500);
}

export async function singleGetTransaction(
  rpcUrl: string,
  signature: string,
): Promise<GetTransactionResult | null> {
  return withRetry(() =>
    rpcCall<GetTransactionResult | null>(rpcUrl, "getTransaction", [
      signature,
      {
        encoding: "json",
        maxSupportedTransactionVersion: 0,
        commitment: "finalized",
      },
    ]),
  );
}

// ── Multi-point density estimation ──

export interface DensitySample {
  label: string;
  slotGte: number;
  slotLte: number;
  sigCount: number;
  density: number;
}

export interface DensityEstimate {
  samples: DensitySample[];
  blendedDensity: number;
  estimatedTotalTxs: number;
}

export async function estimateDensity(
  rpcUrl: string,
  address: string,
  fullRange: { gte: number; lte: number },
  recentSigSlots: number[],
  signal?: AbortSignal,
): Promise<DensityEstimate> {
  const fullSpan = fullRange.lte - fullRange.gte;
  if (fullSpan <= 0) return { samples: [], blendedDensity: 0, estimatedTotalTxs: 0 };

  const samples: DensitySample[] = [];

  if (recentSigSlots.length >= 2) {
    const newest = recentSigSlots[0];
    const oldest = recentSigSlots[recentSigSlots.length - 1];
    const span = newest - oldest;
    if (span > 0) {
      samples.push({
        label: "recent",
        slotGte: oldest,
        slotLte: newest,
        sigCount: recentSigSlots.length,
        density: recentSigSlots.length / span,
      });
    }
  }

  const probePoints = [0.25, 0.5, 0.75];
  const windowSize = Math.max(1, Math.floor(fullSpan * 0.05));

  const probeRanges = probePoints.map((pct) => {
    const center = fullRange.gte + Math.floor(fullSpan * pct);
    return {
      label: `p${Math.round(pct * 100)}`,
      gte: Math.max(fullRange.gte, center - Math.floor(windowSize / 2)),
      lte: Math.min(fullRange.lte, center + Math.floor(windowSize / 2)),
    };
  });

  const probeResults = await parallelMap(
    probeRanges,
    probeRanges.length,
    async (probe) => {
      const result = await probeSignatures(rpcUrl, address, {
        range: { gte: probe.gte, lte: probe.lte },
        limit: 1000,
      }, signal);
      return { label: probe.label, gte: probe.gte, lte: probe.lte, count: result.data.length };
    },
    signal,
  );

  for (const r of probeResults) {
    const span = r.lte - r.gte;
    if (span > 0) {
      samples.push({
        label: r.label,
        slotGte: r.gte,
        slotLte: r.lte,
        sigCount: r.count,
        density: r.count / span,
      });
    }
  }

  const validSamples = samples.filter((s) => s.density > 0);
  const blendedDensity = validSamples.length > 0
    ? validSamples.reduce((sum, s) => sum + s.density, 0) / validSamples.length
    : 0;
  const estimatedTotalTxs = Math.round(blendedDensity * fullSpan);

  debugLog("density", "multi-point estimate", {
    samples: samples.map((s) => ({ label: s.label, sigs: s.sigCount, density: s.density.toFixed(6) })),
    blendedDensity: blendedDensity.toFixed(6),
    estimatedTotalTxs,
  });

  return { samples, blendedDensity, estimatedTotalTxs };
}

export async function getBoundsAndBalance(
  rpcUrl: string,
  address: string,
  signal?: AbortSignal,
): Promise<{
  balance: number;
  firstSlot: number | null;
  lastSlot: number | null;
  firstBlockTime: number | null;
  lastBlockTime: number | null;
  totalSigCount: number;
  sigSlots: number[];
}> {
  debugLog("bounds", "fetching balance and slot bounds");
  const BOUNDS_PROBE_LIMIT = 1000;
  const [balanceResult, firstResult, lastResult] = await Promise.all([
    getBalance(rpcUrl, address, signal),
    withRetry(() =>
      rpcCall<SignaturesResult>(rpcUrl, "getTransactionsForAddress", [
        address,
        {
          transactionDetails: "signatures",
          sortOrder: "asc",
          limit: 1,
          commitment: "finalized",
          filters: { status: "succeeded" },
        },
      ], signal),
      5, 300, signal,
    ),
    withRetry(() =>
      rpcCall<SignaturesResult>(rpcUrl, "getTransactionsForAddress", [
        address,
        {
          transactionDetails: "signatures",
          sortOrder: "desc",
          limit: BOUNDS_PROBE_LIMIT,
          commitment: "finalized",
          filters: { status: "succeeded" },
        },
      ], signal),
      5, 300, signal,
    ),
  ]);

  const first = firstResult.data[0] ?? null;
  const last = lastResult.data[0] ?? null;
  const totalSigCount = lastResult.data.length;
  const sigSlots = lastResult.data.map((s) => s.slot);
  debugLog("bounds", "bounds resolved", {
    firstSlot: first?.slot ?? null,
    lastSlot: last?.slot ?? null,
    balance: balanceResult.value,
    totalSigCount,
  });

  return {
    balance: balanceResult.value,
    firstSlot: first?.slot ?? null,
    lastSlot: last?.slot ?? null,
    firstBlockTime: first?.blockTime ?? null,
    lastBlockTime: last?.blockTime ?? null,
    totalSigCount,
    sigSlots,
  };
}

import {
  getBoundsAndBalance,
  fetchFull,
  getCallCount,
  resetCallCount,
  detectTier,
  initRateLimiter,
} from "./src/rpc.js";
import { reconstructBalance, validateBalance } from "./src/balance.js";
import type { SlotRange, BalanceTx, FullTransaction } from "./src/types.js";

const RPC_URL = process.env.HELIUS_RPC_URL!;
if (!RPC_URL) { console.error("HELIUS_RPC_URL required"); process.exit(1); }

const tier = detectTier();
initRateLimiter(tier.rps);
const CONCURRENCY = tier.concurrency;
const FULL_LIMIT = 100;

// ── Helpers ──

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
    if (!tx.meta) continue;
    if (!Array.isArray(tx.meta.preBalances) || !Array.isArray(tx.meta.postBalances)) continue;
    const idx = findAddressIndex(tx, address);
    if (idx === -1) continue;
    if (idx >= tx.meta.preBalances.length || idx >= tx.meta.postBalances.length) continue;
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

// ── Async work queue ──

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

// ── Core: adaptive queue fetch ──

async function adaptiveQueueFetch(
  address: string,
  fullRange: SlotRange,
  currentBalance: number,
  concurrency: number,
  initialChunks = concurrency,
  signal?: AbortSignal,
): Promise<{ txs: BalanceTx[]; calls: number; durationMs: number; valid: boolean }> {
  resetCallCount();
  const started = performance.now();

  const queue = new AsyncQueue<FetchWorkItem>();
  const allTxs: BalanceTx[] = [];
  const seen = new Set<string>();
  let outstanding = 0;

  const enqueue = (item: FetchWorkItem) => { outstanding++; queue.push(item); };
  const finishItem = () => { outstanding--; if (outstanding === 0) queue.close(); };

  signal?.addEventListener("abort", () => queue.close(), { once: true });

  for (const range of splitRange(fullRange, initialChunks)) {
    enqueue({ range, tokenDepth: 0 });
  }

  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      if (signal?.aborted) return;
      const item = await queue.shift();
      if (!item) return;
      try {
        const result = await fetchFull(RPC_URL, address, item.range, item.paginationToken, signal);
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

  allTxs.sort((a, b) => b.slot - a.slot || b.transactionIndex - a.transactionIndex);
  const timeline = reconstructBalance(allTxs, currentBalance, address);
  const valid = validateBalance(timeline);

  return { txs: allTxs, calls: getCallCount(), durationMs: performance.now() - started, valid };
}

// ── Benchmark ──

const WALLETS = [
  { label: "XS",  addr: "ERKj6Kq7nU5LPgxknzXFAwG1RzUrQ1tmxA89KgZTy5Xn" },
  { label: "XS2", addr: "49zbLGHRSUmL9HXjLjodBZTA4tZT2vgBRSX2jsE55Re1" },
  { label: "SM",  addr: "7RXwsoQD6MzsUbwGR2fptPCpi9CFnSLdCYJckb4327as" },
  { label: "SM2", addr: "BFXdmgRTSPaxH9nsa7PYFXz6YFu1MEBps6d962svZKxk" },
  { label: "MD",  addr: "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR" },
  { label: "MD2", addr: "3FpKm9iSie6ug3xcbSPAeTEDGuizEz9CVUyuHn63inXa" },
  { label: "LG",  addr: "CRqRsrFrEsm4BxU5XP2boWoLds9u91KjkhKSQqdwmUh9" },
  { label: "LG2", addr: "6gwTAyKVg1zH77JWhZceuHFXH3bJz8jaRyrYrHYaXuqz" },
];

async function main() {
  console.log(`\nAdaptive Queue Fetch — concurrency ${CONCURRENCY}\n`);
  console.log("Wallet   | Txs      | Calls  | Time    | Valid");
  console.log("---------|----------|--------|---------|------");

  for (const w of WALLETS) {
    resetCallCount();
    const bounds = await getBoundsAndBalance(RPC_URL, w.addr);
    if (!bounds.firstSlot || !bounds.lastSlot) {
      console.log(`${w.label.padEnd(8)} | no txs`);
      continue;
    }
    const fullRange: SlotRange = { gte: bounds.firstSlot, lte: bounds.lastSlot };
    const r = await adaptiveQueueFetch(w.addr, fullRange, bounds.balance, CONCURRENCY);
    const ms = r.durationMs.toFixed(1).padStart(7) + "s";
    console.log(
      `${w.label.padEnd(8)} | ${String(r.txs.length).padStart(8)} | ${String(r.calls).padStart(6)} | ${(r.durationMs / 1000).toFixed(1).padStart(5)}s  | ${r.valid ? "PASS" : "FAIL"}`,
    );
    await new Promise((r) => setTimeout(r, 2000));
  }
}

main().catch(console.error);

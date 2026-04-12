import { computeSolBalanceOverTime } from "./src/algorithm.js";
import { computeSolBalanceOverTimeV2 } from "./src/algorithm2.js";
import { computeSolBalanceOverTimeV3 } from "./src/algorithm3.js";
import { computeSolBalanceOverTimeV3v2 } from "./src/algorithm3v2.js";
import { reconstructBalance, validateBalance } from "./src/balance.js";
import { detectTier, initRateLimiter, resetCallCount } from "./src/rpc.js";
import type { AlgorithmConfig } from "./src/types.js";

const RPC_URL = process.env.HELIUS_RPC_URL!;
if (!RPC_URL) { console.error("HELIUS_RPC_URL required"); process.exit(1); }

const tier = detectTier();
initRateLimiter(tier.rps);
const c = tier.concurrency;

const WALLETS = [
  { label: "XS", addr: "ERKj6Kq7nU5LPgxknzXFAwG1RzUrQ1tmxA89KgZTy5Xn" },
  { label: "XS2", addr: "49zbLGHRSUmL9HXjLjodBZTA4tZT2vgBRSX2jsE55Re1" },
  { label: "SM", addr: "7RXwsoQD6MzsUbwGR2fptPCpi9CFnSLdCYJckb4327as" },
  { label: "SM2", addr: "BFXdmgRTSPaxH9nsa7PYFXz6YFu1MEBps6d962svZKxk" },
  { label: "MD", addr: "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR" },
  { label: "MD2", addr: "3FpKm9iSie6ug3xcbSPAeTEDGuizEz9CVUyuHn63inXa" },
  { label: "LG", addr: "CRqRsrFrEsm4BxU5XP2boWoLds9u91KjkhKSQqdwmUh9" },
  { label: "LG2", addr: "6gwTAyKVg1zH77JWhZceuHFXH3bJz8jaRyrYrHYaXuqz" },
];

const PAUSE_MS = 5000;

const ALGOS = [
  { key: "v1", fn: computeSolBalanceOverTime },
  { key: "v2", fn: computeSolBalanceOverTimeV2 },
  { key: "v3", fn: computeSolBalanceOverTimeV3 },
  { key: "v4", fn: computeSolBalanceOverTimeV3v2 },
] as const;

// Rotate order so each algo gets to go first equally
const ORDERS = [
  [0, 1, 2, 3],  // v1 first
  [1, 2, 3, 0],  // v2 first
  [2, 3, 0, 1],  // v3 first
  [3, 0, 1, 2],  // v4 first
];

async function runAlgo(
  algoFn: typeof computeSolBalanceOverTime,
  config: AlgorithmConfig,
): Promise<{ ms: number; calls: number; txs: number; valid: boolean }> {
  resetCallCount();
  const start = performance.now();
  const result = await algoFn(config);
  const ms = Math.round(performance.now() - start);
  const timeline = reconstructBalance(result.transactions, result.currentBalance, config.address);
  const validation = validateBalance(timeline, result.currentBalance);
  return { ms, calls: result.totalCalls, txs: result.transactions.length, valid: validation.valid };
}

async function main() {
  console.log(`Tier: ${tier.tier} (${c} concurrency)`);
  console.log(`Fair benchmark: each wallet tested 4 times with rotated algo order\n`);

  const col = (pre: string) => `${pre}_ms`.padStart(7) + " " + `${pre}_call`.padStart(7) + " " + `${pre}_txs`.padStart(7);
  const hdr =
    "wallet".padEnd(6) + " | " +
    col("v1") + " | " + col("v2") + " | " + col("v3") + " | " + col("v4") + " | " +
    "v3v4".padStart(5) + " | valid";
  console.log(hdr);
  console.log("-".repeat(hdr.length));

  for (let wi = 0; wi < WALLETS.length; wi++) {
    const { label, addr } = WALLETS[wi];
    const config: AlgorithmConfig = {
      address: addr,
      rpcUrl: RPC_URL,
      initialChunks: c,
      splitFactor: 6,
      concurrencyLimit: c,
      maxTransactions: tier.maxTransactions,
    };

    const keys = ["v1", "v2", "v3", "v4"];
    const totals: Record<string, { ms: number; calls: number; txs: number; valid: boolean; runs: number }> = {};
    for (const k of keys) totals[k] = { ms: 0, calls: 0, txs: 0, valid: true, runs: 0 };

    const order = ORDERS[wi % ORDERS.length];
    for (const idx of order) {
      const algo = ALGOS[idx];
      const r = await runAlgo(algo.fn, config);
      totals[algo.key].ms += r.ms;
      totals[algo.key].calls += r.calls;
      totals[algo.key].txs = r.txs;
      totals[algo.key].valid = totals[algo.key].valid && r.valid;
      totals[algo.key].runs++;
      await new Promise(r => setTimeout(r, PAUSE_MS));
    }

    const avg = (key: string) => ({
      ms: Math.round(totals[key].ms / totals[key].runs),
      calls: Math.round(totals[key].calls / totals[key].runs),
      txs: totals[key].txs,
      valid: totals[key].valid,
    });

    const v1 = avg("v1"), v2 = avg("v2"), v3 = avg("v3"), v4 = avg("v4");

    const d34 = v3.ms > 0 ? Math.round((1 - v4.ms / v3.ms) * 100) : 0;
    const d34s = (d34 > 0 ? `+${d34}%` : `${d34}%`);

    const allValid = keys.every(k => avg(k).valid);
    const validStr = allValid
      ? "PASS"
      : keys.map(k => `${k}:${avg(k).valid ? "ok" : "FAIL"}`).join(" ");

    const fmtCol = (v: { ms: number; calls: number; txs: number }) =>
      String(v.ms).padStart(7) + " " + String(v.calls).padStart(7) + " " + String(v.txs).padStart(7);

    const orderLabel = order.map(i => `v${i + 1}`).join("→");
    console.log(
      label.padEnd(6) + " | " +
      fmtCol(v1) + " | " + fmtCol(v2) + " | " + fmtCol(v3) + " | " + fmtCol(v4) + " | " +
      d34s.padStart(5) + " | " + validStr + ` (${orderLabel})`
    );
  }
}

main().catch(console.error);

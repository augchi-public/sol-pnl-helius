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
  { label: "XS",  addr: "ERKj6Kq7nU5LPgxknzXFAwG1RzUrQ1tmxA89KgZTy5Xn" },
  { label: "XS2", addr: "49zbLGHRSUmL9HXjLjodBZTA4tZT2vgBRSX2jsE55Re1" },
  { label: "SM",  addr: "7RXwsoQD6MzsUbwGR2fptPCpi9CFnSLdCYJckb4327as" },
  { label: "SM2", addr: "BFXdmgRTSPaxH9nsa7PYFXz6YFu1MEBps6d962svZKxk" },
  { label: "MD",  addr: "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR" },
  { label: "MD2", addr: "3FpKm9iSie6ug3xcbSPAeTEDGuizEz9CVUyuHn63inXa" },
  { label: "LG",  addr: "CRqRsrFrEsm4BxU5XP2boWoLds9u91KjkhKSQqdwmUh9" },
  { label: "LG2", addr: "6gwTAyKVg1zH77JWhZceuHFXH3bJz8jaRyrYrHYaXuqz" },
];

const PAUSE_MS = 3000;

type AlgoFn = typeof computeSolBalanceOverTimeV3;

const ALGOS: { key: string; label: string; fn: AlgoFn }[] = [
  { key: "v3",   label: "algo-3",   fn: computeSolBalanceOverTimeV3 },
  { key: "v3v2", label: "algo-3v2", fn: computeSolBalanceOverTimeV3v2 as unknown as AlgoFn },
];

async function runAlgo(fn: AlgoFn, config: AlgorithmConfig) {
  const start = performance.now();
  const result = await fn(config);
  const ms = performance.now() - start;
  const timeline = reconstructBalance(result.transactions, result.currentBalance, config.address);
  const valid = validateBalance(timeline);
  return { ms, calls: result.totalCalls, txs: result.transactions.length, valid };
}

// Rotate execution order to reduce cache bias
const ORDERS = [[0, 1], [1, 0]];

async function main() {
  console.log(`\nalgo-3 vs algo-3v2 — concurrency ${c}\n`);
  console.log(
    "Wallet".padEnd(8) + " | " +
    "Txs".padStart(8) + " | " +
    "algo-3".padStart(8) + " " + "calls".padStart(6) + " | " +
    "algo-3v2".padStart(8) + " " + "calls".padStart(6) + " | " +
    "Valid",
  );
  console.log("-".repeat(72));

  for (let wi = 0; wi < WALLETS.length; wi++) {
    const w = WALLETS[wi];
    const config: AlgorithmConfig = {
      address: w.addr,
      rpcUrl: RPC_URL,
      initialChunks: c,
      splitFactor: 4,
      concurrencyLimit: c,
      maxTransactions: tier.maxTransactions,
    };

    const results: Record<string, { ms: number; calls: number; txs: number; valid: boolean }> = {};
    const order = ORDERS[wi % ORDERS.length];

    for (const idx of order) {
      const algo = ALGOS[idx];
      const r = await runAlgo(algo.fn, config);
      results[algo.key] = r;
      await new Promise((r) => setTimeout(r, PAUSE_MS));
    }

    const v3 = results["v3"];
    const v3v2 = results["v3v2"];
    const allValid = v3.valid && v3v2.valid;

    console.log(
      w.label.padEnd(8) + " | " +
      String(v3.txs).padStart(8) + " | " +
      (v3.ms / 1000).toFixed(1).padStart(6) + "s " + String(v3.calls).padStart(6) + " | " +
      (v3v2.ms / 1000).toFixed(1).padStart(6) + "s " + String(v3v2.calls).padStart(6) + " | " +
      (allValid ? "PASS" : "FAIL"),
    );
  }
}

main().catch(console.error);

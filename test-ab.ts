import { computeSolBalanceOverTime } from "./src/algorithm.js";
import { computeSolBalanceOverTimeV2 } from "./src/algorithm2.js";
import { reconstructBalance, validateBalance } from "./src/balance.js";
import { detectTier, resetCallCount } from "./src/rpc.js";
import type { AlgorithmConfig } from "./src/types.js";

const RPC_URL = process.env.HELIUS_RPC_URL!;
if (!RPC_URL) { console.error("HELIUS_RPC_URL required"); process.exit(1); }

const tier = detectTier();
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

async function runAlgo(
  label: string,
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
  console.log(`Tier: ${tier.tier} (${c} concurrency)\n`);
  console.log(
    "wallet".padEnd(6) + " | " +
    "v1_ms".padStart(7) + " | " + "v1_calls".padStart(8) + " | " + "v1_txs".padStart(8) + " | " +
    "v2_ms".padStart(7) + " | " + "v2_calls".padStart(8) + " | " + "v2_txs".padStart(8) + " | " +
    "diff".padStart(6) + " | valid"
  );
  console.log("-".repeat(95));

  for (const { label, addr } of WALLETS) {
    const config: AlgorithmConfig = {
      address: addr,
      rpcUrl: RPC_URL,
      initialChunks: c,
      splitFactor: 6,
      concurrencyLimit: c,
      maxTransactions: tier.maxTransactions,
    };

    const v1 = await runAlgo("v1", computeSolBalanceOverTime, config);
    await new Promise(r => setTimeout(r, 4000));
    const v2 = await runAlgo("v2", computeSolBalanceOverTimeV2, config);
    await new Promise(r => setTimeout(r, 4000));

    const diff = v1.ms > 0 ? Math.round((1 - v2.ms / v1.ms) * 100) : 0;
    const diffStr = diff > 0 ? `+${diff}%` : `${diff}%`;
    const validStr = v1.valid && v2.valid ? "PASS" : `v1:${v1.valid ? "ok" : "FAIL"} v2:${v2.valid ? "ok" : "FAIL"}`;

    console.log(
      label.padEnd(6) + " | " +
      String(v1.ms).padStart(7) + " | " + String(v1.calls).padStart(8) + " | " + String(v1.txs).padStart(8) + " | " +
      String(v2.ms).padStart(7) + " | " + String(v2.calls).padStart(8) + " | " + String(v2.txs).padStart(8) + " | " +
      diffStr.padStart(6) + " | " + validStr
    );
  }
}

main().catch(console.error);

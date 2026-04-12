import { computeSolBalanceOverTime } from "./src/algorithm.js";
import { computeSolBalanceOverTimeV2 } from "./src/algorithm2.js";
import { computeSolBalanceOverTimeV3 } from "./src/algorithm3.js";
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

const PAUSE_MS = 4000;

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

  const hdr =
    "wallet".padEnd(6) + " | " +
    "v1_ms".padStart(7) + " " + "v1_call".padStart(7) + " " + "v1_txs".padStart(7) + " | " +
    "v2_ms".padStart(7) + " " + "v2_call".padStart(7) + " " + "v2_txs".padStart(7) + " | " +
    "v3_ms".padStart(7) + " " + "v3_call".padStart(7) + " " + "v3_txs".padStart(7) + " | " +
    "v1v3".padStart(5) + " " + "v2v3".padStart(5) + " | valid";
  console.log(hdr);
  console.log("-".repeat(hdr.length));

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
    await new Promise(r => setTimeout(r, PAUSE_MS));
    const v2 = await runAlgo("v2", computeSolBalanceOverTimeV2, config);
    await new Promise(r => setTimeout(r, PAUSE_MS));
    const v3 = await runAlgo("v3", computeSolBalanceOverTimeV3, config);
    await new Promise(r => setTimeout(r, PAUSE_MS));

    const d13 = v1.ms > 0 ? Math.round((1 - v3.ms / v1.ms) * 100) : 0;
    const d23 = v2.ms > 0 ? Math.round((1 - v3.ms / v2.ms) * 100) : 0;
    const d13s = (d13 > 0 ? `+${d13}%` : `${d13}%`);
    const d23s = (d23 > 0 ? `+${d23}%` : `${d23}%`);

    const validStr = v1.valid && v2.valid && v3.valid
      ? "PASS"
      : `v1:${v1.valid?"ok":"FAIL"} v2:${v2.valid?"ok":"FAIL"} v3:${v3.valid?"ok":"FAIL"}`;

    console.log(
      label.padEnd(6) + " | " +
      String(v1.ms).padStart(7) + " " + String(v1.calls).padStart(7) + " " + String(v1.txs).padStart(7) + " | " +
      String(v2.ms).padStart(7) + " " + String(v2.calls).padStart(7) + " " + String(v2.txs).padStart(7) + " | " +
      String(v3.ms).padStart(7) + " " + String(v3.calls).padStart(7) + " " + String(v3.txs).padStart(7) + " | " +
      d13s.padStart(5) + " " + d23s.padStart(5) + " | " + validStr
    );
  }
}

main().catch(console.error);

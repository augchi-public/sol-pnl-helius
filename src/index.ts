import { computeSolBalanceOverTime } from "./algorithm.js";
import { computeSolBalanceOverTimeV2 } from "./algorithm2.js";
import { computeSolBalanceOverTimeV3 } from "./algorithm3.js";
import { computeSolBalanceOverTimeV3v2 } from "./algorithm3v2.js";
import { reconstructBalance, validateBalance } from "./balance.js";
import { detectTier, initRateLimiter } from "./rpc.js";
import type { AlgorithmConfig } from "./types.js";

function loadRpcUrl(): string {
  const key = process.env.HELIUS_API_KEY;
  const url = process.env.HELIUS_RPC_URL;
  if (url) return url;
  if (key) return `https://mainnet.helius-rpc.com/?api-key=${key}`;
  console.error(
    "Set HELIUS_API_KEY or HELIUS_RPC_URL in env (or .env file).",
  );
  process.exit(1);
}

function parseArgs(): {
  address: string;
  initialChunks: number;
  splitFactor: number;
  concurrency: number;
  jsonOutput: boolean;
  algo: 1 | 2 | 3 | 4;
  fromSlot?: number;
  toSlot?: number;
} {
  const args = process.argv.slice(2);
  let address = "";
  let initialChunks = 0;
  let splitFactor = 6;
  let concurrency = 0;
  let jsonOutput = false;
  let algo: 1 | 2 | 3 | 4 = 4;
  let fromSlot: number | undefined;
  let toSlot: number | undefined;

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === "--chunks" && args[i + 1]) {
      initialChunks = parseInt(args[++i], 10);
    } else if (arg === "--split" && args[i + 1]) {
      splitFactor = parseInt(args[++i], 10);
    } else if (arg === "--concurrency" && args[i + 1]) {
      concurrency = parseInt(args[++i], 10);
    } else if (arg === "--from-slot" && args[i + 1]) {
      fromSlot = parseInt(args[++i], 10);
    } else if (arg === "--to-slot" && args[i + 1]) {
      toSlot = parseInt(args[++i], 10);
    } else if (arg === "--algo" && args[i + 1]) {
      const v = parseInt(args[++i], 10);
      algo = (v >= 1 && v <= 4) ? v as 1 | 2 | 3 | 4 : 4;
    } else if (arg === "--json") {
      jsonOutput = true;
    } else if (!arg.startsWith("--")) {
      address = arg;
    }
  }

  if (!address) {
    console.error(
      "Usage: sol-pnl <WALLET_ADDRESS> [--algo 1|2|3|4] [--from-slot N] [--to-slot N] [--chunks N] [--concurrency N] [--json]",
    );
    process.exit(1);
  }

  return { address, initialChunks, splitFactor, concurrency, jsonOutput, algo, fromSlot, toSlot };
}

function formatLamports(lamports: number): string {
  return (lamports / 1e9).toFixed(9) + " SOL";
}

async function main(): Promise<void> {
  // Load .env if dotenv-style vars are present
  const { address, initialChunks, splitFactor, concurrency, jsonOutput, algo, fromSlot, toSlot } =
    parseArgs();
  const rpcUrl = loadRpcUrl();

  const tier = detectTier();
  initRateLimiter(tier.rps);
  const effectiveConcurrency = concurrency > 0 ? Math.min(concurrency, tier.concurrency) : tier.concurrency;

  const effectiveChunks = initialChunks > 0 ? initialChunks : effectiveConcurrency;

  const config: AlgorithmConfig = {
    address,
    rpcUrl,
    initialChunks: effectiveChunks,
    splitFactor,
    concurrencyLimit: effectiveConcurrency,
    maxTransactions: tier.maxTransactions,
    fromSlot,
    toSlot,
  };

  const algoLabels: Record<number, string> = {
    1: "algo-1 (adaptive probe)",
    2: "algo-2 (c×7 drain)",
    3: "algo-3 (sig→full)",
    4: "algo-4 (sig→queue)",
  };
  const algoLabel = algoLabels[algo] ?? algoLabels[4];

  if (!jsonOutput) {
    console.log(`\n  Tier: ${tier.tier.toUpperCase()} (${tier.rps} rps)\n`);
    console.log(`╔══════════════════════════════════════════════════╗`);
    console.log(`║  SOL Balance Over Time — ${algoLabel.padEnd(24)}║`);
    console.log(`╚══════════════════════════════════════════════════╝\n`);
    console.log(`Address:     ${address}`);
    console.log(`Algorithm:   ${algoLabel}`);
    console.log(`Chunks:      ${initialChunks}`);
    console.log(`Concurrency: ${effectiveConcurrency} (tier max: ${tier.concurrency})`);
    console.log(`Max txs:     ${tier.maxTransactions.toLocaleString()}`);
    console.log(`RPC:         ${rpcUrl.replace(/api-key=.*/, "api-key=***")}`);
    console.log(`─────────────────────────────────────────────────────\n`);
  }

  const result = algo === 1
    ? await computeSolBalanceOverTime(config)
    : algo === 2
      ? await computeSolBalanceOverTimeV2(config)
      : algo === 3
        ? await computeSolBalanceOverTimeV3(config)
        : await computeSolBalanceOverTimeV3v2(config);

  const timeline = reconstructBalance(
    result.transactions,
    result.currentBalance,
    address,
  );

  const validation = validateBalance(timeline, result.currentBalance);

  if (jsonOutput) {
    console.log(
      JSON.stringify(
        {
          address,
          currentBalance: result.currentBalance,
          transactionCount: result.transactions.length,
          timelinePoints: timeline.length,
          validation,
          roundStats: result.roundStats,
          totalCalls: result.totalCalls,
          totalDurationMs: Math.round(result.totalDurationMs),
          timeline: timeline.map((p) => ({
            slot: p.slot,
            blockTime: p.blockTime,
            balanceLamports: p.balanceLamports,
            balanceSol: p.balanceLamports / 1e9,
          })),
        },
        null,
        2,
      ),
    );
    return;
  }

  // ── Print round stats ──
  console.log("Round-by-round breakdown:");
  console.log(
    "─────────────────────────────────────────────────────",
  );
  for (const rs of result.roundStats) {
    console.log(
      `  Round ${rs.round}: ${rs.durationMs.toFixed(0).padStart(6)}ms | ` +
        `${rs.callCount} calls | ` +
        `${rs.probes} probes, ${rs.leafFetches} fetches, ` +
        `${rs.splits} splits, ${rs.empties} empty`,
    );
  }
  console.log(
    "─────────────────────────────────────────────────────",
  );
  console.log(
    `  Total:  ${Math.round(result.totalDurationMs)}ms | ${result.totalCalls} RPC calls`,
  );
  console.log(
    `  Transactions found: ${result.transactions.length}`,
  );
  console.log(
    `  Timeline points:    ${timeline.length}`,
  );
  console.log(
    `  Current balance:    ${formatLamports(result.currentBalance)}`,
  );
  if (validation.valid) {
    console.log(`  Validation:         PASS ✓`);
  } else if (result.capped) {
    console.log(
      `  Validation:         APPROXIMATE (capped, off by ${(validation.discrepancyLamports / 1e9).toFixed(4)} SOL)`,
    );
  } else {
    console.log(
      `  Validation:         FAIL ✗ (off by ${validation.discrepancyLamports} lamports)`,
    );
  }

  // ── Print balance timeline (abbreviated) ──
  console.log(
    `\n─────────────────────────────────────────────────────`,
  );
  console.log("Balance timeline (first 10 / last 10):");
  console.log(
    "─────────────────────────────────────────────────────",
  );

  const showCount = 10;
  const show = (points: typeof timeline) => {
    for (const p of points) {
      const time = p.blockTime
        ? new Date(p.blockTime * 1000).toISOString()
        : "unknown";
      console.log(
        `  slot ${String(p.slot).padStart(12)} | ${time} | ${formatLamports(p.balanceLamports)}`,
      );
    }
  };

  if (timeline.length <= showCount * 2) {
    show(timeline);
  } else {
    show(timeline.slice(0, showCount));
    console.log(`  ... ${timeline.length - showCount * 2} more entries ...`);
    show(timeline.slice(-showCount));
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

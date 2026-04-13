import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { computeSolBalanceOverTime } from "./algorithm.js";
import { computeSolBalanceOverTimeV2 } from "./algorithm2.js";
import { computeSolBalanceOverTimeV3 } from "./algorithm3.js";
import { computeSolBalanceOverTimeV3v2 } from "./algorithm3v2.js";
import { reconstructBalance, validateBalance } from "./balance.js";
import { detectTier, initRateLimiter, CancelledError, type TierConfig } from "./rpc.js";
import { debugLog } from "./logger.js";
import type { AlgorithmConfig } from "./types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function loadRpcUrl(): string {
  const key = process.env.HELIUS_API_KEY;
  const url = process.env.HELIUS_RPC_URL;
  if (url) return url;
  if (key) return `https://mainnet.helius-rpc.com/?api-key=${key}`;
  console.error("Set HELIUS_API_KEY or HELIUS_RPC_URL in env.");
  process.exit(1);
}

const RPC_URL = loadRpcUrl();
const PORT = parseInt(process.env.PORT ?? "3000", 10);

let tierConfig: TierConfig | null = null;

async function rpcCall(method: string, params: unknown[]): Promise<unknown> {
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
  });
  const json = (await res.json()) as { result?: unknown; error?: { message: string } };
  if (json.error) throw new Error(json.error.message);
  return json.result;
}

const KNOWN_PROGRAMS = new Set([
  "11111111111111111111111111111111",
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
  "ComputeBudget111111111111111111111111111111",
  "Vote111111111111111111111111111111111111111",
]);

const PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

type BlockTx = {
  meta: { err: unknown };
  transaction: {
    accountKeys: { pubkey: string; signer: boolean; writable: boolean }[];
  };
};

function extractCandidates(
  block: { transactions?: BlockTx[] } | null,
  pumpOnly: boolean,
): string[] {
  if (!block?.transactions?.length) return [];
  const candidates: string[] = [];
  for (const tx of block.transactions) {
    if (tx.meta.err !== null) continue;
    const keys = tx.transaction.accountKeys;
    if (pumpOnly && !keys.some((k) => k.pubkey === PUMPFUN_PROGRAM)) continue;
    const feePayer = keys[0]?.pubkey;
    if (feePayer && !KNOWN_PROGRAMS.has(feePayer)) {
      candidates.push(feePayer);
    }
  }
  return candidates;
}

async function fetchBlock(
  slot: number,
): Promise<{ transactions?: BlockTx[] } | null> {
  return (await rpcCall("getBlock", [
    slot,
    {
      encoding: "json",
      transactionDetails: "accounts",
      rewards: false,
      maxSupportedTransactionVersion: 0,
    },
  ])) as { transactions?: BlockTx[] } | null;
}

async function pickRandomWalletFromChain(): Promise<string> {
  const slot = (await rpcCall("getSlot", [{ commitment: "finalized" }])) as number;

  for (let attempt = 0; attempt < 5; attempt++) {
    const targetSlot = slot - Math.floor(Math.random() * 500);
    try {
      const block = await fetchBlock(targetSlot);
      const candidates = extractCandidates(block, true);
      if (candidates.length > 0) {
        return candidates[Math.floor(Math.random() * candidates.length)];
      }
    } catch {
      continue;
    }
  }

  const fallbackBlock = await fetchBlock(
    slot - Math.floor(Math.random() * 100),
  );
  const any = extractCandidates(fallbackBlock, false);
  if (any.length > 0) return any[Math.floor(Math.random() * any.length)];
  return "vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg";
}

async function handleApi(
  req: http.IncomingMessage,
  res: http.ServerResponse,
): Promise<void> {
  const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);

  if (url.pathname === "/api/tier") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(tierConfig));
    return;
  }

  if (url.pathname === "/api/current-slot") {
    try {
      const slot = (await rpcCall("getSlot", [{ commitment: "finalized" }])) as number;
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ slot }));
    } catch (err) {
      console.error("Current slot error:", err);
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Failed to fetch current slot" }));
    }
    return;
  }

  if (url.pathname === "/api/random-wallet") {
    try {
      const wallet = await pickRandomWalletFromChain();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ address: wallet }));
    } catch (err) {
      console.error("Random wallet error:", err);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ address: "vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg" }));
    }
    return;
  }

  if (url.pathname === "/api/balance-history") {
    const address = url.searchParams.get("address");
    if (!address) {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Missing 'address' parameter" }));
      return;
    }

    const fromSlotParam = url.searchParams.get("from_slot");
    const toSlotParam = url.searchParams.get("to_slot");
    const algoParam = url.searchParams.get("algo");

    const ac = new AbortController();
    res.on("close", () => ac.abort());

    const tier = tierConfig!;
    const config: AlgorithmConfig = {
      address,
      rpcUrl: RPC_URL,
      initialChunks: tier.concurrency,
      splitFactor: 6,
      concurrencyLimit: tier.concurrency,
      maxTransactions: tier.maxTransactions,
      fromSlot: fromSlotParam ? parseInt(fromSlotParam, 10) : undefined,
      toSlot: toSlotParam ? parseInt(toSlotParam, 10) : undefined,
      signal: ac.signal,
    };

    try {
      const result = algoParam === "1"
        ? await computeSolBalanceOverTime(config)
        : algoParam === "2"
          ? await computeSolBalanceOverTimeV2(config)
          : algoParam === "4"
            ? await computeSolBalanceOverTimeV3v2(config)
            : await computeSolBalanceOverTimeV3(config);
      const timeline = reconstructBalance(
        result.transactions,
        result.currentBalance,
        address,
      );
      const validation = validateBalance(timeline, result.currentBalance);

      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(
        JSON.stringify({
          address,
          currentBalance: result.currentBalance,
          currentBalanceSol: result.currentBalance / 1e9,
          transactionCount: result.transactions.length,
          timelinePoints: timeline.length,
          capped: result.capped,
          tier: tier.tier,
          validation,
          roundStats: result.roundStats,
          totalCalls: result.totalCalls,
          totalDurationMs: Math.round(result.totalDurationMs),
          retries: result.retries,
          retryTimeMs: result.retryTimeMs,
          timeline: timeline.map((p) => ({
            time: p.blockTime ?? Math.floor(p.slot / 2.5),
            slot: p.slot,
            balanceSol: p.balanceLamports / 1e9,
          })),
        }),
      );
    } catch (err) {
      if (err instanceof CancelledError || (err instanceof Error && err.name === "AbortError")) {
        debugLog("server", "request cancelled by client", { address });
        if (!res.headersSent) res.destroy();
        return;
      }
      console.error("Algorithm error:", err);
      const message = err instanceof Error ? err.message : "Unknown error";
      const isInvalidAddress =
        /WrongSize/i.test(message) ||
        /Invalid param/i.test(message) ||
        /invalid.*address/i.test(message);
      if (!res.headersSent) {
        res.writeHead(isInvalidAddress ? 400 : 500, {
          "Content-Type": "application/json",
        });
        res.end(
          JSON.stringify({
            error: isInvalidAddress
              ? "Invalid Solana wallet address. Please check and try again."
              : message,
          }),
        );
      }
    }
    return;
  }

  if (url.pathname === "/" || url.pathname === "/index.html") {
    const htmlPath = path.join(__dirname, "..", "public", "index.html");
    try {
      const html = fs.readFileSync(htmlPath, "utf-8");
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end(html);
    } catch {
      res.writeHead(404);
      res.end("Frontend not found");
    }
    return;
  }

  res.writeHead(404);
  res.end("Not found");
}

async function main() {
  tierConfig = detectTier();
  initRateLimiter(tierConfig.rps);
  console.log(`\n  Tier: ${tierConfig.tier.toUpperCase()} (${tierConfig.rps} rps)`);
  console.log(`  → concurrency: ${tierConfig.concurrency}, max txs: ${tierConfig.maxTransactions.toLocaleString()}`);
  console.log(`  → set HELIUS_TIER env to override (default: developer)\n`);

  const server = http.createServer((req, res) => {
    handleApi(req, res).catch((err) => {
      console.error("Unhandled:", err);
      res.writeHead(500);
      res.end("Internal server error");
    });
  });

  server.listen(PORT, () => {
    console.log(`  SOL Balance Viewer running at http://localhost:${PORT}\n`);
  });
}

main();

/**
 * Smoke test: verify getTransactionsForAddress behavior before running
 * the full algorithm. Tests slot filters, signatures vs full mode,
 * response shape, and batch behavior.
 */

import {
  getBoundsAndBalance,
  probeSignatures,
  fetchFull,
} from "./rpc.js";

function loadRpcUrl(): string {
  const key = process.env.HELIUS_API_KEY;
  const url = process.env.HELIUS_RPC_URL;
  if (url) return url;
  if (key) return `https://mainnet.helius-rpc.com/?api-key=${key}`;
  console.error("Set HELIUS_API_KEY or HELIUS_RPC_URL");
  process.exit(1);
}

const TEST_ADDRESS =
  process.argv[2] || "vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg";

async function main(): Promise<void> {
  const rpcUrl = loadRpcUrl();
  console.log(`Smoke testing with address: ${TEST_ADDRESS}\n`);

  // Test 1: Bounds + balance
  console.log("── Test 1: getBoundsAndBalance ──");
  const bounds = await getBoundsAndBalance(rpcUrl, TEST_ADDRESS);
  console.log("  Balance:", bounds.balance, "lamports");
  console.log("  First slot:", bounds.firstSlot);
  console.log("  Last slot:", bounds.lastSlot);
  console.log("  First blockTime:", bounds.firstBlockTime);
  console.log("  Last blockTime:", bounds.lastBlockTime);

  if (bounds.firstSlot === null || bounds.lastSlot === null) {
    console.log("\n  No transactions found. Skipping remaining tests.");
    return;
  }

  // Test 2: Signatures probe with slot filter
  console.log("\n── Test 2: probeSignatures with slot filter ──");
  const mid = Math.floor((bounds.firstSlot + bounds.lastSlot) / 2);
  const probeResult = await probeSignatures(rpcUrl, TEST_ADDRESS, {
    range: { gte: bounds.firstSlot, lte: mid },
    limit: 101,
  });
  console.log("  Range:", bounds.firstSlot, "to", mid);
  console.log("  Count:", probeResult.data.length);
  console.log("  PaginationToken:", probeResult.paginationToken);
  if (probeResult.data.length > 0) {
    const first = probeResult.data[0];
    console.log("  First entry:", {
      signature: first.signature.slice(0, 20) + "...",
      slot: first.slot,
      transactionIndex: first.transactionIndex,
      blockTime: first.blockTime,
    });
  }

  // Test 3: Full fetch with slot filter
  console.log("\n── Test 3: fetchFull with slot filter ──");
  const narrowRange = {
    gte: bounds.firstSlot,
    lte: Math.min(bounds.firstSlot + 100000, bounds.lastSlot),
  };
  const fullResult = await fetchFull(rpcUrl, TEST_ADDRESS, narrowRange);
  console.log("  Range:", narrowRange.gte, "to", narrowRange.lte);
  console.log("  Count:", fullResult.data.length);
  console.log("  PaginationToken:", fullResult.paginationToken);
  if (fullResult.data.length > 0) {
    const tx = fullResult.data[0];
    console.log("  First tx shape check:");
    console.log("    slot:", tx.slot);
    console.log("    transactionIndex:", tx.transactionIndex);
    console.log("    blockTime:", tx.blockTime);
    console.log("    signature:", tx.transaction.signatures[0]?.slice(0, 20) + "...");
    console.log(
      "    accountKeys count:",
      tx.transaction.message.accountKeys.length,
    );
    console.log("    preBalances count:", tx.meta.preBalances.length);
    console.log("    postBalances count:", tx.meta.postBalances.length);
    console.log("    fee:", tx.meta.fee);
    console.log("    err:", tx.meta.err);

    // Verify accountKeys format
    const firstKey = tx.transaction.message.accountKeys[0];
    console.log(
      "    accountKey[0] type:",
      typeof firstKey === "string" ? "string" : "object",
    );
    if (typeof firstKey !== "string") {
      console.log("    accountKey[0]:", firstKey);
    }
  }

  // Test 4: JSON-RPC batch behavior
  console.log("\n── Test 4: JSON-RPC batch request ──");
  try {
    const batchBody = [
      {
        jsonrpc: "2.0",
        id: 1,
        method: "getBalance",
        params: [TEST_ADDRESS, { commitment: "finalized" }],
      },
      {
        jsonrpc: "2.0",
        id: 2,
        method: "getTransactionsForAddress",
        params: [
          TEST_ADDRESS,
          {
            transactionDetails: "signatures",
            sortOrder: "asc",
            limit: 1,
            commitment: "finalized",
            filters: { status: "any" },
          },
        ],
      },
    ];
    const res = await fetch(rpcUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(batchBody),
    });
    const json = await res.json();
    const isBatchResponse = Array.isArray(json);
    console.log("  Batch supported:", isBatchResponse);
    if (isBatchResponse) {
      console.log("  Response count:", json.length);
      for (const r of json) {
        console.log(`    id=${r.id}: ${r.error ? "ERROR: " + r.error.message : "OK"}`);
      }
    } else {
      console.log("  Response:", JSON.stringify(json).slice(0, 200));
    }
  } catch (err) {
    console.log("  Batch request failed:", err);
  }

  console.log("\n── All smoke tests complete ──");
}

main().catch((err) => {
  console.error("Smoke test failed:", err);
  process.exit(1);
});

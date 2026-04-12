/**
 * Test whether Helius supports JSON-RPC batching for gTFA calls.
 * Sends multiple gTFA requests in a single HTTP request body.
 */
const RPC_URL = process.env.HELIUS_RPC_URL!;
if (!RPC_URL) { console.error("HELIUS_RPC_URL required"); process.exit(1); }

const address = "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR";

async function testBatch() {
  const batch = Array.from({ length: 5 }, (_, i) => ({
    jsonrpc: "2.0" as const,
    id: i,
    method: "getTransactionsForAddress",
    params: [address, {
      transactionDetails: "signatures",
      sortOrder: "asc",
      limit: 10,
      commitment: "finalized",
      filters: { status: "succeeded" },
    }],
  }));

  console.log("Sending batch of 5 gTFA calls in one HTTP request...");
  const start = performance.now();
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(batch),
  });

  const ms = Math.round(performance.now() - start);
  console.log(`HTTP status: ${res.status} (${ms}ms)`);

  const text = await res.text();
  try {
    const json = JSON.parse(text);
    if (Array.isArray(json)) {
      console.log(`\nBatch SUPPORTED! Got ${json.length} responses.`);
      for (const r of json) {
        const sigs = r.result?.data?.length ?? 0;
        const err = r.error?.message ?? "none";
        console.log(`  id=${r.id}: ${sigs} sigs, error: ${err}`);
      }
    } else if (json.error) {
      console.log(`\nBatch NOT supported. Single error response:`);
      console.log(`  code=${json.error.code}: ${json.error.message}`);
    } else {
      console.log(`\nUnexpected response format (single object, not array):`);
      console.log(JSON.stringify(json).slice(0, 500));
    }
  } catch {
    console.log(`\nFailed to parse response:`);
    console.log(text.slice(0, 500));
  }

  // Compare: 5 individual calls
  console.log("\n--- Comparison: 5 individual calls ---");
  const start2 = performance.now();
  const results = await Promise.all(
    Array.from({ length: 5 }, () =>
      fetch(RPC_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          jsonrpc: "2.0", id: 1,
          method: "getTransactionsForAddress",
          params: [address, {
            transactionDetails: "signatures",
            sortOrder: "asc",
            limit: 10,
            commitment: "finalized",
            filters: { status: "succeeded" },
          }],
        }),
      }).then(r => r.json())
    )
  );
  const ms2 = Math.round(performance.now() - start2);
  console.log(`5 parallel individual calls: ${ms2}ms`);
  console.log(`Batch: ${ms}ms | Individual: ${ms2}ms | Diff: ${ms2 - ms}ms`);
}

testBatch().catch(console.error);

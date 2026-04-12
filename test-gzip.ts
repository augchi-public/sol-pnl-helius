const RPC_URL = process.env.HELIUS_RPC_URL!;
if (!RPC_URL) { console.error("HELIUS_RPC_URL required"); process.exit(1); }

const address = "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR";

const body = JSON.stringify({
  jsonrpc: "2.0", id: 1,
  method: "getTransactionsForAddress",
  params: [address, {
    transactionDetails: "full",
    limit: 100,
    commitment: "finalized",
    maxSupportedTransactionVersion: 0,
    encoding: "json",
    filters: { status: "succeeded" },
  }],
});

async function fetchWithout() {
  const start = performance.now();
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
  const data = await res.text();
  const ms = Math.round(performance.now() - start);
  console.log(`No gzip:   ${ms}ms | ${(data.length / 1024).toFixed(0)}KB | Content-Encoding: ${res.headers.get("content-encoding") ?? "none"}`);
}

async function fetchWith() {
  const start = performance.now();
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept-Encoding": "gzip",
    },
    body,
  });
  const data = await res.text();
  const ms = Math.round(performance.now() - start);
  console.log(`With gzip: ${ms}ms | ${(data.length / 1024).toFixed(0)}KB | Content-Encoding: ${res.headers.get("content-encoding") ?? "none"}`);
}

async function main() {
  console.log("── Round 1 (cold) ──");
  await fetchWithout();
  await fetchWith();

  await new Promise(r => setTimeout(r, 2000));

  console.log("\n── Round 2 (warm) ──");
  await fetchWithout();
  await fetchWith();

  await new Promise(r => setTimeout(r, 2000));

  console.log("\n── Round 3 ──");
  await fetchWithout();
  await fetchWith();
}

main().catch(console.error);

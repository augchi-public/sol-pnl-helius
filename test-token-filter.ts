const RPC_URL = process.env.HELIUS_RPC_URL!;
if (!RPC_URL) { console.error("HELIUS_RPC_URL required"); process.exit(1); }

const address = "Az1P8fRm5FKLPqJv4cMrzfjWtYBDNsg6EypZ5CctsdCR";

async function call(label: string, filters: Record<string, unknown>) {
  const body = JSON.stringify({
    jsonrpc: "2.0", id: 1,
    method: "getTransactionsForAddress",
    params: [address, {
      transactionDetails: "full",
      limit: 100,
      commitment: "finalized",
      maxSupportedTransactionVersion: 0,
      encoding: "json",
      filters,
    }],
  });

  const start = performance.now();
  const res = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body,
  });
  const text = await res.text();
  const ms = Math.round(performance.now() - start);
  const kb = Math.round(text.length / 1024);
  const json = JSON.parse(text);
  const txs = json.result?.data?.length ?? 0;
  console.log(`${label.padEnd(30)} ${ms}ms | ${kb}KB | ${txs} txs`);
  return { ms, kb, txs };
}

async function main() {
  console.log("=== Full mode (100 txs) ===\n");

  // Warm up
  await call("warmup", { status: "succeeded" });
  await new Promise(r => setTimeout(r, 1000));

  // Without tokenAccounts filter
  const r1 = await call("status:succeeded", { status: "succeeded" });
  await new Promise(r => setTimeout(r, 1000));

  // With tokenAccounts: "none"
  const r2 = await call("status:succeeded + tokAcct:none", { status: "succeeded", tokenAccounts: "none" });
  await new Promise(r => setTimeout(r, 1000));

  // Repeat reversed order
  const r3 = await call("status:succeeded + tokAcct:none", { status: "succeeded", tokenAccounts: "none" });
  await new Promise(r => setTimeout(r, 1000));

  const r4 = await call("status:succeeded", { status: "succeeded" });

  console.log("\n=== Signatures mode (1000 sigs) ===\n");

  const sigBody = (filters: Record<string, unknown>) => JSON.stringify({
    jsonrpc: "2.0", id: 1,
    method: "getTransactionsForAddress",
    params: [address, {
      transactionDetails: "signatures",
      limit: 1000,
      commitment: "finalized",
      sortOrder: "desc",
      filters,
    }],
  });

  for (const [label, filters] of [
    ["sigs: status only", { status: "succeeded" }],
    ["sigs: status + tokAcct:none", { status: "succeeded", tokenAccounts: "none" }],
  ] as const) {
    const start = performance.now();
    const res = await fetch(RPC_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: sigBody(filters as Record<string, unknown>),
    });
    const text = await res.text();
    const ms = Math.round(performance.now() - start);
    const kb = Math.round(text.length / 1024);
    const json = JSON.parse(text);
    const sigs = json.result?.data?.length ?? 0;
    console.log(`${label.padEnd(30)} ${ms}ms | ${kb}KB | ${sigs} sigs`);
    await new Promise(r => setTimeout(r, 1000));
  }
}

main().catch(console.error);

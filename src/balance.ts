import type { BalanceTx, BalancePoint } from "./types.js";

/**
 * Reconstruct SOL balance over time from slim balance records and a known
 * current balance anchor.
 *
 * Sort transactions chronologically by (slot, transactionIndex), then emit
 * preBalance/postBalance at each step. The last postBalance should match
 * the current on-chain balance.
 */
export function reconstructBalance(
  transactions: BalanceTx[],
  currentBalance: number,
  _address: string,
): BalancePoint[] {
  if (transactions.length === 0) {
    return [{ slot: 0, blockTime: null, balanceLamports: currentBalance }];
  }

  // Deduplicate by signature (pagination overlap can cause duplicates)
  const seen = new Set<string>();
  const deduped: BalanceTx[] = [];
  for (const tx of transactions) {
    if (!seen.has(tx.signature)) {
      seen.add(tx.signature);
      deduped.push(tx);
    }
  }

  // Sort ascending by (slot, transactionIndex) for chronological order
  deduped.sort((a, b) => {
    if (a.slot !== b.slot) return a.slot - b.slot;
    return a.transactionIndex - b.transactionIndex;
  });

  const timeline: BalancePoint[] = [];

  for (const tx of deduped) {
    timeline.push({
      slot: tx.slot,
      blockTime: tx.blockTime,
      balanceLamports: tx.preBalance,
    });
  }

  // Final point: postBalance of the last tx
  const last = deduped[deduped.length - 1];
  timeline.push({
    slot: last.slot,
    blockTime: last.blockTime,
    balanceLamports: last.postBalance,
  });

  return timeline;
}

/**
 * Validate the reconstructed balance timeline against the known current
 * balance. The last entry's balance should match currentBalance.
 */
export function validateBalance(
  timeline: BalancePoint[],
  currentBalance: number,
): { valid: boolean; discrepancyLamports: number } {
  if (timeline.length === 0) {
    return { valid: true, discrepancyLamports: 0 };
  }
  const lastBalance = timeline[timeline.length - 1].balanceLamports;
  const diff = Math.abs(lastBalance - currentBalance);
  return { valid: diff === 0, discrepancyLamports: diff };
}

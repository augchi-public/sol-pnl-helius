// ── JSON-RPC envelope ──

export interface JsonRpcRequest {
  jsonrpc: "2.0";
  id: number;
  method: string;
  params: unknown[];
}

export interface JsonRpcResponse<T = unknown> {
  jsonrpc: "2.0";
  id: number;
  result?: T;
  error?: { code: number; message: string; data?: unknown };
}

// ── getBalance ──

export interface GetBalanceResult {
  context: { slot: number };
  value: number; // lamports
}

// ── getTransactionsForAddress — signatures mode ──

export interface SignatureEntry {
  signature: string;
  slot: number;
  transactionIndex: number;
  err: unknown | null;
  memo: string | null;
  blockTime: number | null;
  confirmationStatus: string;
}

export interface SignaturesResult {
  data: SignatureEntry[];
  paginationToken: string | null;
}

// ── getTransactionsForAddress — full mode ──

export interface AccountKey {
  pubkey: string;
  signer: boolean;
  source: string;
  writable: boolean;
}

export interface TransactionMeta {
  err: unknown | null;
  fee: number;
  preBalances: number[];
  postBalances: number[];
  preTokenBalances: unknown[];
  postTokenBalances: unknown[];
  logMessages: string[];
  innerInstructions: unknown[];
  computeUnitsConsumed?: number;
  loadedAddresses?: {
    readonly: string[];
    writable: string[];
  };
}

export interface FullTransaction {
  transaction: {
    message: {
      accountKeys: AccountKey[];
      instructions: unknown[];
      recentBlockhash: string;
    };
    signatures: string[];
  };
  meta: TransactionMeta;
  slot: number;
  blockTime: number | null;
  transactionIndex: number;
}

export interface FullTransactionsResult {
  data: FullTransaction[];
  paginationToken: string | null;
}

// ── Slim per-tx record for balance reconstruction (memory-efficient) ──

export interface BalanceTx {
  signature: string;
  slot: number;
  blockTime: number | null;
  transactionIndex: number;
  preBalance: number;  // lamports for the target address
  postBalance: number; // lamports for the target address
}

// ── Slot range used throughout the algorithm ──

export interface SlotRange {
  gte: number; // inclusive lower bound
  lte: number; // inclusive upper bound
}

// ── Range classification after probing ──

export const enum RangeClass {
  EMPTY = 0,
  LEAF = 1,
  SPLIT = 2,
}

export interface ClassifiedRange {
  range: SlotRange;
  classification: RangeClass;
  count: number; // actual count returned by probe (0-1000)
}

// ── Balance timeline output ──

export interface BalancePoint {
  slot: number;
  blockTime: number | null;
  balanceLamports: number;
}

// ── Algorithm config ──

export interface AlgorithmConfig {
  address: string;
  rpcUrl: string;
  initialChunks: number;
  splitFactor: number;
  concurrencyLimit: number;
  maxTransactions: number;
  fromSlot?: number;
  toSlot?: number;
  signal?: AbortSignal;
}

// ── Benchmark stats ──

export interface RoundStats {
  round: number;
  durationMs: number;
  callCount: number;
  probes: number;
  leafFetches: number;
  splits: number;
  empties: number;
}

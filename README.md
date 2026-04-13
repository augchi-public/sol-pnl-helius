# sol-pnl-helius

Reconstructs a Solana wallet's SOL balance over time using Helius `getTransactionsForAddress`. Four algorithm variants with size-aware fast paths, parallel sig sweeps, and bounded concurrency.

## Demo

https://github.com/user-attachments/assets/c255136c-d625-4797-a284-056d8a679a53

## Setup

```bash
npm install
```

Create a `.env` file (see `.env.example`):

```
HELIUS_RPC_URL=https://mainnet.helius-rpc.com/?api-key=your-api-key-here
HELIUS_TIER=developer          # free | developer | business | professional (default: developer)
SOL_PNL_DEBUG=0                # set to 1 for detailed timestamped debug logs
```

The tier controls concurrency and transaction caps:

| Tier | RPS | Concurrency | Max Txs |
|------|-----|-------------|---------|
| free | 10 | 8 | 10K |
| developer | 50 | 40 | 50K |
| business | 200 | 197 | 250K |
| professional | 500 | 497 | 500K |

Concurrency is set to `RPS - 3`, reserving headroom for retries on transient 429s. A global token-bucket rate limiter paces dispatch to the tier's RPS ceiling, preventing bursts from overshooting the limit when responses are fast.

## Usage

### CLI

```bash
export $(cat .env | xargs) && npx tsx src/index.ts <WALLET_ADDRESS>
```

Options:

- `--chunks N` — number of initial probe chunks (default: tier concurrency limit)
- `--concurrency N` — override max concurrent requests
- `--from-slot N` — only scan transactions from this slot onwards
- `--to-slot N` — only scan transactions up to this slot
- `--algo 1|2|3|4` — algorithm version (default: 3)
- `--json` — output results as JSON

### Web UI

```bash
export $(cat .env | xargs) && npm run serve
```

Opens at `http://localhost:3000`. Features:

- Wallet address input with preset wallets (xs/sm/md/lg/xl sizes) and random pump.fun wallet picker
- Algorithm selector: algo-3 (default), algo-4, algo-2, algo-1
- Time filter buttons (1D, 1W, 15D, 1M, 6M, 1Y, All) using slot-based filtering — defaults to 1D
- Interactive SOL balance chart (latest 1K data points when truncated)
- Per-round stats table, validation badge, and tier info
- Cancel button to abort running queries

## Algorithms

All four algorithms share the same bounds discovery and small-wallet fast paths. They differ in how they handle large wallets (1000+ transactions).

### Common steps (all algorithms)

**Round 0 — Bounds + quick size estimate:** Three parallel calls:

- `getBalance` — current SOL balance (anchor for reconstruction)
- gTFA signatures `asc limit=1` — first-ever transaction slot
- gTFA signatures `desc limit=1000` — last transaction slot + up to 1000 recent sig slots

This gives the full slot range and a free transaction count estimate. If time filters are active, an additional probe runs on the filtered range.

For large wallets (1000+ txs, no time filter), a **multi-point density estimation** fires 3 additional `probeSignatures` calls in parallel, sampling 5%-wide windows at the 25%, 50%, and 75% marks of the slot range. Combined with the "recent" sample already in hand from the desc probe, this gives 4 density readings. The blended average drives a more accurate capped-vs-uncapped routing decision — especially for wallets with uneven activity distribution (e.g., active early then quiet, or vice versa). Cost: 3 extra RPC calls, all parallel, ~one RTT of added wall time.

**Size-aware routing:**

- **Tiny** (0–100 txs): single gTFA `full` call on the entire range. ~0.6s.
- **Small** (101–999 txs): exact count known. Builds balanced sub-ranges from actual sig slot positions (each ≤100 txs). Single parallel fetch. ~1–1.5s.
- **Large** (1000+ txs): enters the algorithm-specific pipeline below.

**Balance reconstruction** (all algorithms):

1. Deduplicate by signature
2. Sort by `(slot, transactionIndex)`
3. Walk backwards from current balance using `preBalances`/`postBalances`
4. Resolve ALT-loaded addresses for v0 transactions
5. Filter out zero-delta transactions
6. Validate reconstructed starting balance

### algo-3: Sig Sweep → Targeted Full Fetch (default)

The default algorithm. Two clean phases with predictable dispatch patterns — sweeps signatures first, then fetches full transactions in precisely-sized ranges. Simpler and more stable than algo-4's dynamic queue, with equivalent performance on most wallets.

```
Large wallet (≥1000 txs)
        |
   multi-point density
   estimate (4 samples)
        |
   +----+----+
   |         |
 capped    uncapped
   |         |
 direct   Phase 1: parallel sig sweep
 capped     (197 chunks × 1000 sigs/call,
 fetch       paginated with server tokens)
   |            |
 return   Phase 2: targeted full fetch
            (balanced ranges of ~97 sigs each,
             static parallelMap)
               |
            return
```

**Phase 1 — Parallel sig sweep:** Splits the full slot range into `concurrency` chunks and sweeps all signatures with proper pagination token forwarding.

**Phase 2 — Targeted full fetch:** Groups the known sig slots into ranges of ~97 sigs each using `buildBalancedRanges`. Each range targets ≤100 txs, so nearly every full-fetch call returns a single page. On rare "hot slots" (many txs in one slot), a range may slightly exceed 100 txs and paginate internally — but this is uncommon and handled transparently.

The two key constants (shared with algo-4) are derived from Helius API limits:

- **197 concurrency** = 200 RPS (Business plan) − 3 headroom for retry capacity
- **97 sigs per chunk** = 100 max txs per gTFA full-mode response − 3 headroom (covers most slots; hot slots with many txs in a single slot may still paginate)

**Call count formula:** `3 + 3 + ⌈txs/1000⌉ + ⌈txs/97⌉`

### algo-4: Sig Sweep → Queue Fetch

Builds on algo-3's two-phase design but replaces Phase 2 with a dynamic async work queue. Workers pull ranges from a shared queue; when a fetch returns a full page (100 txs), the worker enqueues a child work item for the remaining range. Hot slots automatically fall back to pagination token continuation.

```
Large wallet (≥1000 txs)
        |
   multi-point density
   estimate (4 samples)
        |
   +----+----+
   |         |
 capped    uncapped
   |         |
 direct   Phase 1: parallel sig sweep
 capped     (197 chunks × 1000 sigs/call,
 fetch       paginated with server tokens)
   |            |
 return   Phase 2: queue-based full fetch
            (initial ranges of ~97 sigs each,
             overflow auto-requeued by workers)
               |
            return
```

**Phase 1 — Parallel sig sweep:** Same as algo-3.

**Phase 2 — Queue-based full fetch:** Initial ranges are built from sig slots (~97 sigs each). Ranges are fed into an `AsyncQueue`. Each worker:
1. Pulls a range from the queue
2. Fetches full transactions (gTFA full mode, 100/call)
3. If the page is full (100 txs returned), builds a child work item:
   - **Normal case:** narrows the range to `[gte, minSlot]` for the older remainder
   - **Hot slot (no slot progress):** continues with the server's `paginationToken`
4. Enqueues the child and immediately pulls the next item

Workers are never idle — dense ranges dynamically spawn more work while sparse ranges complete fast. The dynamic dispatch pattern can create burstier RPC traffic compared to algo-3's static approach, which may trigger more transient connection errors under load.

**Call count formula:** `3 + 3 + ⌈txs/1000⌉ + ⌈txs/97⌉`

### algo-2: c×7 Direct Drain

Simple and effective. Skips probing entirely for large wallets.

```
Large wallet (≥1000 txs)
        |
   multi-point density
   estimate (4 samples)
        |
   +----+----+
   |         |
 capped    uncapped
   |         |
 direct   split into concurrency×7 chunks
 capped     (e.g. 195×7 = 1365 ranges)
 fetch        |
   |       parallel drain, each chunk
 return     paginated internally
               |
            return
```

Splits the full slot range into `concurrency × 7` uniform chunks (e.g. 197×7 = 1,379 ranges). Workers drain from a shared queue — each worker fetches a chunk page-by-page, then grabs the next. Simple load balancing without probing overhead.

Trade-off: some chunks may be empty (wasted calls) and some may need pagination (sequential within that chunk). But the simplicity and immediate start (no sig sweep phase) make it competitive for very large wallets.

### algo-1: Adaptive Iterative Probing

The original algorithm. Most sophisticated, best for wallets with very uneven density.

```
Large wallet (≥1000 txs)
        |
   multi-point density
   estimate (4 samples)
        |
   +----+--------+
   |              |
 capped     iterative probe loop
   |         (up to 8 rounds)
 direct           |
 capped      classify ranges:
 fetch        EMPTY → discard
   |          LEAF → fetch queue
 return       SPLIT (known) → subdivide
              SPLIT (saturated) → re-probe
                    |
               early exits:
               - cap exceeded
               - >50% dense
               - fan-out too large
                    |
              fetch all leaves
                    |
                 return
```

Iteratively probes ranges using gTFA signatures mode, classifying each as EMPTY, LEAF, or SPLIT. Precise ranges mean very few wasted calls, but the multi-round probing adds latency. Best suited for wallets with large empty gaps between activity periods.

## Benchmark comparison

Tested on business tier (197 concurrency, token-bucket paced to 200 RPS), all-time range. Execution order is rotated per wallet to reduce (but not fully eliminate) RPC-side response caching bias — small wallets are especially affected since one cold call dominates.

| Wallet | Txs | algo-3 | algo-3 calls | algo-4 | algo-4 calls |
|--------|-----|--------|-------------|--------|-------------|
| XS (3 txs) | 3 | 0.4s | 4 | 0.2s | 4 |
| XS2 (44 txs) | 44 | 0.3s | 4 | 0.4s | 4 |
| SM (627 txs) | 627 | 1.3s | 15 | 0.9s | 15 |
| SM2 (709 txs) | 709 | 0.5s | 16 | 1.2s | 16 |
| MD (33K txs) | 33,571 | 4.4s | 577 | **3.8s** | 577 |
| MD2 (15K txs) | 15,386 | 3.4s | 386 | 3.2s | 386 |
| LG (116K txs) | 116,825 | 12.0s | 1,462 | 12.4s | 1,462 |
| LG2 (174K txs) | 174,461 | 19.3s | 2,239 | 19.6s | 2,124 |

Small wallets (XS–SM) are noise-dominated — all algorithms share the same fast-path code, and RPC response caching heavily favors whichever algo runs second. Medium wallets (MD) show algo-4's slight edge (3.8s vs 4.4s). Large wallets (100K+) are throughput-bottlenecked by the plan's RPS limit regardless of strategy. Algo-4 uses fewer calls on large wallets (2,124 vs 2,239 on LG2) thanks to the queue avoiding empty range fetches.

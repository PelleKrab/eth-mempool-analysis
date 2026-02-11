# Ethereum FOCIL Censorship Analysis

Analysis framework for studying transaction censorship patterns on Ethereum using Fork-Choice enforced Inclusion Lists (FOCIL) methodology.

## Overview

This project analyzes historical Ethereum mempool data to construct mock inclusion lists and measure how many of those transactions builders naturally include. The analysis produces 6 variants per block: 2 strategies (top fee, censored) evaluated at 3 delay levels (0, 1, 2 slots).

### Inclusion List Strategies

**Top Fee Strategy** - Selects the highest effective-priority-fee EIP-1559 transactions from the mempool window. One IL is built per block, then evaluated at 3 delay levels.

**Censored Strategy** - Flags transactions that meet inclusion criteria but were excluded from blocks (censorship detection). One IL is built per block, then evaluated at 3 delay levels.

### Delay Levels

All ILs are built from block N's perspective:
- **0-delay**: built at N, check inclusion in block N+1
- **1-delay**: built at N, check inclusion in blocks N+1, N+2
- **2-delay**: built at N, check inclusion in blocks N+1, N+2, N+3

Higher inclusion rates at higher delays are expected (more blocks to check).

### Key Features

- EIP-7805 compliant (8 KiB size cap per inclusion list)
- Effective priority fee: `min(priority_fee, max_fee - base_fee)` per EIP-1559
- EIP-1559 (type 2) transaction filter to exclude phantom/spam legacy txs
- Nonce replacement detection to avoid false positive censorship flags
- Dual capacity check (previous and current block gas headroom)
- Results saved in Parquet format

## Architecture

```
ClickHouse (Xatu)       Python Script                   Output
┌─────────────────┐     ┌─────────────────────────┐     ┌──────────────┐
│ mempool_         │────>│ focil_censorship_        │────>│ .parquet     │
│   transaction    │     │   analysis.py            │     │ per-block    │
│ canonical_       │────>│                          │     │ metrics      │
│   beacon_block   │     │ 6 variants per block:    │     └──────────────┘
│ canonical_       │────>│  3 delays x 2 strategies │
│   execution_     │     └─────────────────────────┘
│   transaction    │
└─────────────────┘
```

## Project Structure

```
eth-mempool-analysis/
├── config/
│   └── config.yaml                    Configuration (ClickHouse creds + analysis params)
├── scripts/
│   ├── focil_censorship_analysis.py   Main analysis script
│   ├── batch_runner.py                Parallel batch processing for large ranges
│   ├── combine_chunks.py             Combine chunk parquets into one file
│   └── generate_chunk_ranges.py       Generate chunk ranges CSV for distributed runs
├── results/                           Analysis output (parquet files)
└── README.md
```

## Setup

### Prerequisites

```bash
pip install -r requirements.txt
```

### Configuration

Create a `.env` file in the project root or set environment variables:

```
CLICKHOUSE_URL=https://clickhouse.xatu.ethpandaops.io
CLICKHOUSE_USER=your_username
CLICKHOUSE_PASSWORD=your_password
```

Analysis parameters in `config/config.yaml`:

```yaml
analysis:
  start_block: 21575000
  end_block: 21576000
  time_window_start_secs: -20       # ~2 slots lookback
  time_window_end_secs: 8           # 8s after block timestamp
  censorship_dwell_time_secs: 12    # Minimum mempool dwell (1 slot)
  censorship_fee_percentile: 0.50   # Median fee threshold for censorship
  censorship_percentile_window_secs: 30
  batch_size_blocks: 100
```

## Usage

### Quick Start

```bash
python scripts/focil_censorship_analysis.py
```

### Override block range via CLI

```bash
python scripts/focil_censorship_analysis.py --start 21575000 --end 21576000
```

### Large-Scale Batch Processing

```bash
# Process a large range in parallel chunks
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000 --parallel 4

# Resume (skips already-completed chunks)
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000 --resume

# Combine chunk results into a single file
python scripts/combine_chunks.py
```

## Methodology

### Censorship Detection

Transactions are flagged as censored when all criteria are met:

1. **FOCIL-valid**: `max_fee >= base_fee`
2. **Competitive fee**: effective priority fee >= 50th percentile of mempool
3. **Sufficient dwell time**: in mempool >= 12 seconds (no upper cap)
4. **Not user-replaced**: excluded from nonce replacement set
5. **Dual capacity check**: gas fits in both previous and current block
6. **Not already included**: not in any block up to and including block N (IL builder can't see future)
7. **EIP-1559 only**: type 2 transactions (filters phantom/spam)

### Nonce Replacement Detection

For each (sender, nonce) pair with multiple transactions:
- If one was included on-chain, all others are marked as replaced
- If none were included, the highest `max_fee` tx is kept (matches geth mempool behavior)

### Inclusion List Construction

**Top Fee**: Filter mempool window to FOCIL-valid EIP-1559 txs, sort by effective priority fee descending, pack into 8 KiB cap (skip oversized txs, don't stop).

**Censored**: Use pre-flagged censored txs, revalidate against current base fee, pack into 8 KiB cap.

### Inclusion Rate (Redundancy)

These are mock ILs -- nothing is forced on-chain. The inclusion rate measures what percentage of IL transactions were naturally included by builders without FOCIL enforcement. Higher rates at higher delays are expected since there are more blocks to check.

## Output Schema

The output Parquet file contains one row per block:

**Block metadata:**
- `block_number`, `block_timestamp`, `base_fee`, `gas_used`, `gas_limit`, `included_tx_count`

**Mempool diagnostics:**
- `mempool_coverage_of_next_block` - % of N+1 block txs visible in mempool window
- `mempool_unique_txs_in_window` - unique tx count in mempool window

**Per-variant columns** (for each of `{0,1,2}delay_{topfee,censored}`):
- `{variant}_tx_count` - number of transactions in the IL
- `{variant}_size_bytes` - IL size in bytes
- `{variant}_inclusion_rate` - % of IL txs naturally included (redundancy)

**Censorship detection:**
- `censored_detected_count` - total censored txs detected at this block

## Notes

- ClickHouse (Xatu) is distributed: JOINs across tables are denied. All joins are done in Python.
- ~72.7% of block txs appear in the public mempool (based on empirical analysis).
- Top ~50-100 txs by raw fee are typically phantom/spam (legacy type 0, 0s observation spread, inactive senders). The type 2 filter eliminates these.

## Reference

- [FOCIL Research Specification](https://hackmd.io/@pellekrab/HkzMiXkmZe)
- [EIP-7805: FOCIL](https://eips.ethereum.org/EIPS/eip-7805)

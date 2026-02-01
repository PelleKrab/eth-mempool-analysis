# Ethereum FOCIL Censorship Analysis

Analysis framework for studying transaction censorship patterns on Ethereum using Fork-Choice enforced Inclusion Lists (FOCIL) methodology.

## Overview

This project analyzes historical Ethereum mempool data to identify and quantify transaction censorship. The analysis constructs three types of inclusion lists per block and calculates overlap metrics for bandwidth optimization studies.

### Inclusion List Types

- **L₀**: Top-N highest priority fee transactions from the current block time window
- **L₋₁**: Transactions flagged as censored at block N-1 (1-slot delay)
- **L₋₂**: Transactions flagged as censored at block N-2 (2-slot delay)

### Key Features

1. **Censorship Detection**: Identifies transactions meeting inclusion criteria but excluded from blocks
2. **Nonce Replacement Handling**: Filters user-initiated transaction replacements to avoid false positives
3. **Bandwidth Analysis**: Calculates overlap between inclusion lists and effective bandwidth requirements
4. **EIP-7805 Compliance**: Enforces 8 KiB size cap and priority-based ordering

## Architecture

```
┌─────────────────┐
│   ClickHouse    │  Historical mempool and block data
│   Data Source   │
└────────┬────────┘
         │
         │ Query mempool transactions
         ▼
┌─────────────────┐
│  Python Script  │  focil_censorship_analysis.py
│   (Analysis)    │
└────────┬────────┘
         │
         │ Export results
         ▼
┌─────────────────┐
│ Parquet Output  │  Metrics per block
│  (Results)      │
└─────────────────┘
```

## Project Structure

```
eth-mempool-analysis/
├── config/
│   └── config.yaml                    Configuration parameters
├── queries/
│   ├── censorship_events.sql          Reference: Censorship detection patterns
│   ├── nonce_replacements.sql         Reference: Replacement detection patterns
│   ├── block_il_metrics.sql           Reference: Block metrics
│   └── mempool_time_range.sql         Reference: Mempool queries
├── scripts/
│   ├── focil_censorship_analysis.py   Main analysis script
│   ├── batch_processor.py             Legacy batch processor
│   └── analyze_results.py             Results visualization
├── results/
│   ├── focil_censorship_analysis.parquet  Analysis output
│   └── archive_old_methodology/           Archived results
├── requirements.txt                   Python dependencies
└── README.md                          This file
```

## Setup

### Prerequisites

```bash
cd ~/eth-mempool-analysis
pip install -r requirements.txt
```

### Configuration

Edit `config/config.yaml` with ClickHouse credentials:

```yaml
clickhouse:
  url: "https://your-server.com:8123"
  user: "your_username"
  password: "your_password"
  database: "default"

analysis:
  time_window_start_secs: -4
  time_window_end_secs: 8
```

## Usage

### Running the Analysis

```bash
cd scripts
python focil_censorship_analysis.py
```

The script reads configuration from `config/config.yaml` and processes the specified block range.

### Configuration

Edit `config/config.yaml` to set the block range:

```yaml
analysis:
  start_block: 21575000      # Starting block number
  end_block: 21575500        # Ending block number
  batch_size_blocks: 100     # Blocks per batch (adjust for memory constraints)
```

### Output

The script generates:
- Console summary with aggregate statistics
- Parquet file: `results/focil_censorship_analysis.parquet`

## Methodology

### Censorship Detection

Transactions are flagged as censored when all criteria are met:

1. **FOCIL-valid**: Transaction fee cap exceeds block base fee
2. **Competitive fee**: Priority fee at or above 25th percentile of mempool
3. **Sufficient dwell time**: Present in mempool for at least 12 seconds
4. **Not user-replaced**: Excluded from nonce replacement set
5. **Not included**: Remains in mempool after block publication

### Nonce Replacement Detection

To prevent false positive censorship flags:

```python
# Group transactions by (sender, nonce)
# Identify highest-fee transaction as final version
# Mark all other transactions in group as user replacements
# Exclude replacements from censorship detection
```

### Inclusion List Construction

**L₀ Construction:**
```python
# Filter transactions by base fee
# Sort by priority fee (descending)
# Select top-N until 8 KiB size cap
```

**L₋₁ and L₋₂ Construction:**
```python
# Detect censored transactions at block N-1 or N-2
# Sort censored transactions by priority fee (descending)
# Pack until 8 KiB size cap
```

## Results

### Data Schema

The output Parquet file contains per-block metrics:

**Block metadata:**
- `block_number`, `block_timestamp`, `base_fee`, `included_tx_count`

**L₀ metrics:**
- `L0_tx_count`, `L0_size_bytes`

**L₋₁ metrics:**
- `L1_tx_count` - Transactions in inclusion list (after size cap)
- `L1_size_bytes` - Inclusion list size
- `L1_censored_tx_count` - Total censored transactions detected
- `L0_L1_intersection_count` - Overlap with L₀
- `L0_L1_intersection_bytes` - Overlap size
- `L1_effective_bytes` - Bandwidth after deduplication
- `L1_bandwidth_saved` - Bytes saved
- `L1_savings_pct` - Savings percentage

**L₋₂ metrics:**
- Similar structure to L₋₁

### Example Analysis

```python
import pandas as pd

# Load results
df = pd.read_parquet('results/focil_censorship_analysis.parquet')

# Summary statistics
print(f"Blocks analyzed: {len(df)}")
print(f"Mean censored transactions (N-1): {df['L1_censored_tx_count'].mean():.2f}")
print(f"Blocks with censorship: {(df['L1_censored_tx_count'] > 0).sum()}")

# Identify blocks with censorship
censored_blocks = df[df['L1_censored_tx_count'] > 0]
print(censored_blocks[['block_number', 'L1_censored_tx_count', 'L2_censored_tx_count']])
```

### Empirical Findings

Analysis of blocks 21,575,000 - 21,575,497 (January 2025):

**Censorship Prevalence:**
- 4.8% of blocks contained censored transactions
- Mean: 0.2 censored transactions per block
- 24 blocks out of 497 showed censorship activity

**Nonce Replacements:**
- 45% of mempool transactions are user-initiated replacements
- Critical for avoiding false positive censorship detection

**Inclusion List Characteristics:**
- L₀ mean size: 6.26 KiB (16.0 transactions)
- L₋₁ mean size: 0.02 KiB (0.1 transactions)
- L₋₂ mean size: 0.02 KiB (0.1 transactions)

**Overlap Analysis:**
- Negligible overlap between L₀ and L₋₁/L₋₂
- Censored transactions typically have competitive but not highest fees

## Technical Details

### Time Window

Transactions are considered for inclusion if first seen within:
```
[block_timestamp - 4 seconds, block_timestamp + 8 seconds]
```

### Fee Percentile Calculation

The 25th percentile is calculated from mempool transactions observed in:
```
[block_timestamp - 30 seconds, block_timestamp]
```

### Dwell Time

Minimum dwell time threshold: 12 seconds (approximately 1 slot)

### Size Cap

EIP-7805 specification: 8,192 bytes (8 KiB) maximum per inclusion list

## Performance

### Processing Time

- Approximately 0.4 seconds per block
- 100 blocks: ~40 seconds
- 500 blocks: ~3 minutes
- Batch processing recommended for large ranges

### Memory Usage

- Batch size: 100 blocks
- Peak memory: ~500 MB per batch
- Scales linearly with batch size

## Reference

Research specification: https://hackmd.io/@pellekrab/HkzMiXkmZe

## Citation

```bibtex
@misc{eth_focil_censorship_2025,
  title={FOCIL-based Transaction Censorship Analysis on Ethereum},
  year={2025},
  howpublished={\url{https://github.com/yourusername/eth-mempool-analysis}}
}
```

## License

MIT

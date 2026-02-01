# Quick Start Guide

## Overview

This guide provides instructions for running the FOCIL censorship analysis on Ethereum historical data.

## Running the Analysis

### Execute the Script

```bash
cd ~/eth-mempool-analysis/scripts
python focil_censorship_analysis.py
```

The script will process the configured block range and output results to the console and a Parquet file.

### Configure Block Range

Edit `config/config.yaml`:

```yaml
analysis:
  start_block: 21575000      # Starting block number
  end_block: 21575500        # Ending block number
  batch_size_blocks: 100     # Blocks per batch
```

Adjust these parameters based on your analysis requirements and available memory.

## Output

### Console Output

The script displays:
- Block range processed
- Nonce replacement statistics
- L₀ inclusion list metrics (highest-fee transactions)
- L₋₁ inclusion list metrics (censored from N-1)
- L₋₂ inclusion list metrics (censored from N-2)
- Censorship detection summary
- Bandwidth analysis

### Data File

Results are saved to:
```
results/focil_censorship_analysis.parquet
```

## Analyzing Results

### Load Data

```python
import pandas as pd

df = pd.read_parquet('results/focil_censorship_analysis.parquet')
```

### View Summary Statistics

```python
# Block coverage
print(f"Blocks analyzed: {len(df)}")

# Censorship metrics
print(f"Mean censored (N-1): {df['L1_censored_tx_count'].mean():.2f}")
print(f"Mean censored (N-2): {df['L2_censored_tx_count'].mean():.2f}")

# Blocks with censorship
censored_blocks = df[df['L1_censored_tx_count'] > 0]
print(f"Blocks with censorship: {len(censored_blocks)}")
```

### Filter Specific Blocks

```python
# Blocks with censorship activity
censored = df[df['L1_censored_tx_count'] > 0]
print(censored[['block_number', 'L1_censored_tx_count', 'L2_censored_tx_count']])
```

## Sample Results

From blocks 21,575,000 - 21,575,497 (500 block sample):

**Censorship Statistics:**
- Blocks with censorship: 24 (4.8%)
- Mean censored transactions: 0.2 per block

**Nonce Replacements:**
- Replacement rate: 45% of mempool transactions
- These are excluded from censorship detection

**Inclusion List Sizes:**
- L₀ mean: 6.26 KiB (16.0 transactions)
- L₋₁ mean: 0.02 KiB (0.1 transactions)
- L₋₂ mean: 0.02 KiB (0.1 transactions)

**Overlap:**
- L₀ ∩ L₋₁: Approximately 0%
- L₀ ∩ L₋₂: Approximately 0%

## Interpretation

### Inclusion List Composition

**L₀**: Contains the highest priority fee transactions from the current block time window. This represents the optimal inclusion list without censorship consideration.

**L₋₁ and L₋₂**: Contain only transactions that were flagged as censored at the previous block (N-1) or two blocks prior (N-2). These are typically small as censorship events are relatively infrequent.

### Nonce Replacement Significance

Approximately 45% of mempool transactions are user-initiated replacements (higher fee for same sender and nonce). These must be excluded from censorship detection to avoid false positives.

### Overlap Analysis

The minimal overlap between L₀ and L₋₁/L₋₂ indicates that censored transactions typically have competitive fees (above 25th percentile) but are not among the highest-fee transactions.

## Data Schema

The output file contains the following columns per block:

**Block Metadata:**
- `block_number`, `block_timestamp`, `base_fee`, `included_tx_count`

**L₀ Metrics:**
- `L0_tx_count`, `L0_size_bytes`

**L₋₁ Metrics:**
- `L1_tx_count` - Transactions in inclusion list
- `L1_size_bytes` - Inclusion list size
- `L1_censored_tx_count` - Total censored transactions detected
- `L0_L1_intersection_count` - Overlap count with L₀
- `L0_L1_intersection_bytes` - Overlap size
- `L1_effective_bytes` - Net bandwidth after deduplication
- `L1_bandwidth_saved` - Bandwidth savings

**L₋₂ Metrics:**
- Analogous to L₋₁ metrics

## Reference

Research specification: https://hackmd.io/@pellekrab/HkzMiXkmZe

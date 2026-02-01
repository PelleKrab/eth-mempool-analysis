# FOCIL Censorship Analysis - Quick Start

## What This Does

Analyzes Ethereum transaction censorship using the **correct FOCIL research methodology**:

- **L₀**: Highest-fee transactions from current block
- **L₋₁**: ONLY censored transactions from N-1 block
- **L₋₂**: ONLY censored transactions from N-2 block
- **Nonce replacement tracking**: Excludes user replacements (~45% of transactions!)

## Run Analysis

```bash
cd ~/eth-mempool-analysis/scripts
python proper_focil_analysis.py
```

**That's it!** The script will:
1. Detect nonce replacements
2. Flag censored transactions
3. Build L₀, L₋₁, L₋₂ inclusion lists
4. Calculate metrics and save results

## Configure Block Range

Edit `proper_focil_analysis.py` lines 396-398:

```python
start_block = 21575000
end_block = 21575500    # Change to your desired range
batch_size = 100        # Blocks per batch
```

## Output

**Console summary:**
- Blocks analyzed
- Censorship statistics
- IL sizes and overlap metrics
- Bandwidth calculations

**Saved file:**
- `results/proper_focil_analysis.parquet`

## View Results

```python
import pandas as pd
df = pd.read_parquet('results/proper_focil_analysis.parquet')

# Censorship statistics
print(f"Avg censored at N-1: {df['L1_censored_tx_count'].mean():.2f}")
print(f"Blocks with censorship: {(df['L1_censored_tx_count'] > 0).sum()}")

# View censored blocks
df[df['L1_censored_tx_count'] > 0][['block_number', 'L1_censored_tx_count']]
```

## Key Results (Jan 2025)

From 500 block sample:
- **4.8%** of blocks had censored transactions
- **0.2** censored txs per block average
- **45%** of mempool txs are user replacements
- **0.0%** overlap between censored and highest-fee txs

## Understanding Results

**Q: Why is L₋₁/L₋₂ so small?**
A: They contain ONLY censored transactions, not all high-fee transactions.

**Q: Why track nonce replacements?**
A: Without this, we'd falsely flag 45% of transactions as censored.

**Q: Why no overlap with L₀?**
A: Censored txs have competitive fees (≥25th percentile) but aren't the highest-fee transactions.

## Files Cleaned Up

The following used **incorrect methodology** and were removed:
- ❌ `scripts/focil_bandwidth_analysis.py` - assumed overlap rates
- ❌ `queries/bandwidth_analysis.sql` - didn't detect censorship
- ❌ `queries/focil_delay_analysis.sql` - didn't detect censorship

Old results: `results/archive_old_methodology/`

## Reference

Research spec: https://hackmd.io/@pellekrab/HkzMiXkmZe

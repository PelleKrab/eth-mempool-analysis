# Quick Start: Process 1 Year of Data

## TL;DR

```bash
# Process 1 year of data in chunks (will take ~13 days with 4 workers)
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000 --parallel 4

# Combine results
python scripts/combine_chunks.py

# Results will be in: results/focil_full_year_analysis.parquet
```

## What This Does

**Processes 2,628,000 blocks** (1 year of Ethereum data from Feb 2025 to Feb 2026)

**Splits into 262 chunks** of 10,000 blocks each

**Runs 4 chunks in parallel** using multiprocessing

**Saves individual chunk results** to `results/chunks/chunk_XXXX_*.parquet`

**Combines everything** into one final analysis file

---

## Step-by-Step Guide

### 1. Verify Your Setup

```bash
# Check you have the latest code
cd ~/eth-mempool-analysis
git pull  # if using git

# Check config is correct
cat config/config.yaml

# Test with a small range first (100 blocks)
python scripts/focil_censorship_analysis.py --start 21575000 --end 21575100
```

### 2. Start Batch Processing

```bash
# Run in background with logging
nohup python scripts/batch_runner.py 19272000 21900000 \
    --chunk-size 10000 \
    --parallel 4 \
    > batch.log 2>&1 &

# Save the process ID
echo $! > batch.pid
```

### 3. Monitor Progress

```bash
# Watch the log
tail -f batch.log

# Check how many chunks completed
ls -1 results/chunks/*.parquet | wc -l

# Should show: X / 262 chunks complete

# Check system resources
htop  # or top
```

### 4. If Process Gets Interrupted

```bash
# Resume where you left off (skips completed chunks)
python scripts/batch_runner.py 19272000 21900000 \
    --chunk-size 10000 \
    --parallel 4 \
    --resume
```

### 5. Combine Results

```bash
# After all 262 chunks complete
python scripts/combine_chunks.py

# Check the output
ls -lh results/focil_full_year_analysis.parquet
# Should be ~520 MB

# View summary
head -100 batch.log  # scroll to see final summary
```

---

## Timeline Estimates

With **4 parallel workers** and **0.4s per block**:

- **Per chunk (10K blocks)**: ~70 minutes
- **262 chunks / 4 workers**: 66 batches of 4
- **Total time**: ~66 × 70 min = **77 hours (~3.2 days)**

With **16 parallel workers** (if you have a beefy machine):
- **Total time**: ~20 hours

**Sequential (1 worker)**:
- **Total time**: ~262 × 70 min = **~13 days**

---

## Disk Space Requirements

- **Each chunk**: ~2 MB
- **262 chunks**: ~520 MB
- **Combined file**: ~520 MB
- **Total with logs**: **~1.5 GB**

Make sure you have at least **2 GB free** in your `results/` directory.

---

## Memory Requirements

- **Per worker**: ~500 MB RAM
- **4 workers**: 2-3 GB RAM total
- **16 workers**: 8-10 GB RAM total

**Recommendation**: Don't exceed your CPU core count for workers.

---

## Common Issues

### Issue: "Could not fetch inclusion data"

**Solution**: ClickHouse might be rate-limiting you. Reduce parallelism:
```bash
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000 --parallel 2
```

### Issue: Out of memory

**Solution**: Reduce batch size in config:
```bash
# Edit config/config.yaml
batch_size_blocks: 50  # down from 100
```

### Issue: Process killed

**Solution**: Check system logs for OOM killer. Reduce parallelism or chunk size.

### Issue: Some chunks failed

**Solution**: Re-run with `--resume` flag to only process failed chunks:
```bash
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000 --parallel 4 --resume
```

---

## What Happens Next?

After combining, you'll have:

**File**: `results/focil_full_year_analysis.parquet`

**Size**: ~520 MB

**Rows**: 2,628,000 (one per block)

**Columns**: ~40 metrics including:
- Censorship detection counts
- IL sizes (topfee and censored strategies)
- Inclusion rates at 0, 1, 2-delay
- Gas usage, base fees, mempool coverage

### Use This Data For:

1. **Statistical significance testing** (massive sample size)
2. **Time-series analysis** (track censorship over time)
3. **Correlation studies** (MEV, gas prices, major events)
4. **Publication-ready results** (1 year >>> 497 blocks)

### Example Analysis:

```python
import pandas as pd

# Load full year data
df = pd.read_parquet('results/focil_full_year_analysis.parquet')

# Calculate annual censorship rate
censored_blocks = (df['censored_detected_count'] > 0).sum()
print(f"Censorship rate: {censored_blocks / len(df) * 100:.2f}%")

# Monthly trends
df['month'] = pd.to_datetime(df['block_timestamp'], unit='s').dt.to_period('M')
monthly_censorship = df.groupby('month')['censored_detected_count'].mean()
monthly_censorship.plot(title='Monthly Censorship Trends')

# Bandwidth analysis
topfee_annual_tb = df['0delay_topfee_size_bytes'].sum() / (1024**4)
censored_annual_tb = df['0delay_censored_size_bytes'].sum() / (1024**4)
print(f"Top Fee: {topfee_annual_tb:.2f} TB/year")
print(f"Censored: {censored_annual_tb:.2f} TB/year")
```

---

## Need Help?

Check the detailed guide: `BATCH_PROCESSING.md`

Questions? Review the logs:
- `batch.log` - Main processing log
- `results/chunks/` - Individual chunk outputs

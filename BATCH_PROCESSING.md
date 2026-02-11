# Batch Processing Guide: Processing 1 Year of Data

This guide explains how to process a full year (2.6M blocks) of Ethereum data for FOCIL censorship analysis.

## Quick Start

### Option 1: Automated Batch Processing (Recommended)

Run all chunks sequentially:
```bash
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000
```

Run with 4 parallel workers (faster if you have multiple cores):
```bash
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000 --parallel 4
```

Resume if interrupted:
```bash
python scripts/batch_runner.py 19272000 21900000 --chunk-size 10000 --resume
```

Then combine all chunks:
```bash
python scripts/combine_chunks.py
```

### Option 2: Distributed Processing

If you want to run chunks on different machines or at different times:

1. **Generate chunk ranges:**
```bash
python scripts/generate_chunk_ranges.py 19272000 21900000 10000
```

This creates `chunk_ranges.csv` with 262 chunk ranges.

2. **Run individual chunks:**
```bash
# Run chunk 0 (blocks 19,272,000 to 19,282,000)
python scripts/focil_censorship_analysis.py --start 19272000 --end 19282000 \
    --output results/chunks/chunk_0000_19272000_19282000.parquet

# Run chunk 1
python scripts/focil_censorship_analysis.py --start 19282000 --end 19292000 \
    --output results/chunks/chunk_0001_19282000_19292000.parquet

# ... run more chunks as needed
```

3. **Combine when done:**
```bash
python scripts/combine_chunks.py
```

---

## Understanding the Numbers

### 1 Year of Ethereum Data:
- **Blocks per year**: 2,628,000 (7200 blocks/day × 365 days)
- **Latest block** (Feb 2026): ~21,900,000
- **1 year ago**: Block 19,272,000
- **Date range**: ~Feb 2025 to Feb 2026

### Recommended Chunk Sizes:

| Chunk Size | # of Chunks | Time per Chunk* | Total Time* |
|------------|-------------|-----------------|-------------|
| 1,000 blocks | 2,628 | ~7 min | ~13 days |
| 5,000 blocks | 525 | ~35 min | ~13 days |
| 10,000 blocks | 262 | ~70 min | ~13 days |
| 50,000 blocks | 52 | ~6 hours | ~13 days |

*Estimated at ~0.4s per block processing time. Actual time varies based on mempool density and network latency to ClickHouse.

### Recommendation:
- **10,000 blocks/chunk** is optimal (262 chunks, ~70 min each)
- Balance between granularity and overhead
- Easy to parallelize
- Can resume if interrupted

---

## Workflow Examples

### Example 1: Process Full Year Overnight

```bash
# Start batch processing (will run for ~13 days sequentially, or 3-4 days with 4 workers)
nohup python scripts/batch_runner.py 19272000 21900000 \
    --chunk-size 10000 \
    --parallel 4 \
    > batch_processing.log 2>&1 &

# Check progress
tail -f batch_processing.log

# When done, combine results
python scripts/combine_chunks.py
```

### Example 2: Manual Chunk Distribution

If you have multiple machines:

**Machine 1:**
```bash
# Process first 100 chunks
for i in {0..99}; do
    start=$((19272000 + i * 10000))
    end=$((start + 10000))
    python scripts/focil_censorship_analysis.py \
        --start $start --end $end \
        --output results/chunks/chunk_$(printf "%04d" $i)_${start}_${end}.parquet
done
```

**Machine 2:**
```bash
# Process next 100 chunks
for i in {100..199}; do
    start=$((19272000 + i * 10000))
    end=$((start + 10000))
    python scripts/focil_censorship_analysis.py \
        --start $start --end $end \
        --output results/chunks/chunk_$(printf "%04d" $i)_${start}_${end}.parquet
done
```

Then copy all chunk files to one machine and combine.

### Example 3: Cloud Processing with AWS/GCP

```bash
# Upload code to cloud instance
scp -r eth-mempool-analysis/ user@cloud-instance:~/

# SSH to instance
ssh user@cloud-instance

# Run with high parallelism
cd eth-mempool-analysis
python scripts/batch_runner.py 19272000 21900000 \
    --chunk-size 10000 \
    --parallel 16

# Download results
scp user@cloud-instance:~/eth-mempool-analysis/results/chunks/*.parquet ./results/chunks/
```

---

## Monitoring Progress

### Check chunk completion:
```bash
# Count completed chunks
ls results/chunks/*.parquet | wc -l

# List failed/missing chunks (compare against chunk_ranges.csv)
python scripts/check_missing_chunks.py  # TODO: create this script if needed
```

### Disk space requirements:
- **Per-block output**: ~200 bytes/block
- **10,000-block chunk**: ~2 MB
- **Full year (2.6M blocks)**: ~520 MB total
- **With overhead**: Budget 1-2 GB for results directory

### Memory requirements:
- **Per chunk**: ~500 MB RAM
- **Parallel processing**: 500 MB × number of workers
- **Safe setup**: 4 workers = 2-3 GB RAM

---

## Combining Results

After all chunks complete:

```bash
# Basic combine
python scripts/combine_chunks.py

# Custom input/output
python scripts/combine_chunks.py \
    --input-dir results/chunks \
    --output results/ethereum_2025_full_year.parquet \
    --verbose
```

This will:
1. Load all `chunk_*.parquet` files
2. Sort by block number
3. Save to single combined file
4. Print comprehensive summary statistics

---

## Troubleshooting

### Chunk failed to process
```bash
# Re-run specific chunk
python scripts/focil_censorship_analysis.py \
    --start 19272000 \
    --end 19282000 \
    --output results/chunks/chunk_0000_19272000_19282000.parquet
```

### Out of memory errors
```bash
# Reduce batch size in config
# config/config.yaml: batch_size_blocks: 50

# Or reduce chunk size
python scripts/batch_runner.py 19272000 21900000 --chunk-size 5000
```

### ClickHouse connection timeouts
```bash
# Reduce parallelism to avoid rate limits
python scripts/batch_runner.py 19272000 21900000 \
    --chunk-size 10000 \
    --parallel 2
```

### Resume after interruption
```bash
# The --resume flag skips already-completed chunks
python scripts/batch_runner.py 19272000 21900000 \
    --chunk-size 10000 \
    --parallel 4 \
    --resume
```

---

## Performance Optimization

### 1. Parallel Processing
Use `--parallel` flag based on your system:
- **4 cores**: `--parallel 4`
- **8 cores**: `--parallel 6` (leave 2 cores for system)
- **16+ cores**: `--parallel 12-16`

### 2. Chunk Size Selection
- **Too small** (1K blocks): High overhead from setup/teardown
- **Too large** (100K blocks): Memory issues, can't resume easily
- **Optimal**: 10K-50K blocks depending on RAM

### 3. ClickHouse Connection
- Ensure stable network to ClickHouse server
- Use retry logic (already implemented)
- Monitor for rate limiting

### 4. Storage
- Use SSD for results directory (faster writes)
- Compress old chunks: `gzip results/chunks/*.parquet`

---

## Expected Results After 1 Year

Based on your initial 497-block sample:

**Projected Annual Metrics:**
- Total blocks: 2,628,000
- Censorship events: ~126,000 blocks (4.8%)
- Top Fee IL: ~16.4 TB/year (6.26 KiB/block)
- Censored IL: ~0.05 TB/year (0.02 KiB/block)

These are extrapolations - actual results may vary based on network conditions.

---

## Next Steps After Processing

1. **Statistical Analysis**: Use the combined dataset for significance testing
2. **Time-series Analysis**: Track censorship trends over the year
3. **Correlation Studies**: Compare with major events, gas spikes, MEV activity
4. **Publication**: Use full year data for academic paper

---

## Questions?

- Check logs in `batch_processing.log`
- Review individual chunk outputs in `results/chunks/`
- Monitor system resources: `htop`, `df -h`, `nvidia-smi` (if using GPU)

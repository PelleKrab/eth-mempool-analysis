# Quick Start Guide

Get up and running in 5 minutes!

## Step 1: Install Dependencies (30 seconds)

```bash
cd ~/eth-mempool-analysis
pip install -r requirements.txt
```

## Step 2: Configure ClickHouse (1 minute)

Edit `config/config.yaml`:

```bash
nano config/config.yaml
```

Update your ClickHouse credentials:
```yaml
clickhouse:
  url: "https://your-server.com:8123"
  user: "your_username"
  password: "your_password"
```

## Step 3: Test on Small Sample (2 minutes)

Process just 1 week of data to verify everything works:

```bash
cd scripts
python batch_processor.py --start-block 15537394 --end-block 15587394 --batch-size 10000
```

You should see:
```
Connecting to ClickHouse...
Processing batch: 15537394 to 15547394
✓ Batch completed successfully
```

## Step 4: Analyze Results (1 minute)

```bash
python analyze_results.py
```

Check output:
- `../results/figures/` - PNG visualizations
- `../results/summary_report.md` - Summary statistics

## Step 5: Run Full Analysis

Once you've verified it works, process all 5 years:

```bash
python batch_processor.py
```

This will take 6-30 hours. You can monitor progress in the logs.

## Pro Tips

### Run in Background

```bash
nohup python batch_processor.py > batch.log 2>&1 &
tail -f batch.log
```

### Resume Failed Batches

The system automatically skips completed batches if files exist. Just re-run:

```bash
python batch_processor.py
```

### Export Specific Queries Only

```bash
# Only run bandwidth analysis
python batch_processor.py --query bandwidth
```

### View Results Without Running Analysis

```bash
# Load parquet files directly
python
>>> import pandas as pd
>>> df = pd.read_parquet('../results/block_il_metrics_*.parquet')
>>> df.head()
```

## Troubleshooting

**ClickHouse connection failed?**
```bash
# Test connection manually
clickhouse-client --host=your-host --user=user --password=pass
```

**Out of memory?**
```bash
# Use smaller batches
python batch_processor.py --batch-size 50000
```

**Want to process specific date range?**
```bash
# Blocks ~7200 per day, so:
# Jan 2023 = block ~16_300_000
# Dec 2024 = block ~20_900_000
python batch_processor.py --start-block 16300000 --end-block 20900000
```

## What's Next?

1. **Read README.md** for full documentation
2. **Modify SQL queries** in `queries/` for custom analysis
3. **Create Jupyter notebooks** in `notebooks/` for interactive exploration
4. **Share results** - export figures for your research paper

## Need Help?

- Check the logs: `logs/analysis.log`
- Review the queries: `queries/*.sql`
- Open an issue on GitHub

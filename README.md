# Ethereum Mempool Historical Analysis

Batch processing system for analyzing 5 years of Ethereum mempool data to study inclusion lists, transaction censorship, and bandwidth savings.

## Overview

This project processes historical Ethereum mempool data from ClickHouse to generate insights about:

1. **Inclusion Lists (ILs)** - Transactions that should be included based on time windows
2. **Bandwidth Savings** - Comparing 1-slot vs 2-slot IL strategies
3. **Transaction Replacements** - Nonce-based RBF (Replace-By-Fee) behavior
4. **Censorship Events** - Transactions that met inclusion criteria but weren't included

## Architecture

```
┌─────────────────┐
│   ClickHouse    │  (5 years of block + mempool data)
│   (Raw Data)    │
└────────┬────────┘
         │
         │ SQL Queries (batch processing)
         ▼
┌─────────────────┐
│  Python Scripts │  (batch_processor.py)
│  (Orchestrator) │
└────────┬────────┘
         │
         │ Export to Parquet
         ▼
┌─────────────────┐
│  Results Files  │  (Parquet/CSV)
│  (Local Store)  │
└────────┬────────┘
         │
         │ Analysis
         ▼
┌─────────────────┐
│  Visualizations │  (analyze_results.py)
│   & Reports     │
└─────────────────┘
```

## Project Structure

```
eth-mempool-analysis/
├── config/
│   └── config.yaml           # Configuration (ClickHouse, params)
├── queries/
│   ├── block_il_metrics.sql        # Inclusion list metrics per block
│   ├── nonce_replacements.sql      # Transaction replacement detection
│   ├── bandwidth_analysis.sql      # 1-slot vs 2-slot bandwidth
│   └── censorship_events.sql       # Censorship detection
├── scripts/
│   ├── batch_processor.py          # Main batch processing script
│   └── analyze_results.py          # Analysis and visualization
├── results/                   # Output directory (generated)
│   ├── *.parquet             # Processed data
│   ├── figures/              # Generated plots
│   └── summary_report.md     # Summary statistics
├── notebooks/                # Jupyter notebooks (optional)
├── requirements.txt          # Python dependencies
└── README.md                 # This file
```

## Setup

### 1. Install Dependencies

```bash
cd ~/eth-mempool-analysis
pip install -r requirements.txt
```

### 2. Configure ClickHouse Connection

Edit `config/config.yaml`:

```yaml
clickhouse:
  url: "https://your-clickhouse-server.com:8123"
  user: "your_username"
  password: "your_password"
  database: "default"
```

Or use environment variables:
```bash
export CLICKHOUSE_URL="https://your-server.com:8123"
export CLICKHOUSE_USER="your_username"
export CLICKHOUSE_PASSWORD="your_password"
```

### 3. Configure Analysis Parameters

In `config/config.yaml`:

```yaml
analysis:
  start_block: 15537394   # The Merge (Sep 15, 2022)
  end_block: 21000000     # Adjust to current block
  time_window_start_secs: -4
  time_window_end_secs: 8
  batch_size_blocks: 216000  # ~1 month (~30 days * 7200 blocks/day)
```

## Usage

### Run Full Analysis (All 5 Years)

Process all data in batches:

```bash
cd scripts
python batch_processor.py
```

This will:
- Process blocks in monthly chunks
- Run all SQL queries (IL metrics, replacements, bandwidth)
- Save results to `results/*.parquet`
- Take several hours to complete

### Run Specific Analysis

Process only certain metrics:

```bash
# Just inclusion list metrics
python batch_processor.py --query il_metrics

# Just bandwidth analysis
python batch_processor.py --query bandwidth

# Just nonce replacements
python batch_processor.py --query replacements
```

### Custom Block Range

```bash
# Process specific range
python batch_processor.py --start-block 15537394 --end-block 16000000

# Smaller batches for testing
python batch_processor.py --start-block 15537394 --end-block 15547394 --batch-size 1000
```

### Analyze Results

After processing, generate visualizations and reports:

```bash
python analyze_results.py
```

This creates:
- `results/figures/il_metrics_analysis.png`
- `results/figures/bandwidth_savings.png`
- `results/figures/nonce_replacements.png`
- `results/summary_report.md`

### Analyze Specific Metric

```bash
python analyze_results.py --analysis il
python analyze_results.py --analysis bandwidth
python analyze_results.py --analysis replacements
```

## Research Questions Answered

### 1. What are the characteristics of Inclusion Lists?

**Query:** `block_il_metrics.sql`

**Metrics:**
- IL transaction count per block
- IL size in bytes (bandwidth)
- Average time offset from block timestamp
- Fee distribution of IL transactions

**Key Insights:**
- How many transactions fall within the [-4s, +8s] window?
- What's the typical IL size?
- Are transactions clustered before or after block time?

### 2. How much bandwidth can 2-slot ILs save?

**Query:** `bandwidth_analysis.sql`

**Metrics:**
- 1-slot IL size vs 2-slot IL size
- Number of transactions already included (waste)
- Effective bandwidth (size - waste)
- Percentage savings

**Key Insights:**
- Is using previous block's IL more efficient?
- What % of 1-slot IL transactions get included anyway?
- Cumulative bandwidth savings over time

### 3. How common are transaction replacements?

**Query:** `nonce_replacements.sql`

**Metrics:**
- Number of replacements per (sender, nonce)
- Fee increase (multiplier)
- Time between replacements
- Unique senders using RBF

**Key Insights:**
- How often do users replace transactions?
- What's the typical fee bump?
- How quickly do replacements happen?

### 4. How prevalent is transaction censorship?

**Query:** `censorship_events.sql`

**Metrics:**
- Transactions pending >N blocks with competitive fees
- Confidence score based on fee percentiles
- Time in mempool vs block inclusion

**Key Insights:**
- How many high-fee transactions are delayed?
- What's the distribution of pending times?
- Are there patterns in censorship?

## Data Flow Details

### Time Window Logic

```python
# From handwritten notes: [-4, +8] seconds
block_timestamp = block.timestamp
window_start = block_timestamp - 4 seconds
window_end = block_timestamp + 8 seconds

# Filter mempool transactions
included_in_il = (
    tx.seen_timestamp >= window_start AND
    tx.seen_timestamp <= window_end AND
    tx.max_fee >= block.base_fee AND
    tx NOT in block.transactions
)
```

### Nonce Replacement Logic

```sql
-- Find highest-fee tx per (sender, nonce)
ROW_NUMBER() OVER (
    PARTITION BY sender, nonce
    ORDER BY max_fee DESC
) as fee_rank

-- fee_rank = 1 is the final transaction
-- fee_rank > 1 are replaced transactions
```

### Bandwidth Calculation

```
effective_bandwidth = il_size - already_included_bytes

savings = 2_slot_effective - 1_slot_effective
savings_pct = (savings / 1_slot_effective) * 100
```

## Performance

### Estimated Processing Time

For 5 years of data (~40M blocks):

- **Monthly batches** (216k blocks/batch): ~186 batches
- **Per batch**: 2-10 minutes (depending on mempool density)
- **Total time**: 6-30 hours

### Optimization Tips

1. **Increase batch size** for faster processing (but higher memory usage):
   ```yaml
   batch_size_blocks: 432000  # 2 months
   ```

2. **Run queries in parallel** (modify batch_processor.py):
   ```python
   # Use ThreadPoolExecutor for concurrent queries
   ```

3. **Use ClickHouse materialized views** (pre-aggregate):
   ```sql
   CREATE MATERIALIZED VIEW mv_il_metrics AS ...
   ```

4. **Sample data** for testing:
   ```bash
   python batch_processor.py --start-block 15537394 --end-block 15637394  # 1 week
   ```

## Output Files

### Parquet Files

- `block_il_metrics_<start>_<end>_<timestamp>.parquet`
- `nonce_replacements_<start>_<end>_<timestamp>.parquet`
- `bandwidth_analysis_<start>_<end>_<timestamp>.parquet`
- `censorship_events_<start>_<end>_<timestamp>.parquet` (optional)

### Figures

- `il_metrics_analysis.png` - IL size, distribution, time offsets
- `bandwidth_savings.png` - 1-slot vs 2-slot comparison
- `nonce_replacements.png` - Fee increases, replacement timing

### Reports

- `summary_report.md` - Key statistics and findings

## Advanced Usage

### Custom SQL Queries

Modify queries in `queries/` directory and re-run:

```bash
# Edit the query
vim queries/block_il_metrics.sql

# Re-run batch processor
python batch_processor.py --query il_metrics
```

### Jupyter Notebooks

For interactive analysis:

```bash
jupyter notebook
```

Example notebook:

```python
import pandas as pd
import duckdb

# Load all IL metrics
con = duckdb.connect()
df = con.execute("""
    SELECT * FROM 'results/block_il_metrics_*.parquet'
    WHERE il_tx_count > 100
""").df()

# Analyze high-IL blocks
df.describe()
```

### Export to Other Formats

```python
# In analyze_results.py, add:
df.to_csv('results/il_metrics.csv')
df.to_json('results/il_metrics.json')
df.to_excel('results/il_metrics.xlsx')
```

## Troubleshooting

### ClickHouse Connection Issues

```bash
# Test connection
clickhouse-client --host=your-host --user=your-user --password=your-password

# Check query execution time
clickhouse-client --query "SELECT count(*) FROM mempool_transaction"
```

### Out of Memory

Reduce batch size:
```yaml
batch_size_blocks: 108000  # 2 weeks instead of 1 month
```

### Slow Queries

Add indexes in ClickHouse:
```sql
ALTER TABLE mempool_transaction ADD INDEX idx_seen_timestamp seen_timestamp TYPE minmax GRANULARITY 3;
```

### Missing Data

Check ClickHouse tables:
```sql
SELECT
    min(block_number),
    max(block_number),
    count(*)
FROM canonical_beacon_block;
```

## Research Paper Template

### Introduction

This analysis processes **5 years** of Ethereum mempool data (blocks **15,537,394** to **21,000,000**) to evaluate inclusion list effectiveness and bandwidth optimization.

### Methodology

- **Data Source**: ClickHouse database with full Ethereum mempool history
- **Time Window**: -4 to +8 seconds around block timestamp (based on Fort specification)
- **Batch Processing**: Monthly chunks (~216k blocks) for scalability
- **Metrics**: IL size, bandwidth savings, transaction replacements, censorship events

### Key Findings

*[Fill in after running analysis]*

- Average IL size: **X** transactions, **Y** KB
- Bandwidth savings (2-slot vs 1-slot): **Z**%
- Transaction replacement rate: **N**%
- Censorship detection rate: **M**%

### Visualizations

*[Include figures from results/figures/]*

## Citation

If you use this analysis in research:

```bibtex
@misc{eth_mempool_analysis_2025,
  title={Historical Analysis of Ethereum Mempool for Inclusion List Research},
  author={Your Name},
  year={2025},
  howpublished={\url{https://github.com/yourusername/eth-mempool-analysis}}
}
```

## License

MIT

## Contact

For questions or contributions, please open an issue or PR.

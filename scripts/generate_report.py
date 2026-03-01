import pandas as pd
import numpy as np
from pathlib import Path
import datetime

def generate_report(parquet_file):
    df = pd.read_parquet(parquet_file)
    
    # --- derived metrics ---
    blocks_per_year = 2628000 # approx 12s blocks
    # actually let's use the observed time range to project annual if needed, 
    # but standardized blocks_per_year is better for comparisons.
    # Ethereum is exactly 12s/block => 5 slots/min * 60 * 24 * 365 = 2,628,000
    
    total_blocks = len(df)
    start_blk = df['block_number'].min()
    end_blk = df['block_number'].max()
    
    # Dates
    # Assuming block_timestamp is unix seconds
    if 'block_timestamp' in df.columns:
        start_ts = df['block_timestamp'].min()
        end_ts = df['block_timestamp'].max()
        start_date = datetime.datetime.fromtimestamp(start_ts)
        end_date = datetime.datetime.fromtimestamp(end_ts)
        duration = end_date - start_date
        days = duration.days
    else:
        days = 0
        start_date = "N/A"
        end_date = "N/A"

    # --- Section 1: Chain Health & Mempool ---
    avg_gas_used = df['gas_used'].mean()
    avg_gas_limit = df['gas_limit'].mean()
    gas_utilization = (avg_gas_used / avg_gas_limit) * 100 if avg_gas_limit else 0
    
    avg_base_fee_gwei = df['base_fee'].mean() / 1e9
    median_base_fee_gwei = df['base_fee'].median() / 1e9
    
    mempool_cov = df['mempool_coverage_of_next_block'].mean() if 'mempool_coverage_of_next_block' in df.columns else 0
    
    # --- Section 2: Top Fee Strategy (Standard FOCIL) ---
    tf_txs = df['0delay_topfee_tx_count'].mean()
    tf_bytes = df['0delay_topfee_size_bytes'].mean()
    tf_kb = tf_bytes / 1024
    tf_annual_gb = (tf_bytes * blocks_per_year) / (1024**3)
    
    # Inclusion Rates
    # Data is already in percentage (0-100)
    tf_inc_0 = df['0delay_topfee_inclusion_rate'].mean()
    tf_inc_1 = df['1delay_topfee_inclusion_rate'].mean() if '1delay_topfee_inclusion_rate' in df.columns else 0
    tf_inc_2 = df['2delay_topfee_inclusion_rate'].mean() if '2delay_topfee_inclusion_rate' in df.columns else 0
    
    # Redundancy (Useful vs Redundant)
    # 0delay_topfee_redundant_bytes vs useful
    tf_useful = df['0delay_topfee_useful_bytes'].mean()
    tf_redundant = df['0delay_topfee_redundant_bytes'].mean()
    tf_efficiency = (tf_useful / tf_bytes * 100) if tf_bytes > 0 else 0

    # --- Section 3: Censored/Robust Strategy ---
    c_txs = df['0delay_censored_tx_count'].mean()
    c_bytes = df['0delay_censored_size_bytes'].mean()
    c_kb = c_bytes / 1024
    c_annual_gb = (c_bytes * blocks_per_year) / (1024**3)
    
    c_inc_0 = df['0delay_censored_inclusion_rate'].mean()
    c_inc_1 = df['1delay_censored_inclusion_rate'].mean() if '1delay_censored_inclusion_rate' in df.columns else 0
    c_inc_2 = df['2delay_censored_inclusion_rate'].mean() if '2delay_censored_inclusion_rate' in df.columns else 0
    
    # --- Section 4: Censorship/Latency ---
    # censored_detected_count = txs that were high fee but delayed
    censored_avg = df['censored_detected_count'].mean()
    censored_blocks_pct = (df[df['censored_detected_count'] > 0].shape[0] / total_blocks) * 100

    # Report Text
    report = []
    report.append("# Ethereum FOCIL & Censorship Analysis Report")
    report.append(f"**Date Generated:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"**Dataset:** {parquet_file.name}")
    report.append("")
    
    report.append("## 1. Overview")
    report.append(f"- **Block Range:** {start_blk:,} - {end_blk:,}")
    report.append(f"- **Time Period:** {start_date} to {end_date} ({days} days)")
    report.append(f"- **Total Blocks Analyzed:** {total_blocks:,}")
    report.append(f"- **Avg Base Fee:** {avg_base_fee_gwei:.2f} Gwei (Median: {median_base_fee_gwei:.2f} Gwei)")
    report.append(f"- **Gas Utilization:** {gas_utilization:.1f}%")
    report.append(f"- **Mempool Coverage (Next Block):** {mempool_cov:.1f}%")
    report.append("")

    report.append("## 2. Inclusion List Strategies")
    report.append("Comparing the standard 'Top Fee' FOCIL strategy against a 'Censored-Only' robust strategy.")
    report.append("")
    
    report.append("| Metric | Top Fee (Standard) | Censored (Robust) | Delta |")
    report.append("| :--- | :--- | :--- | :--- |")
    report.append(f"| **Avg IL Size (Tx/blk)** | {tf_txs:.1f} | {c_txs:.1f} | {((c_txs-tf_txs)/tf_txs*100):.1f}% |")
    report.append(f"| **Avg IL Size (KiB/blk)** | {tf_kb:.2f} KiB | {c_kb:.2f} KiB | {((c_kb-tf_kb)/tf_kb*100):.1f}% |")
    report.append(f"| **Annual Bandwidth** | **{tf_annual_gb:.2f} GB/yr** | **{c_annual_gb:.2f} GB/yr** | **{((c_annual_gb-tf_annual_gb)/tf_annual_gb*100):.1f}%** |")
    report.append(f"| **0-Delay Inclusion Rate** | {tf_inc_0:.1f}% | {c_inc_0:.1f}% | - |")
    report.append(f"| **1-Delay Inclusion Rate** | {tf_inc_1:.1f}% | {c_inc_1:.1f}% | - |")
    report.append(f"| **2-Delay Inclusion Rate** | {tf_inc_2:.1f}% | {c_inc_2:.1f}% | - |")
    report.append("")

    report.append("### Efficiency (Top Fee Strategy)")
    report.append(f"- **Useful Data:** {tf_useful/1024:.2f} KiB/blk")
    report.append(f"- **Redundant Data:** {tf_redundant/1024:.2f} KiB/blk")
    report.append(f"- **Efficiency:** {tf_efficiency:.1f}% (unique bytes / total bytes)")
    report.append("")

    report.append("## 3. Censorship & Latency Detection")
    report.append("Transactions detected as 'high fee but not included' (potential censorship or latency issues).")
    report.append(f"- **Avg Suspicious Txs per Block:** {censored_avg:.2f}")
    report.append(f"- **Blocks with Suspicious Txs:** {censored_blocks_pct:.1f}%")
    
    # Distribution of censorship counts
    if 'censored_detected_count' in df.columns:
        counts = df['censored_detected_count'].value_counts().sort_index().head(5)
        report.append("\n**Distribution (Top 5 counts):**")
        for count, freq in counts.items():
            pct = (freq / total_blocks) * 100
            report.append(f"- {int(count)} txs: {freq:,} blocks ({pct:.1f}%)")

    print("\n".join(report))

if __name__ == "__main__":
    file_path = Path("/home/router/eth-mempool-analysis/results/bn_full_analysis.parquet")
    if file_path.exists():
        generate_report(file_path)
    else:
        print(f"Error: File not found at {file_path}")

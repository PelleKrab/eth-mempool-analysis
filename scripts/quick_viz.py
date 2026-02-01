#!/usr/bin/env python3
"""
Quick visualization of IL metrics
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys


def plot_il_metrics(df: pd.DataFrame, output_dir: Path):
    """Generate quick visualizations of IL metrics"""

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Ethereum Inclusion List Metrics - January 2025 (First Week)', fontsize=16)

    # Plot 1: IL transaction count over time
    ax = axes[0, 0]
    ax.plot(df['block_number'], df['il_tx_count'], linewidth=0.5, alpha=0.7)
    ax.set_xlabel('Block Number')
    ax.set_ylabel('IL Transaction Count')
    ax.set_title('IL Size (Transaction Count)')
    ax.grid(True, alpha=0.3)

    # Plot 2: IL size in bytes
    ax = axes[0, 1]
    ax.plot(df['block_number'], df['il_size_bytes']/1024, linewidth=0.5, alpha=0.7)
    ax.set_xlabel('Block Number')
    ax.set_ylabel('IL Size (KB)')
    ax.set_title('IL Size (Bytes)')
    ax.grid(True, alpha=0.3)

    # Plot 3: Time offset distribution
    ax = axes[1, 0]
    ax.hist(df['avg_time_offset_secs'].dropna(), bins=50, edgecolor='black', alpha=0.7)
    ax.set_xlabel('Average Time Offset (seconds)')
    ax.set_ylabel('Frequency')
    ax.set_title('Distribution of Average Time Offsets')
    ax.axvline(x=0, color='r', linestyle='--', label='Block time')
    ax.legend()
    ax.grid(True, alpha=0.3)

    # Plot 4: Priority fee over time
    ax = axes[1, 1]
    ax.plot(df['block_number'], df['median_priority_fee']/1e9, linewidth=0.5, alpha=0.7)
    ax.set_xlabel('Block Number')
    ax.set_ylabel('Median Priority Fee (Gwei)')
    ax.set_title('Median Priority Fee in Time Window')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    # Save plot
    plot_file = output_dir / "il_metrics_overview.png"
    plt.savefig(plot_file, dpi=150, bbox_inches='tight')
    print(f"Saved plot: {plot_file}")

    # Create a second figure for detailed analysis
    fig2, axes2 = plt.subplots(2, 1, figsize=(15, 10))
    fig2.suptitle('IL Coverage Analysis', fontsize=16)

    # IL transaction count vs included transaction count
    ax = axes2[0]
    ax.scatter(df['included_tx_count'], df['il_tx_count'], alpha=0.1, s=1)
    ax.set_xlabel('Included Transaction Count')
    ax.set_ylabel('IL Transaction Count')
    ax.set_title('IL Size vs Block Size')
    ax.grid(True, alpha=0.3)

    # Time window transaction count over time
    ax = axes2[1]
    ax.plot(df['block_number'], df['time_window_tx_count'], linewidth=0.5, alpha=0.7, label='Time Window Txs')
    ax.plot(df['block_number'], df['included_tx_count'], linewidth=0.5, alpha=0.7, label='Included Txs')
    ax.set_xlabel('Block Number')
    ax.set_ylabel('Transaction Count')
    ax.set_title('Time Window Transactions vs Included Transactions')
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    plot_file2 = output_dir / "il_coverage_analysis.png"
    plt.savefig(plot_file2, dpi=150, bbox_inches='tight')
    print(f"Saved plot: {plot_file2}")

    plt.close('all')


def main():
    results_dir = Path(__file__).parent.parent / "results"
    combined_file = results_dir / "block_il_metrics_combined.parquet"

    if not combined_file.exists():
        print(f"Combined results not found: {combined_file}")
        print("Run combine_results.py first!")
        sys.exit(1)

    print(f"Loading data from {combined_file}...")
    df = pd.read_parquet(combined_file)

    print(f"Generating visualizations for {len(df)} blocks...")
    plot_il_metrics(df, results_dir)

    print("\n✓ Visualizations complete!")


if __name__ == "__main__":
    main()

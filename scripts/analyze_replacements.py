#!/usr/bin/env python3
"""
Analyze nonce replacement patterns
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path


def analyze_replacements(df: pd.DataFrame):
    """Generate comprehensive replacement analysis"""

    print("="*70)
    print("NONCE REPLACEMENT ANALYSIS")
    print("="*70)

    # Filter valid replacements
    valid = df[np.isfinite(df['fee_multiplier']) & (df['original_max_fee'] > 0)].copy()

    print(f"\n## DATASET")
    print(f"Total replacement events: {len(df):,}")
    print(f"Valid replacements (finite fees): {len(valid):,}")

    print(f"\n## REPLACEMENT TYPES")
    same_fee = valid[valid['fee_multiplier'] == 1.0]
    fee_increase = valid[valid['fee_multiplier'] > 1.0]
    fee_decrease = valid[valid['fee_multiplier'] < 1.0]

    print(f"Same fee (1.0x): {len(same_fee):,} ({len(same_fee)/len(valid)*100:.1f}%)")
    print(f"  → Likely nonce cancellations or tx modifications")
    print(f"Fee increase (>1.0x): {len(fee_increase):,} ({len(fee_increase)/len(valid)*100:.1f}%)")
    print(f"  → True RBF (Replace-By-Fee)")
    print(f"Fee decrease (<1.0x): {len(fee_decrease):,} ({len(fee_decrease)/len(valid)*100:.1f}%)")
    print(f"  → Unusual, possibly error correction")

    print(f"\n## FEE MULTIPLIER STATISTICS")
    print(f"All valid replacements:")
    print(f"  Mean: {valid['fee_multiplier'].mean():.3f}x")
    print(f"  Median: {valid['fee_multiplier'].median():.3f}x")
    print(f"  95th percentile: {valid['fee_multiplier'].quantile(0.95):.3f}x")
    print(f"  99th percentile: {valid['fee_multiplier'].quantile(0.99):.3f}x")

    if len(fee_increase) > 0:
        print(f"\nFee increases only:")
        print(f"  Mean: {fee_increase['fee_multiplier'].mean():.3f}x")
        print(f"  Median: {fee_increase['fee_multiplier'].median():.3f}x")
        print(f"  95th percentile: {fee_increase['fee_multiplier'].quantile(0.95):.3f}x")
        print(f"  99th percentile: {fee_increase['fee_multiplier'].quantile(0.99):.3f}x")

    print(f"\n## REPLACEMENT COUNT DISTRIBUTION")
    counts = df['replacement_count'].value_counts().sort_index()
    print("How many versions of each transaction:")
    for i in range(2, min(11, counts.index.max()+1)):
        if i in counts.index:
            pct = counts[i] / len(df) * 100
            print(f"  {i} versions: {counts[i]:,} ({pct:.1f}%)")
    if counts.index.max() > 10:
        many = counts[counts.index > 10].sum()
        pct = many / len(df) * 100
        print(f"  >10 versions: {many:,} ({pct:.1f}%)")
        print(f"  Max versions: {counts.index.max()}")

    print(f"\n## TIMING STATISTICS")
    print(f"Replacement time (seconds):")
    print(f"  Mean: {df['replacement_time_secs'].mean():.1f}s")
    print(f"  Median: {df['replacement_time_secs'].median():.1f}s")
    print(f"  95th percentile: {df['replacement_time_secs'].quantile(0.95):.1f}s")

    # Quick replacements (< 1 minute)
    quick = df[df['replacement_time_secs'] < 60]
    print(f"\nQuick replacements (<1 min): {len(quick):,} ({len(quick)/len(df)*100:.1f}%)")

    # Slow replacements (> 1 hour)
    slow = df[df['replacement_time_secs'] > 3600]
    print(f"Slow replacements (>1 hour): {len(slow):,} ({len(slow)/len(df)*100:.1f}%)")

    print("\n" + "="*70)


def plot_replacements(df: pd.DataFrame, output_dir: Path):
    """Create replacement visualizations"""

    valid = df[np.isfinite(df['fee_multiplier']) & (df['original_max_fee'] > 0) & (df['fee_multiplier'] > 0)]

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Transaction Replacement Analysis', fontsize=16)

    # Plot 1: Fee multiplier distribution (log scale, excluding 1.0x)
    ax = axes[0, 0]
    increased = valid[valid['fee_multiplier'] > 1.0]
    if len(increased) > 0:
        ax.hist(np.log10(increased['fee_multiplier']), bins=50, edgecolor='black', alpha=0.7)
        ax.set_xlabel('Fee Multiplier (log10)')
        ax.set_ylabel('Frequency')
        ax.set_title(f'Fee Increase Distribution ({len(increased):,} replacements)')
        ax.grid(True, alpha=0.3)

    # Plot 2: Replacement count distribution
    ax = axes[0, 1]
    counts = df['replacement_count'].value_counts().sort_index()
    ax.bar(counts.index, counts.values, edgecolor='black', alpha=0.7)
    ax.set_xlabel('Number of Versions')
    ax.set_ylabel('Frequency')
    ax.set_title('Transaction Version Count Distribution')
    ax.set_xticks(range(2, min(11, counts.index.max()+1)))
    ax.grid(True, alpha=0.3, axis='y')

    # Plot 3: Replacement time distribution
    ax = axes[1, 0]
    time_bins = [0, 10, 60, 300, 3600, 7200, df['replacement_time_secs'].max()]
    time_labels = ['<10s', '10s-1m', '1m-5m', '5m-1h', '1h-2h', '>2h']
    time_counts = pd.cut(df['replacement_time_secs'], bins=time_bins, labels=time_labels).value_counts()
    ax.bar(range(len(time_counts)), time_counts.values, edgecolor='black', alpha=0.7)
    ax.set_xticks(range(len(time_labels)))
    ax.set_xticklabels(time_labels, rotation=45)
    ax.set_ylabel('Frequency')
    ax.set_title('Replacement Time Distribution')
    ax.grid(True, alpha=0.3, axis='y')

    # Plot 4: Same fee vs fee increase over time
    ax = axes[1, 1]
    df_sorted = df.sort_values('final_timestamp')
    df_sorted['is_fee_increase'] = df_sorted['fee_multiplier'] > 1.0
    rolling_window = 1000
    rolling_pct = df_sorted['is_fee_increase'].rolling(rolling_window).mean() * 100
    ax.plot(range(len(rolling_pct)), rolling_pct, linewidth=0.5, alpha=0.7)
    ax.set_xlabel(f'Replacement Number (rolling {rolling_window})')
    ax.set_ylabel('% with Fee Increase')
    ax.set_title('Fee Increase Rate Over Time')
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    plot_file = output_dir / "replacement_analysis.png"
    plt.savefig(plot_file, dpi=150, bbox_inches='tight')
    print(f"\nSaved plot: {plot_file}")
    plt.close()


def main():
    results_dir = Path(__file__).parent.parent / "results"
    combined_file = results_dir / "nonce_replacements_combined.parquet"

    if not combined_file.exists():
        print(f"Combined results not found: {combined_file}")
        print("Run combine_results.py first!")
        return

    print(f"Loading data from {combined_file}...")
    df = pd.read_parquet(combined_file)

    analyze_replacements(df)
    plot_replacements(df, results_dir)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
FOCIL Bandwidth Analysis: 1-slot vs 2-slot delay
Calculates bandwidth savings from delaying inclusion list enforcement
"""

import pandas as pd
from pathlib import Path
import numpy as np


def analyze_focil_bandwidth(il_df: pd.DataFrame):
    """
    Analyze bandwidth savings from FOCIL slot delays

    For each block N:
    - L₀: IL based on block N timestamp (no delay)
    - L₋₁: IL based on block N-1 timestamp (1-slot delay)
    - L₋₂: IL based on block N-2 timestamp (2-slot delay)

    Bandwidth savings = fewer redundant transactions
    """

    print("="*70)
    print("FOCIL BANDWIDTH ANALYSIS")
    print("1-slot vs 2-slot Delay Comparison")
    print("="*70)

    # Sort by block number
    df = il_df.sort_values('block_number').reset_index(drop=True)

    # Create shifted dataframes for L₋₁ and L₋₂
    df['L_minus_1_size'] = df['il_size_bytes'].shift(1)
    df['L_minus_1_count'] = df['il_tx_count'].shift(1)

    df['L_minus_2_size'] = df['il_size_bytes'].shift(2)
    df['L_minus_2_count'] = df['il_tx_count'].shift(2)

    # Remove first 2 blocks (no previous data)
    df = df[df['block_number'] > df['block_number'].min() + 2].copy()

    # Calculate bandwidth metrics
    # Assumption: ~70% of IL transactions get included naturally over time
    # This is conservative - likely higher with delay

    # L₀ (0-delay): Current block IL
    df['L0_size'] = df['il_size_bytes']
    df['L0_count'] = df['il_tx_count']

    # L₋₁ (1-slot delay): Assume 30% overlap with block contents
    # (txs that were in L₋₁ but got included in block N)
    df['L1_overlap_rate'] = 0.30
    df['L1_effective_size'] = df['L_minus_1_size'] * (1 - df['L1_overlap_rate'])
    df['L1_bandwidth_saved'] = df['L0_size'] - df['L1_effective_size']

    # L₋₂ (2-slot delay): Assume 50% overlap
    # (more time means more txs naturally included)
    df['L2_overlap_rate'] = 0.50
    df['L2_effective_size'] = df['L_minus_2_size'] * (1 - df['L2_overlap_rate'])
    df['L2_bandwidth_saved'] = df['L0_size'] - df['L2_effective_size']

    # Calculate percentages
    df['L1_savings_pct'] = (df['L1_bandwidth_saved'] / df['L0_size']) * 100
    df['L2_savings_pct'] = (df['L2_bandwidth_saved'] / df['L0_size']) * 100

    # Summary statistics
    print(f"\n## DATASET")
    print(f"Blocks analyzed: {len(df):,}")
    print(f"Block range: {df['block_number'].min():,} to {df['block_number'].max():,}")

    print(f"\n## INCLUSION LIST SIZES")
    print(f"L₀ (0-delay):")
    print(f"  Average: {df['L0_size'].mean() / 1024:.2f} KB")
    print(f"  Median: {df['L0_size'].median() / 1024:.2f} KB")

    print(f"\nL₋₁ (1-slot delay):")
    print(f"  Average: {df['L_minus_1_size'].mean() / 1024:.2f} KB")
    print(f"  Effective (after overlap): {df['L1_effective_size'].mean() / 1024:.2f} KB")

    print(f"\nL₋₂ (2-slot delay):")
    print(f"  Average: {df['L_minus_2_size'].mean() / 1024:.2f} KB")
    print(f"  Effective (after overlap): {df['L2_effective_size'].mean() / 1024:.2f} KB")

    print(f"\n## BANDWIDTH SAVINGS")
    print(f"\n1-Slot Delay (L₋₁):")
    print(f"  Average savings: {df['L1_bandwidth_saved'].mean() / 1024:.2f} KB per block")
    print(f"  Percentage: {df['L1_savings_pct'].mean():.1f}%")
    print(f"  Daily savings: {df['L1_bandwidth_saved'].mean() * 7200 / (1024**2):.2f} MB")
    print(f"  Annual savings: {df['L1_bandwidth_saved'].mean() * 7200 * 365 / (1024**3):.2f} GB")

    print(f"\n2-Slot Delay (L₋₂):")
    print(f"  Average savings: {df['L2_bandwidth_saved'].mean() / 1024:.2f} KB per block")
    print(f"  Percentage: {df['L2_savings_pct'].mean():.1f}%")
    print(f"  Daily savings: {df['L2_bandwidth_saved'].mean() * 7200 / (1024**2):.2f} MB")
    print(f"  Annual savings: {df['L2_bandwidth_saved'].mean() * 7200 * 365 / (1024**3):.2f} GB")

    print(f"\n## COMPARISON")
    additional_savings = df['L2_bandwidth_saved'].mean() - df['L1_bandwidth_saved'].mean()
    print(f"Additional savings from 2-slot vs 1-slot:")
    print(f"  Per block: {additional_savings / 1024:.2f} KB")
    print(f"  Per day: {additional_savings * 7200 / (1024**2):.2f} MB")
    print(f"  Per year: {additional_savings * 7200 * 365 / (1024**3):.2f} GB")
    print(f"  Percentage improvement: {(additional_savings / df['L1_bandwidth_saved'].mean()) * 100:.1f}%")

    print(f"\n## CENSORSHIP WINDOW TRADE-OFF")
    print(f"1-Slot Delay: Censorship window = ~12 seconds")
    print(f"2-Slot Delay: Censorship window = ~24 seconds")
    print(f"Bandwidth reduction for 2x longer censorship window: {df['L2_savings_pct'].mean():.1f}%")

    print("\n" + "="*70)

    # Save detailed results
    results_dir = Path(__file__).parent.parent / "results"
    output_file = results_dir / "focil_bandwidth_analysis.csv"

    output_df = df[[
        'block_number', 'block_timestamp', 'included_tx_count',
        'L0_size', 'L0_count',
        'L_minus_1_size', 'L1_effective_size', 'L1_bandwidth_saved', 'L1_savings_pct',
        'L_minus_2_size', 'L2_effective_size', 'L2_bandwidth_saved', 'L2_savings_pct'
    ]].copy()

    output_df.to_csv(output_file, index=False)
    print(f"\nDetailed results saved to: {output_file}")

    return output_df


def main():
    results_dir = Path(__file__).parent.parent / "results"
    il_file = results_dir / "block_il_metrics_combined.parquet"

    if not il_file.exists():
        print(f"IL metrics file not found: {il_file}")
        print("Run batch_processor.py first!")
        return

    print(f"Loading IL metrics from {il_file}...")
    il_df = pd.read_parquet(il_file)

    analyze_focil_bandwidth(il_df)


if __name__ == "__main__":
    main()

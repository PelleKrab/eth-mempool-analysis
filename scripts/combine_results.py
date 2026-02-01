#!/usr/bin/env python3
"""
Combine batch results into single files and generate summary statistics
"""

import glob
import pandas as pd
from pathlib import Path
import sys


def combine_il_metrics(results_dir: Path):
    """Combine all IL metrics files"""
    pattern = str(results_dir / "block_il_metrics_*.parquet")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"No IL metrics files found in {results_dir}")
        return None

    print(f"Found {len(files)} IL metrics files")

    # Load and concatenate
    dfs = []
    for f in files:
        df = pd.read_parquet(f)
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values('block_number').reset_index(drop=True)

    # Save combined file
    output_file = results_dir / "block_il_metrics_combined.parquet"
    combined.to_parquet(output_file, compression='snappy', index=False)
    print(f"Saved combined file: {output_file} ({len(combined)} rows)")

    # Generate summary statistics
    print("\n=== IL Metrics Summary ===")
    print(f"Block range: {combined['block_number'].min()} to {combined['block_number'].max()}")
    print(f"Total blocks: {len(combined):,}")
    print(f"\nInclusion List Statistics:")
    print(f"  Avg IL tx count: {combined['il_tx_count'].mean():.2f}")
    print(f"  Median IL tx count: {combined['il_tx_count'].median():.0f}")
    print(f"  Max IL tx count: {combined['il_tx_count'].max()}")
    print(f"  Avg IL size: {combined['il_size_bytes'].mean()/1024:.2f} KB")
    print(f"  Max IL size: {combined['il_size_bytes'].max()/1024:.2f} KB")
    print(f"\nTime Window Statistics:")
    print(f"  Avg time offset: {combined['avg_time_offset_secs'].mean():.2f} seconds")
    print(f"  Avg priority fee: {combined['avg_priority_fee'].mean()/1e9:.2f} Gwei")
    print(f"  Median priority fee: {combined['median_priority_fee'].median()/1e9:.2f} Gwei")

    return combined


def combine_nonce_replacements(results_dir: Path):
    """Combine all nonce replacement files"""
    pattern = str(results_dir / "nonce_replacements_*.parquet")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"\nNo nonce replacement files found")
        return None

    print(f"\nFound {len(files)} nonce replacement files")

    dfs = [pd.read_parquet(f) for f in files]
    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values('final_timestamp').reset_index(drop=True)

    output_file = results_dir / "nonce_replacements_combined.parquet"
    combined.to_parquet(output_file, compression='snappy', index=False)
    print(f"Saved combined file: {output_file} ({len(combined)} rows)")

    print("\n=== Replacement Summary ===")
    print(f"Total replacements: {len(combined):,}")
    print(f"Avg fee multiplier: {combined['fee_multiplier'].mean():.2f}x")
    print(f"Median fee multiplier: {combined['fee_multiplier'].median():.2f}x")
    print(f"Max replacement count: {combined['replacement_count'].max()}")

    return combined


def combine_bandwidth_analysis(results_dir: Path):
    """Combine all bandwidth analysis files"""
    pattern = str(results_dir / "bandwidth_analysis_*.parquet")
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"\nNo bandwidth analysis files found")
        return None

    print(f"\nFound {len(files)} bandwidth analysis files")

    dfs = [pd.read_parquet(f) for f in files]
    combined = pd.concat(dfs, ignore_index=True)
    combined = combined.sort_values('block_number').reset_index(drop=True)

    output_file = results_dir / "bandwidth_analysis_combined.parquet"
    combined.to_parquet(output_file, compression='snappy', index=False)
    print(f"Saved combined file: {output_file} ({len(combined)} rows)")

    print("\n=== Bandwidth Summary ===")
    print(f"Avg bandwidth savings: {combined['bandwidth_savings_percent'].mean():.2f}%")
    print(f"Median bandwidth savings: {combined['bandwidth_savings_percent'].median():.2f}%")

    return combined


def main():
    results_dir = Path(__file__).parent.parent / "results"

    if not results_dir.exists():
        print(f"Results directory not found: {results_dir}")
        sys.exit(1)

    print("Combining batch results...\n")

    # Combine each type of result
    il_metrics = combine_il_metrics(results_dir)
    nonce_replacements = combine_nonce_replacements(results_dir)
    bandwidth_analysis = combine_bandwidth_analysis(results_dir)

    print("\n" + "="*60)
    print("✓ All results combined!")
    print("="*60)

    # Save CSV versions for easy viewing
    if il_metrics is not None:
        csv_file = results_dir / "block_il_metrics_combined.csv"
        il_metrics.to_csv(csv_file, index=False)
        print(f"\nCSV version saved: {csv_file}")


if __name__ == "__main__":
    main()

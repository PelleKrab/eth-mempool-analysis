#!/usr/bin/env python3
"""
Combine Chunk Results

Combines all chunk parquet files into a single analysis file and generates
summary statistics.

Usage:
    python combine_chunks.py
    python combine_chunks.py --input-dir results/chunks --output results/full_year_analysis.parquet
"""

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)


def combine_chunks(input_dir: Path, output_file: Path, verbose: bool = False):
    """Combine all chunk parquet files into a single file."""

    # Find all chunk files
    chunk_files = sorted(input_dir.glob("chunk_*.parquet"))

    if not chunk_files:
        log.error(f"No chunk files found in {input_dir}")
        return None

    log.info(f"Found {len(chunk_files):,} chunk files")

    # Load and combine
    all_chunks = []
    total_blocks = 0

    for chunk_file in chunk_files:
        if verbose:
            log.info(f"Loading {chunk_file.name}...")

        df = pd.read_parquet(chunk_file)
        all_chunks.append(df)
        total_blocks += len(df)

        if verbose:
            log.info(f"  Loaded {len(df):,} blocks")

    log.info(f"Combining {len(all_chunks):,} chunks with {total_blocks:,} total blocks...")
    combined_df = pd.concat(all_chunks, ignore_index=True)

    # Sort by block number to ensure order
    combined_df = combined_df.sort_values('block_number').reset_index(drop=True)

    # Save combined results
    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(output_file, index=False)

    log.info(f"Saved combined results to: {output_file}")
    log.info(f"Total blocks: {len(combined_df):,}")

    # Print summary statistics
    print_summary_stats(combined_df)

    return combined_df


def print_summary_stats(df: pd.DataFrame):
    """Print summary statistics for the combined dataset."""
    blocks_per_year = 7200 * 365

    print("\n" + "=" * 70)
    print("COMBINED ANALYSIS SUMMARY")
    print("=" * 70)

    print(f"\nBlocks analyzed: {len(df):,}")
    print(f"Block range: {df['block_number'].min():,} to {df['block_number'].max():,}")

    if 'block_timestamp' in df.columns:
        import datetime
        start_date = datetime.datetime.fromtimestamp(df['block_timestamp'].min())
        end_date = datetime.datetime.fromtimestamp(df['block_timestamp'].max())
        duration_days = (end_date - start_date).days
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({duration_days} days)")

    # Gas usage
    if 'gas_used' in df.columns and 'gas_limit' in df.columns:
        avg_gas_pct = df['gas_used'].mean() / df['gas_limit'].mean() * 100
        print(f"\nAverage gas usage: {df['gas_used'].mean() / 1e6:.2f}M ({avg_gas_pct:.1f}% of limit)")

    # Mempool coverage
    if 'mempool_coverage_of_next_block' in df.columns:
        print(f"Avg mempool coverage of next block: {df['mempool_coverage_of_next_block'].mean():.1f}%")

    # Top Fee Strategy
    print("\n" + "-" * 70)
    print("TOP FEE STRATEGY")
    print("-" * 70)

    if '0delay_topfee_size_bytes' in df.columns:
        avg_kb = df['0delay_topfee_size_bytes'].mean() / 1024
        avg_count = df['0delay_topfee_tx_count'].mean()
        annual_gb = df['0delay_topfee_size_bytes'].mean() * blocks_per_year / (1024 ** 3)
        print(f"IL size: {avg_kb:.2f} KiB/block, {avg_count:.1f} txs, {annual_gb:.2f} GB/year")

    for delay in [0, 1, 2]:
        col = f'{delay}delay_topfee_inclusion_rate'
        if col in df.columns:
            rate = df[col].dropna().mean()
            print(f"  {delay}-delay inclusion rate: {rate:.1f}%")

    # Censored Strategy
    print("\n" + "-" * 70)
    print("CENSORED STRATEGY")
    print("-" * 70)

    if '0delay_censored_size_bytes' in df.columns:
        avg_kb = df['0delay_censored_size_bytes'].mean() / 1024
        avg_count = df['0delay_censored_tx_count'].mean()
        annual_gb = df['0delay_censored_size_bytes'].mean() * blocks_per_year / (1024 ** 3)
        print(f"IL size: {avg_kb:.2f} KiB/block, {avg_count:.1f} txs, {annual_gb:.2f} GB/year")

    for delay in [0, 1, 2]:
        col = f'{delay}delay_censored_inclusion_rate'
        if col in df.columns:
            rate = df[col].dropna().mean()
            print(f"  {delay}-delay inclusion rate: {rate:.1f}%")

    # Censorship detection
    if 'censored_detected_count' in df.columns:
        print("\n" + "-" * 70)
        print("CENSORSHIP DETECTION")
        print("-" * 70)
        avg = df['censored_detected_count'].mean()
        blocks_with = (df['censored_detected_count'] > 0).sum()
        pct = blocks_with / len(df) * 100
        print(f"Average censored txs/block: {avg:.2f}")
        print(f"Blocks with censorship: {blocks_with:,} ({pct:.1f}%)")

    # Bandwidth comparison
    if '0delay_topfee_size_bytes' in df.columns and '0delay_censored_size_bytes' in df.columns:
        print("\n" + "-" * 70)
        print("BANDWIDTH COMPARISON")
        print("-" * 70)
        topfee_annual = df['0delay_topfee_size_bytes'].mean() * blocks_per_year / (1024 ** 3)
        censored_annual = df['0delay_censored_size_bytes'].mean() * blocks_per_year / (1024 ** 3)
        diff_pct = (censored_annual / topfee_annual - 1) * 100 if topfee_annual > 0 else 0
        print(f"Top Fee:  {topfee_annual:.2f} GB/year")
        print(f"Censored: {censored_annual:.2f} GB/year ({diff_pct:+.1f}% vs top fee)")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Combine chunk results into final analysis")
    parser.add_argument("--input-dir", type=str, default="results/chunks",
                       help="Directory containing chunk parquet files")
    parser.add_argument("--output", type=str, default="results/focil_full_year_analysis.parquet",
                       help="Output file for combined results")
    parser.add_argument("--verbose", action="store_true",
                       help="Print verbose loading information")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    input_dir = Path(__file__).parent.parent / args.input_dir
    output_file = Path(__file__).parent.parent / args.output

    if not input_dir.exists():
        log.error(f"Input directory does not exist: {input_dir}")
        return 1

    result = combine_chunks(input_dir, output_file, args.verbose)

    return 0 if result is not None else 1


if __name__ == "__main__":
    sys.exit(main())

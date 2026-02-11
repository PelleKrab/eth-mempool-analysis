#!/usr/bin/env python3
"""
Data quality verification script for FOCIL analysis output.
Checks for gaps, duplicates, null values, and reasonable value ranges.
"""
import sys
from pathlib import Path
import pandas as pd
import numpy as np

def verify_data_quality(parquet_file: Path) -> bool:
    """
    Verify data quality of a parquet file.
    Returns True if all checks pass, False otherwise.
    """
    print(f"\n{'='*70}")
    print(f"DATA QUALITY VERIFICATION: {parquet_file.name}")
    print(f"{'='*70}\n")

    try:
        df = pd.read_parquet(parquet_file)
    except Exception as e:
        print(f"❌ ERROR: Failed to load parquet file: {e}")
        return False

    all_checks_passed = True

    # Check 1: Row count
    print(f"✓ Loaded {len(df):,} blocks")
    if len(df) == 0:
        print("❌ ERROR: No data in file")
        return False

    # Check 2: Block number range and gaps
    min_block = df['block_number'].min()
    max_block = df['block_number'].max()
    expected_count = max_block - min_block + 1
    print(f"✓ Block range: {min_block:,} to {max_block:,}")

    if len(df) != expected_count:
        print(f"❌ ERROR: Block gap detected! Expected {expected_count:,} blocks, got {len(df):,}")
        all_checks_passed = False
    else:
        print(f"✓ No gaps: {len(df):,} consecutive blocks")

    # Check 3: Duplicate blocks
    duplicates = df['block_number'].duplicated().sum()
    if duplicates > 0:
        print(f"❌ ERROR: {duplicates} duplicate block numbers found")
        all_checks_passed = False
    else:
        print("✓ No duplicate blocks")

    # Check 4: Column count
    expected_cols = [
        'block_number', 'block_timestamp', 'base_fee', 'gas_used', 'gas_limit',
        'included_tx_count', 'mempool_coverage_of_next_block',
        'mempool_unique_txs_in_window', 'censored_detected_count'
    ]
    # Add variant columns (6 variants × 3 metrics each = 18 columns)
    variants = ['0delay_topfee', '1delay_topfee', '2delay_topfee',
                '0delay_censored', '1delay_censored', '2delay_censored']
    for variant in variants:
        expected_cols.extend([
            f'{variant}_tx_count',
            f'{variant}_size_bytes',
            f'{variant}_inclusion_rate'
        ])

    missing_cols = set(expected_cols) - set(df.columns)
    if missing_cols:
        print(f"❌ ERROR: Missing columns: {missing_cols}")
        all_checks_passed = False
    else:
        print(f"✓ All {len(expected_cols)} expected columns present")

    # Check 5: Null values in critical columns
    critical_cols = ['block_number', 'block_timestamp', 'base_fee', 'gas_used', 'gas_limit']
    null_counts = df[critical_cols].isnull().sum()
    if null_counts.any():
        print(f"❌ ERROR: Null values in critical columns:")
        for col, count in null_counts[null_counts > 0].items():
            print(f"  - {col}: {count} nulls")
        all_checks_passed = False
    else:
        print("✓ No null values in critical columns")

    # Check 6: Value ranges
    print("\n" + "="*70)
    print("VALUE RANGE CHECKS")
    print("="*70)

    # Base fee should be positive
    if (df['base_fee'] <= 0).any():
        print("❌ ERROR: Non-positive base_fee values found")
        all_checks_passed = False
    else:
        print(f"✓ Base fee range: {df['base_fee'].min():,.0f} to {df['base_fee'].max():,.0f} wei")

    # Gas used should not exceed gas limit
    over_limit = (df['gas_used'] > df['gas_limit']).sum()
    if over_limit > 0:
        print(f"❌ ERROR: {over_limit} blocks with gas_used > gas_limit")
        all_checks_passed = False
    else:
        print(f"✓ Gas used within limits (max: {df['gas_used'].max():,})")

    # Inclusion rates should be 0-100 (stored as percentages)
    for variant in variants:
        col = f'{variant}_inclusion_rate'
        if col in df.columns:
            # Allow None values, check non-null values
            non_null = df[col].dropna()
            if len(non_null) > 0:
                invalid = ((non_null < 0) | (non_null > 100)).sum()
                if invalid > 0:
                    print(f"❌ ERROR: {col} has {invalid} values outside [0,100] range")
                    all_checks_passed = False
    print("✓ All inclusion rates in valid range [0,100]")

    # IL sizes should not exceed 8 KiB (8192 bytes) per EIP-7805
    for variant in variants:
        col = f'{variant}_size_bytes'
        if col in df.columns:
            oversized = (df[col] > 8192).sum()
            if oversized > 0:
                print(f"❌ ERROR: {col} has {oversized} values exceeding 8 KiB limit")
                all_checks_passed = False
    print("✓ All IL sizes within 8 KiB limit")

    # Check 7: Summary statistics
    print("\n" + "="*70)
    print("SUMMARY STATISTICS")
    print("="*70)

    print(f"\nCensorship Detection:")
    blocks_with_censorship = (df['censored_detected_count'] > 0).sum()
    print(f"  - Blocks with censored txs: {blocks_with_censorship:,} ({blocks_with_censorship/len(df)*100:.1f}%)")
    if blocks_with_censorship > 0:
        print(f"  - Avg censored txs/block: {df[df['censored_detected_count']>0]['censored_detected_count'].mean():.1f}")
        print(f"  - Max censored txs/block: {df['censored_detected_count'].max():,}")

    print(f"\nTop Fee ILs (0-delay):")
    print(f"  - Avg size: {df['0delay_topfee_size_bytes'].mean():.2f} bytes")
    print(f"  - Avg tx count: {df['0delay_topfee_tx_count'].mean():.1f}")
    print(f"  - Avg inclusion rate: {df['0delay_topfee_inclusion_rate'].mean():.1f}%")

    print(f"\nCensored ILs (0-delay):")
    print(f"  - Avg size: {df['0delay_censored_size_bytes'].mean():.2f} bytes")
    print(f"  - Avg tx count: {df['0delay_censored_tx_count'].mean():.1f}")
    non_empty = df[df['0delay_censored_tx_count'] > 0]
    if len(non_empty) > 0:
        print(f"  - Avg inclusion rate (non-empty): {non_empty['0delay_censored_inclusion_rate'].mean():.1f}%")

    print(f"\nMempool Coverage:")
    print(f"  - Avg coverage: {df['mempool_coverage_of_next_block'].mean():.1f}%")
    print(f"  - Avg unique txs in window: {df['mempool_unique_txs_in_window'].mean():.0f}")

    # Final verdict
    print("\n" + "="*70)
    if all_checks_passed:
        print("✅ ALL DATA QUALITY CHECKS PASSED")
    else:
        print("❌ SOME DATA QUALITY CHECKS FAILED")
    print("="*70 + "\n")

    return all_checks_passed


def main():
    if len(sys.argv) > 1:
        parquet_file = Path(sys.argv[1])
    else:
        # Default: check the combined file
        parquet_file = Path("results/focil_full_year_analysis.parquet")

    if not parquet_file.exists():
        print(f"ERROR: File not found: {parquet_file}")
        sys.exit(1)

    success = verify_data_quality(parquet_file)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

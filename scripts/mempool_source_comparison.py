#!/usr/bin/env python3
"""
Mempool Source Comparison: Xatu vs BlockNative

Side-by-side comparison of two mempool data sources available in Xatu ClickHouse:
  - mempool_transaction (ethPandaOps sentries)
  - block_native_mempool_transaction (BlockNative)

Compares per block:
  1. Coverage: tx overlap, source-exclusive txs, next-block coverage
  2. Timing: which source sees txs first, average detection delta
  3. BN metadata: status distribution, stuck rate, drop/rejection reasons
  4. FOCIL ILs: top-fee ILs from each source, inclusion rates, IL overlap

Both sources must have data for the chosen block range. Good overlap exists
for Feb–Mar 2025 (blocks ~21750000–22000000).

Reference: EIP-7805 (FOCIL), 8 KiB inclusion list cap.
"""

import logging

import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from utils import (
    load_config, execute_query, fetch_block_data, fetch_included_txs,
    pack_il, MAX_IL_BYTES,
)

log = logging.getLogger(__name__)

# Default block range in Feb 2025 where both sources have data
DEFAULT_START_BLOCK = 21750000
DEFAULT_END_BLOCK = 21750010


# ---------------------------------------------------------------------------
# Data fetching (source-specific)
# ---------------------------------------------------------------------------

def fetch_xatu_mempool(min_ts: int, max_ts: int, config: dict) -> pd.DataFrame:
    """Fetch mempool observations from Xatu sentries."""
    query = f"""
    SELECT
        hash as tx_hash,
        `from` as sender,
        nonce,
        toUnixTimestamp(event_date_time) as seen_timestamp,
        toUInt256(gas_fee_cap) as max_fee,
        toUInt256(gas_tip_cap) as priority_fee,
        size as tx_size,
        toUInt256(gas) as gas_limit,
        type as tx_type
    FROM mempool_transaction
    WHERE event_date_time >= toDateTime({int(min_ts)})
      AND event_date_time < toDateTime({int(max_ts)})
    ORDER BY event_date_time
    """
    df = execute_query(query, config)
    df['gas_limit'] = pd.to_numeric(df['gas_limit'], errors='coerce')
    return df


def fetch_bn_mempool(min_ts: int, max_ts: int, config: dict) -> pd.DataFrame:
    """Fetch mempool observations from BlockNative."""
    query = f"""
    SELECT
        hash as tx_hash,
        fromaddress as sender,
        nonce,
        toUnixTimestamp(detecttime) as seen_timestamp,
        toUInt256(COALESCE(maxfeepergas, gasprice)) as max_fee,
        toUInt256(COALESCE(maxpriorityfeepergas, gasprice)) as priority_fee,
        toUInt256(gasprice) as gas_price,
        type as tx_type,
        gas as gas_limit,
        datasize as call_data_size,
        status,
        region,
        stuck,
        timepending,
        blockspending,
        replace,
        dropreason
    FROM block_native_mempool_transaction
    WHERE detecttime >= toDateTime({int(min_ts)})
      AND detecttime < toDateTime({int(max_ts)})
      AND status = 'pending'
      AND network = 'main'
    ORDER BY detecttime
    """
    df = execute_query(query, config)
    df['gas_limit'] = pd.to_numeric(df['gas_limit'], errors='coerce')
    return df


# ---------------------------------------------------------------------------
# IL construction
# ---------------------------------------------------------------------------

def build_topfee_il(mempool_df: pd.DataFrame, base_fee: int,
                    already_included: set, max_bytes: int = MAX_IL_BYTES) -> pd.DataFrame:
    """Build a top-fee IL (type 2 only) from a mempool DataFrame.

    Expects the DataFrame to already be filtered to the correct time window.
    Must have columns: tx_hash, max_fee, priority_fee, tx_size, tx_type.
    """
    candidates = mempool_df[mempool_df['max_fee'] >= base_fee].copy()

    if 'tx_type' in candidates.columns:
        candidates = candidates[candidates['tx_type'] == 2]

    if len(candidates) == 0:
        return pd.DataFrame()

    candidates['effective_priority_fee'] = np.minimum(
        candidates['priority_fee'].astype(int),
        candidates['max_fee'].astype(int) - base_fee,
    )
    candidates = candidates.sort_values('effective_priority_fee', ascending=False)
    candidates = candidates.drop_duplicates(subset='tx_hash', keep='first')

    if already_included:
        candidates = candidates[~candidates['tx_hash'].isin(already_included)]

    if len(candidates) == 0:
        return pd.DataFrame()

    return pack_il(candidates, max_bytes)


# ---------------------------------------------------------------------------
# Per-block comparison
# ---------------------------------------------------------------------------

def compare_single_block(
    block_row: pd.Series,
    blocks_df: pd.DataFrame,
    xatu_df: pd.DataFrame,
    bn_df: pd.DataFrame,
    included_txs_map: dict,
    config: dict,
) -> dict:
    """Compare Xatu vs BlockNative for a single block."""
    block_num = int(block_row['block_number'])
    block_ts = int(block_row['block_timestamp'])
    base_fee = int(block_row['base_fee'])

    # Comparison uses topfee window (the IL comparison builds topfee ILs)
    win_lo = block_ts + config['analysis']['topfee_window_start_secs']
    win_hi = block_ts + config['analysis']['topfee_window_end_secs']

    result = {
        'block_number': block_num,
        'block_timestamp': block_ts,
        'base_fee': base_fee,
        'gas_used': int(block_row['gas_used']),
        'gas_limit': int(block_row['gas_limit']),
        'included_tx_count': int(block_row['included_tx_count']),
    }

    # --- Slice mempool windows ---
    xatu_win = xatu_df[
        (xatu_df['seen_timestamp'] >= win_lo) &
        (xatu_df['seen_timestamp'] <= win_hi)
    ].copy()
    bn_win = bn_df[
        (bn_df['seen_timestamp'] >= win_lo) &
        (bn_df['seen_timestamp'] <= win_hi)
    ].copy()

    # Deduplicate: keep earliest observation per tx
    xatu_dedup = xatu_win.sort_values('seen_timestamp').drop_duplicates(
        subset='tx_hash', keep='first')
    bn_dedup = bn_win.sort_values('seen_timestamp').drop_duplicates(
        subset='tx_hash', keep='first')

    xatu_hashes = set(xatu_dedup['tx_hash'])
    bn_hashes = set(bn_dedup['tx_hash'])

    # --- 1. Coverage metrics ---
    overlap = xatu_hashes & bn_hashes
    xatu_only = xatu_hashes - bn_hashes
    bn_only = bn_hashes - xatu_hashes
    union = xatu_hashes | bn_hashes

    result['xatu_tx_count'] = len(xatu_hashes)
    result['bn_tx_count'] = len(bn_hashes)
    result['overlap_count'] = len(overlap)
    result['xatu_only_count'] = len(xatu_only)
    result['bn_only_count'] = len(bn_only)
    result['jaccard_similarity'] = len(overlap) / len(union) if union else 0.0

    # Next-block coverage
    next_block_txs = included_txs_map.get(block_num + 1, set())
    result['next_block_tx_count'] = len(next_block_txs)

    if next_block_txs:
        result['xatu_next_block_coverage'] = len(xatu_hashes & next_block_txs) / len(next_block_txs) * 100
        result['bn_next_block_coverage'] = len(bn_hashes & next_block_txs) / len(next_block_txs) * 100
        result['union_next_block_coverage'] = len(union & next_block_txs) / len(next_block_txs) * 100
    else:
        result['xatu_next_block_coverage'] = None
        result['bn_next_block_coverage'] = None
        result['union_next_block_coverage'] = None

    # --- 2. Timing comparison (overlapping txs only) ---
    if overlap:
        xatu_times = xatu_dedup.set_index('tx_hash')['seen_timestamp']
        bn_times = bn_dedup.set_index('tx_hash')['seen_timestamp']
        overlap_list = list(overlap)
        deltas = xatu_times.loc[overlap_list] - bn_times.loc[overlap_list]
        # Positive delta = BN saw it first, negative = Xatu saw it first
        result['timing_mean_delta_s'] = deltas.mean()
        result['timing_median_delta_s'] = deltas.median()
        result['bn_saw_first_pct'] = (deltas > 0).sum() / len(deltas) * 100
        result['xatu_saw_first_pct'] = (deltas < 0).sum() / len(deltas) * 100
        result['same_second_pct'] = (deltas == 0).sum() / len(deltas) * 100
    else:
        result['timing_mean_delta_s'] = None
        result['timing_median_delta_s'] = None
        result['bn_saw_first_pct'] = None
        result['xatu_saw_first_pct'] = None
        result['same_second_pct'] = None

    # --- 3. BN metadata insights ---
    # Stuck tx rate
    if 'stuck' in bn_dedup.columns:
        result['bn_stuck_pct'] = bn_dedup['stuck'].sum() / len(bn_dedup) * 100 if len(bn_dedup) > 0 else 0
    else:
        result['bn_stuck_pct'] = None

    # Replace field usage (BN knows about replacements natively)
    if 'replace' in bn_dedup.columns:
        result['bn_replace_count'] = bn_dedup['replace'].notna().sum()
    else:
        result['bn_replace_count'] = None

    # Drop reason distribution
    if 'dropreason' in bn_dedup.columns:
        result['bn_drop_count'] = bn_dedup['dropreason'].notna().sum()
    else:
        result['bn_drop_count'] = None

    # Type distribution
    for src_name, src_df in [('xatu', xatu_dedup), ('bn', bn_dedup)]:
        if 'tx_type' in src_df.columns and len(src_df) > 0:
            result[f'{src_name}_type2_pct'] = (src_df['tx_type'] == 2).sum() / len(src_df) * 100
        else:
            result[f'{src_name}_type2_pct'] = None

    # --- 4. FOCIL IL comparison (0-delay top fee only) ---
    already_included = set()
    for bn, txs in included_txs_map.items():
        if bn <= block_num:
            already_included |= txs

    # BN doesn't have tx_size — use Xatu sizes for overlapping txs,
    # estimate for BN-only txs using call_data_size + 100 bytes overhead
    if 'tx_size' not in bn_dedup.columns:
        xatu_sizes = xatu_dedup.set_index('tx_hash')['tx_size']
        bn_il_df = bn_dedup.copy()
        bn_il_df['tx_size'] = bn_il_df['tx_hash'].map(xatu_sizes)
        # For BN-only txs, estimate size from call_data_size + overhead
        if 'call_data_size' in bn_il_df.columns:
            mask = bn_il_df['tx_size'].isna()
            bn_il_df.loc[mask, 'tx_size'] = bn_il_df.loc[mask, 'call_data_size'] + 100
        bn_il_df['tx_size'] = bn_il_df['tx_size'].fillna(200)  # fallback
    else:
        bn_il_df = bn_dedup.copy()

    xatu_il = build_topfee_il(xatu_dedup, base_fee, already_included)
    bn_il = build_topfee_il(bn_il_df, base_fee, already_included)

    result['xatu_il_tx_count'] = len(xatu_il)
    result['bn_il_tx_count'] = len(bn_il)
    result['xatu_il_size_bytes'] = int(xatu_il['tx_size'].sum()) if len(xatu_il) > 0 else 0
    result['bn_il_size_bytes'] = int(bn_il['tx_size'].sum()) if len(bn_il) > 0 else 0

    # IL overlap
    xatu_il_hashes = set(xatu_il['tx_hash']) if len(xatu_il) > 0 else set()
    bn_il_hashes = set(bn_il['tx_hash']) if len(bn_il) > 0 else set()
    il_union = xatu_il_hashes | bn_il_hashes
    result['il_overlap_count'] = len(xatu_il_hashes & bn_il_hashes)
    result['il_jaccard'] = (
        len(xatu_il_hashes & bn_il_hashes) / len(il_union) if il_union else 0.0
    )

    # Inclusion rates (0-delay: check N+1)
    next_included = included_txs_map.get(block_num + 1, set())
    if xatu_il_hashes and next_included:
        result['xatu_il_inclusion_rate'] = len(xatu_il_hashes & next_included) / len(xatu_il_hashes) * 100
    else:
        result['xatu_il_inclusion_rate'] = None

    if bn_il_hashes and next_included:
        result['bn_il_inclusion_rate'] = len(bn_il_hashes & next_included) / len(bn_il_hashes) * 100
    else:
        result['bn_il_inclusion_rate'] = None

    return result


# ---------------------------------------------------------------------------
# Range analysis
# ---------------------------------------------------------------------------

def analyze_block_range(start_block: int, end_block: int, config: dict) -> pd.DataFrame:
    """Run comparison across a block range."""
    log.info("Analyzing blocks %d to %d", start_block, end_block)

    # Fetch blocks with padding for next-block lookups
    blocks_df = fetch_block_data(start_block, end_block + 2, config)
    if len(blocks_df) == 0:
        log.warning("No blocks found")
        return None

    log.info("Got %d blocks", len(blocks_df))

    # Time range for mempool queries (topfee window for comparison)
    tf_start = config['analysis']['topfee_window_start_secs']
    tf_end = config['analysis']['topfee_window_end_secs']
    min_ts = blocks_df['block_timestamp'].min() + tf_start - 2
    max_ts = blocks_df['block_timestamp'].max() + tf_end + 2

    # Fetch both mempool sources
    log.info("Fetching Xatu mempool data...")
    xatu_df = fetch_xatu_mempool(min_ts, max_ts, config)
    log.info("Got %d Xatu mempool observations", len(xatu_df))

    log.info("Fetching BlockNative mempool data...")
    bn_df = fetch_bn_mempool(min_ts, max_ts, config)
    log.info("Got %d BlockNative mempool observations", len(bn_df))

    if len(xatu_df) == 0 or len(bn_df) == 0:
        log.error("One or both mempool sources returned no data. "
                  "Ensure the block range has coverage in both sources.")
        return None

    # Fetch included transactions
    log.info("Fetching included transactions...")
    included_txs_map = fetch_included_txs(start_block, end_block + 2, config)
    log.info("Got included txs for %d blocks", len(included_txs_map))

    # Process each block
    analysis_blocks = blocks_df[
        (blocks_df['block_number'] >= start_block) &
        (blocks_df['block_number'] < end_block)
    ]

    results = []
    for _, block_row in tqdm(analysis_blocks.iterrows(),
                              total=len(analysis_blocks), desc="Comparing"):
        row = compare_single_block(
            block_row, blocks_df, xatu_df, bn_df, included_txs_map, config)
        results.append(row)

    return pd.DataFrame(results) if results else None


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame):
    """Print comparison summary."""
    print("\n" + "=" * 70)
    print("MEMPOOL SOURCE COMPARISON: XATU vs BLOCKNATIVE")
    print("=" * 70)

    print(f"\nBlocks analyzed: {len(df):,}")
    print(f"Block range: {df['block_number'].min():,} - {df['block_number'].max():,}")

    # --- Coverage ---
    print("\n" + "-" * 70)
    print("COVERAGE")
    print("-" * 70)
    print(f"  Avg Xatu txs in window:       {df['xatu_tx_count'].mean():,.0f}")
    print(f"  Avg BlockNative txs in window: {df['bn_tx_count'].mean():,.0f}")
    print(f"  Avg overlap:                   {df['overlap_count'].mean():,.0f}")
    print(f"  Avg Xatu-only:                 {df['xatu_only_count'].mean():,.0f}")
    print(f"  Avg BN-only:                   {df['bn_only_count'].mean():,.0f}")
    print(f"  Avg Jaccard similarity:        {df['jaccard_similarity'].mean():.3f}")

    print("\n  Next-block coverage:")
    for col, label in [('xatu_next_block_coverage', 'Xatu'),
                        ('bn_next_block_coverage', 'BlockNative'),
                        ('union_next_block_coverage', 'Union (both)')]:
        vals = df[col].dropna()
        if len(vals) > 0:
            print(f"    {label:15s}: {vals.mean():.1f}%")

    # --- Timing ---
    print("\n" + "-" * 70)
    print("TIMING (for overlapping txs)")
    print("-" * 70)
    if df['timing_mean_delta_s'].notna().any():
        mean_delta = df['timing_mean_delta_s'].dropna().mean()
        direction = "BN earlier" if mean_delta > 0 else "Xatu earlier"
        print(f"  Avg mean delta:    {abs(mean_delta):.2f}s ({direction})")
        print(f"  Avg median delta:  {abs(df['timing_median_delta_s'].dropna().mean()):.2f}s")
        print(f"  BN saw first:      {df['bn_saw_first_pct'].dropna().mean():.1f}%")
        print(f"  Xatu saw first:    {df['xatu_saw_first_pct'].dropna().mean():.1f}%")
        print(f"  Same second:       {df['same_second_pct'].dropna().mean():.1f}%")

    # --- BN Metadata ---
    print("\n" + "-" * 70)
    print("BLOCKNATIVE METADATA")
    print("-" * 70)
    if df['bn_stuck_pct'].notna().any():
        print(f"  Avg stuck tx rate:   {df['bn_stuck_pct'].dropna().mean():.2f}%")
    if df['bn_replace_count'].notna().any():
        print(f"  Avg replace count:   {df['bn_replace_count'].dropna().mean():.1f} per block")
    if df['bn_drop_count'].notna().any():
        print(f"  Avg drop count:      {df['bn_drop_count'].dropna().mean():.1f} per block")

    # Type distribution
    print("\n  EIP-1559 (type 2) rate:")
    for col, label in [('xatu_type2_pct', 'Xatu'), ('bn_type2_pct', 'BlockNative')]:
        vals = df[col].dropna()
        if len(vals) > 0:
            print(f"    {label:15s}: {vals.mean():.1f}%")

    # --- FOCIL IL Comparison ---
    print("\n" + "-" * 70)
    print("FOCIL IL COMPARISON (0-delay top fee)")
    print("-" * 70)
    print(f"  Avg Xatu IL txs:    {df['xatu_il_tx_count'].mean():.1f}  "
          f"({df['xatu_il_size_bytes'].mean() / 1024:.2f} KiB)")
    print(f"  Avg BN IL txs:      {df['bn_il_tx_count'].mean():.1f}  "
          f"({df['bn_il_size_bytes'].mean() / 1024:.2f} KiB)")
    print(f"  Avg IL overlap:     {df['il_overlap_count'].mean():.1f} txs  "
          f"(Jaccard: {df['il_jaccard'].mean():.3f})")

    print("\n  Inclusion rates (0-delay, N+1 block):")
    for col, label in [('xatu_il_inclusion_rate', 'Xatu'),
                        ('bn_il_inclusion_rate', 'BlockNative')]:
        vals = df[col].dropna()
        if len(vals) > 0:
            print(f"    {label:15s}: {vals.mean():.1f}%")

    # Statistical notes
    print("\n" + "-" * 70)
    n = len(df)
    if n < 100:
        print(f"NOTE: Small sample ({n} blocks). Results are indicative only.")
    elif n < 1000:
        print(f"NOTE: Moderate sample ({n:,} blocks). Extend for publication-quality results.")
    else:
        print(f"Sample size: {n:,} blocks.")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S',
    )

    config = load_config()

    # Use config block range if available, else defaults for Feb 2025
    start_block = config['analysis'].get('comparison_start_block', DEFAULT_START_BLOCK)
    end_block = config['analysis'].get('comparison_end_block', DEFAULT_END_BLOCK)

    print("=" * 70)
    print("MEMPOOL SOURCE COMPARISON: XATU vs BLOCKNATIVE")
    print("=" * 70)
    print(f"Block range: {start_block:,} - {end_block:,} ({end_block - start_block} blocks)")
    print(f"Top Fee window: [{config['analysis']['topfee_window_start_secs']}, "
          f"{config['analysis']['topfee_window_end_secs']}] seconds")
    print(f"IL size cap: {MAX_IL_BYTES:,} bytes ({MAX_IL_BYTES / 1024:.1f} KiB)")

    batch_size = config['analysis'].get('batch_size_blocks', 100)
    all_results = []

    for batch_start in range(start_block, end_block, batch_size):
        batch_end = min(batch_start + batch_size, end_block)
        result = analyze_block_range(batch_start, batch_end, config)
        if result is not None:
            all_results.append(result)

    if not all_results:
        log.error("No results produced.")
        return

    df = pd.concat(all_results, ignore_index=True)

    # Save results
    results_dir = Path(__file__).parent.parent / "results"
    results_dir.mkdir(exist_ok=True)
    output_file = results_dir / "mempool_source_comparison.parquet"
    df.to_parquet(output_file, index=False)
    print(f"\nResults saved to: {output_file}")

    print_summary(df)


if __name__ == "__main__":
    main()

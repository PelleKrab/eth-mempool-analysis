#!/usr/bin/env python3
"""
FOCIL Analysis using BlockNative Mempool Data

Same 6-variant FOCIL inclusion list analysis as focil_censorship_analysis.py,
but powered by BlockNative's richer mempool data. Key advantages:

  - Full tx lifecycle tracking (pending → confirmed/evicted/cancelled)
  - Native replacement detection (cancel/speedup status + replace field)
  - timepending/blockspending metadata for inclusion timing
  - Cleaner data allows fewer heuristic filters

Differences from Xatu-based script:
  - All tx types allowed except type 3 (EIP-4844 blob txs)
  - No active sender filter needed (BN rejects spam natively)
  - Replacement detection via BN cancel/speedup status (not nonce heuristic)
  - Dual inclusion rate: canonical cross-validation + BN native
  - tx_size estimated from datasize + overhead (BN lacks full tx size)

Inclusion List Variants (3 delays × 2 strategies):
  Top Fee: L0, L-1, L-2 (highest priority fee transactions)
  Censored: L0, L-1, L-2 (transactions meeting censorship criteria)

Reference: EIP-7805 (FOCIL), 8 KiB inclusion list cap.
"""

import logging

import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from utils import (
    load_config, execute_query, fetch_block_data, fetch_included_txs,
    pack_il, get_block_int, MAX_IL_BYTES, BLOB_TX_TYPE, TX_OVERHEAD_BYTES,
    VARIANT_NAMES,
)

log = logging.getLogger(__name__)

# Status priority for resolving final status per tx
STATUS_PRIORITY = {
    'confirmed': 0,
    'failed': 1,
    'cancel': 2,
    'speedup': 3,
    'evicted': 4,
    'rejected': 5,
    'pending': 6,
}

DEFAULT_START_BLOCK = 21750000
DEFAULT_END_BLOCK = 21750010


# ---------------------------------------------------------------------------
# Data fetching (BN-specific)
# ---------------------------------------------------------------------------

def fetch_bn_mempool_all(min_ts: int, max_ts: int, config: dict) -> pd.DataFrame:
    """Fetch ALL BlockNative mempool events (all statuses) for the time window.

    Returns raw events — multiple rows per tx (one per status × region).
    Use build_tx_lifecycle() to aggregate into per-tx summaries.
    """
    query = f"""
    SELECT
        hash as tx_hash,
        fromaddress as sender,
        nonce,
        toUnixTimestamp(detecttime) as event_timestamp,
        toUInt256(COALESCE(maxfeepergas, gasprice)) as max_fee,
        toUInt256(COALESCE(maxpriorityfeepergas, gasprice)) as priority_fee,
        toUInt256(gasprice) as gas_price,
        type as tx_type,
        gas as gas_limit,
        datasize as call_data_size,
        status,
        stuck,
        timepending,
        blockspending,
        replace
    FROM block_native_mempool_transaction
    WHERE detecttime >= toDateTime({int(min_ts)})
      AND detecttime < toDateTime({int(max_ts)})
      AND network = 'main'
    ORDER BY detecttime
    """
    df = execute_query(query, config)
    df['gas_limit'] = pd.to_numeric(df['gas_limit'], errors='coerce')
    df['timepending'] = pd.to_numeric(df['timepending'], errors='coerce')
    df['blockspending'] = pd.to_numeric(df['blockspending'], errors='coerce')
    return df


# ---------------------------------------------------------------------------
# BN lifecycle processing
# ---------------------------------------------------------------------------

def build_tx_lifecycle(bn_raw_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate raw BN events into per-tx lifecycle summaries.

    For each unique tx_hash, produces:
      - first_seen: earliest event timestamp
      - sender, nonce, fee fields: from first pending event (or earliest event)
      - tx_type, gas_limit, call_data_size: from first event
      - final_status: highest-priority status observed
      - was_confirmed: True if any confirmed event exists
      - timepending_ms: from confirmed event (if available)
      - blocks_pending: from confirmed event (if available)
      - was_replaced: True if cancel/speedup status exists
      - was_stuck: True if any event had stuck=True
      - tx_size: estimated from call_data_size + overhead
    """
    if len(bn_raw_df) == 0:
        return pd.DataFrame()

    # Map status to priority for resolving final status
    bn_raw_df = bn_raw_df.copy()
    bn_raw_df['_status_priority'] = bn_raw_df['status'].map(STATUS_PRIORITY).fillna(99)

    # Group by tx_hash
    grouped = bn_raw_df.groupby('tx_hash')

    # First seen (earliest event)
    first_events = bn_raw_df.sort_values('event_timestamp').drop_duplicates(
        subset='tx_hash', keep='first')
    first_events = first_events.set_index('tx_hash')

    # Final status (highest priority)
    best_status = bn_raw_df.sort_values('_status_priority').drop_duplicates(
        subset='tx_hash', keep='first')
    best_status = best_status.set_index('tx_hash')[['status']].rename(
        columns={'status': 'final_status'})

    # Confirmed metadata (timepending, blockspending from confirmed events)
    confirmed = bn_raw_df[bn_raw_df['status'] == 'confirmed'].copy()
    confirmed_meta = confirmed.dropna(subset=['timepending']).drop_duplicates(
        subset='tx_hash', keep='first').set_index('tx_hash')[
        ['timepending', 'blockspending']
    ].rename(columns={'timepending': 'timepending_ms', 'blockspending': 'blocks_pending'})

    # Was confirmed (any confirmed event)
    was_confirmed = confirmed.drop_duplicates('tx_hash')[['tx_hash']].assign(
        was_confirmed=True).set_index('tx_hash')

    # Was replaced (cancel or speedup status)
    replaced = bn_raw_df[bn_raw_df['status'].isin(['cancel', 'speedup'])].drop_duplicates(
        'tx_hash')[['tx_hash']].assign(was_replaced=True).set_index('tx_hash')

    # Was stuck (any event with stuck=True)
    stuck = bn_raw_df[bn_raw_df['stuck'].astype(bool)].drop_duplicates(
        'tx_hash')[['tx_hash']].assign(was_stuck=True).set_index('tx_hash')

    # Build lifecycle dataframe
    lifecycle = first_events[['sender', 'nonce', 'event_timestamp', 'max_fee',
                               'priority_fee', 'gas_price', 'tx_type', 'gas_limit',
                               'call_data_size', 'stuck']].copy()
    lifecycle = lifecycle.rename(columns={'event_timestamp': 'first_seen',
                                          'stuck': '_first_stuck'})
    lifecycle = lifecycle.drop(columns=['_first_stuck'])

    # Join metadata
    lifecycle = lifecycle.join(best_status, how='left')
    lifecycle = lifecycle.join(confirmed_meta, how='left')
    lifecycle = lifecycle.join(was_confirmed, how='left')
    lifecycle = lifecycle.join(replaced, how='left')
    lifecycle = lifecycle.join(stuck, how='left')

    # Fill booleans
    lifecycle['was_confirmed'] = lifecycle['was_confirmed'].fillna(False)
    lifecycle['was_replaced'] = lifecycle['was_replaced'].fillna(False)
    lifecycle['was_stuck'] = lifecycle['was_stuck'].fillna(False)

    # Estimate tx_size
    lifecycle['tx_size'] = lifecycle['call_data_size'].fillna(0) + TX_OVERHEAD_BYTES

    lifecycle = lifecycle.reset_index()
    return lifecycle


# ---------------------------------------------------------------------------
# IL packing & construction
# ---------------------------------------------------------------------------

def build_topfee_il(pool_df: pd.DataFrame, base_fee: int,
                    already_included: set,
                    max_bytes: int = MAX_IL_BYTES) -> pd.DataFrame:
    """Build a top-fee IL from lifecycle pool.

    Filters: FOCIL-valid, not blob, not stuck, not already included.
    Allows types 0, 1, 2.
    """
    candidates = pool_df[
        (pool_df['max_fee'] >= base_fee) &
        (pool_df['tx_type'] != BLOB_TX_TYPE) &
        (~pool_df['was_stuck'])
    ].copy()

    if len(candidates) == 0:
        return pd.DataFrame()

    candidates['effective_priority_fee'] = np.minimum(
        candidates['priority_fee'].astype(float),
        candidates['max_fee'].astype(float) - base_fee,
    )
    candidates = candidates.sort_values('effective_priority_fee', ascending=False)
    candidates = candidates.drop_duplicates(subset='tx_hash', keep='first')

    if already_included:
        candidates = candidates[~candidates['tx_hash'].isin(already_included)]

    if len(candidates) == 0:
        return pd.DataFrame()

    return pack_il(candidates, max_bytes)


def flag_censored_transactions(
    lifecycle_df: pd.DataFrame,
    current_block_ts: int,
    current_base_fee: int,
    prev_block_gas_used: int,
    prev_block_gas_limit: int,
    curr_block_gas_used: int,
    curr_block_gas_limit: int,
    all_included_txs: set,
    config: dict,
) -> pd.DataFrame:
    """Flag transactions as censored using BN metadata.

    Criteria:
    1. FOCIL-valid (max_fee >= base_fee)
    2. Competitive effective priority fee (>= configured percentile)
    3. Not stuck
    4. Not a blob tx (type != 3)
    5. Sufficient dwell time (first_seen + dwell_threshold <= block_ts)
    6. Not replaced (BN cancel/speedup status)
    7. Not included in relevant blocks (canonical cross-validation)
    8. Gas fits in both prev and current block
    """
    if len(lifecycle_df) == 0:
        return pd.DataFrame()

    win_start = config['analysis']['censored_window_start_secs']
    win_end = config['analysis']['censored_window_end_secs']
    fee_pct = config['analysis'].get('censorship_fee_percentile', 0.50)
    min_dwell = config['analysis'].get('censorship_dwell_time_secs', 12)

    # Work with txs in the censored window
    pool = lifecycle_df[
        (lifecycle_df['first_seen'] >= current_block_ts + win_start) &
        (lifecycle_df['first_seen'] <= current_block_ts + win_end)
    ].copy()

    if len(pool) == 0:
        return pd.DataFrame()

    # Compute effective priority fee
    pool['effective_priority_fee'] = np.minimum(
        pool['priority_fee'].astype(float),
        pool['max_fee'].astype(float) - current_base_fee,
    )

    # Fee threshold from FOCIL-valid txs in the window
    focil_valid = pool[pool['max_fee'] >= current_base_fee]
    if len(focil_valid) == 0:
        return pd.DataFrame()
    fee_threshold = focil_valid['effective_priority_fee'].quantile(fee_pct)

    # Dwell time
    pool['dwell_time'] = current_block_ts - pool['first_seen']

    # Gas capacity
    prev_available = max(0, prev_block_gas_limit - prev_block_gas_used)
    curr_available = max(0, curr_block_gas_limit - curr_block_gas_used)

    # Apply all criteria
    censored = pool[
        (pool['max_fee'] >= current_base_fee) &                          # 1. FOCIL-valid
        (pool['effective_priority_fee'] >= fee_threshold) &              # 2. Competitive fee
        (~pool['was_stuck']) &                                           # 3. Not stuck
        (pool['tx_type'] != BLOB_TX_TYPE) &                             # 4. Not blob
        (pool['dwell_time'] >= min_dwell) &                              # 5. Sufficient dwell
        (~pool['was_replaced']) &                                        # 6. Not replaced
        (~pool['tx_hash'].isin(all_included_txs)) &                     # 7. Not included
        (pool['gas_limit'] <= prev_available) &                          # 8a. Fits prev block
        (pool['gas_limit'] <= curr_available)                            # 8b. Fits curr block
    ].copy()

    return censored


def construct_il_variant(
    lifecycle_df: pd.DataFrame,
    variant_type: str,
    delay: int,
    block_num: int,
    block_ts: int,
    base_fee: int,
    blocks_df: pd.DataFrame,
    censored_txs: pd.DataFrame,
    already_included: set,
    config: dict,
    max_bytes: int = MAX_IL_BYTES,
) -> pd.DataFrame:
    """Build one IL variant from BN lifecycle data."""
    # Each strategy has its own mempool window
    if variant_type == 'topfee':
        win_start = config['analysis']['topfee_window_start_secs']
        win_end = config['analysis']['topfee_window_end_secs']
    else:
        win_start = config['analysis']['censored_window_start_secs']
        win_end = config['analysis']['censored_window_end_secs']

    # IL is always built from block N's mempool window. The delay only affects
    # which block the IL is enforced at (and thus the inclusion rate check range).
    if variant_type == 'topfee':
        pool = lifecycle_df[
            (lifecycle_df['first_seen'] >= block_ts + win_start) &
            (lifecycle_df['first_seen'] <= block_ts + win_end)
        ].copy()

        return build_topfee_il(pool, base_fee, already_included, max_bytes)

    elif variant_type == 'censored':
        if censored_txs is None or len(censored_txs) == 0:
            return pd.DataFrame()
        # Revalidate against current base fee
        candidates = censored_txs[censored_txs['max_fee'] >= base_fee].copy()
        if len(candidates) == 0:
            return pd.DataFrame()

        candidates['effective_priority_fee'] = np.minimum(
            candidates['priority_fee'].astype(float),
            candidates['max_fee'].astype(float) - base_fee,
        )
        candidates = candidates.sort_values('effective_priority_fee', ascending=False)
        candidates = candidates.drop_duplicates(subset='tx_hash', keep='first')

        if already_included:
            candidates = candidates[~candidates['tx_hash'].isin(already_included)]

        if len(candidates) == 0:
            return pd.DataFrame()

        return pack_il(candidates, max_bytes)

    else:
        raise ValueError(f"Unknown variant_type: {variant_type}")


# ---------------------------------------------------------------------------
# Per-block processing
# ---------------------------------------------------------------------------

def process_single_block(
    block_row: pd.Series,
    blocks_df: pd.DataFrame,
    lifecycle_df: pd.DataFrame,
    included_txs_map: dict,
    config: dict,
    collect_metrics: bool = True,
) -> dict | None:
    """Process a single block: build 6 IL variants, compute metrics."""
    block_num = int(block_row['block_number'])
    block_ts = int(block_row['block_timestamp'])
    base_fee = int(block_row['base_fee'])

    if not collect_metrics:
        # Warm-up: just run through to establish state
        return None

    result = {
        'block_number': block_num,
        'block_timestamp': block_ts,
        'base_fee': base_fee,
        'gas_used': int(block_row['gas_used']),
        'gas_limit': int(block_row['gas_limit']),
        'included_tx_count': int(block_row['included_tx_count']),
    }

    # --- BN-specific metrics ---
    # Use union of both strategy windows for general mempool stats
    tf_start = config['analysis']['topfee_window_start_secs']
    tf_end = config['analysis']['topfee_window_end_secs']
    cs_start = config['analysis']['censored_window_start_secs']
    cs_end = config['analysis']['censored_window_end_secs']
    win_lo = block_ts + min(tf_start, cs_start)
    win_hi = block_ts + max(tf_end, cs_end)

    window_txs = lifecycle_df[
        (lifecycle_df['first_seen'] >= win_lo) &
        (lifecycle_df['first_seen'] <= win_hi)
    ]
    window_hashes = set(window_txs['tx_hash'])

    result['bn_pending_count'] = len(window_txs)
    result['bn_confirmed_in_window'] = int(window_txs['was_confirmed'].sum())
    result['bn_replaced_count'] = int(window_txs['was_replaced'].sum())
    result['bn_stuck_count'] = int(window_txs['was_stuck'].sum())

    # Timing metrics for confirmed txs
    confirmed_in_window = window_txs[window_txs['timepending_ms'].notna()]
    if len(confirmed_in_window) > 0:
        result['avg_timepending_ms'] = confirmed_in_window['timepending_ms'].mean()
        result['avg_blockspending'] = confirmed_in_window['blocks_pending'].mean()
    else:
        result['avg_timepending_ms'] = None
        result['avg_blockspending'] = None

    # Mempool coverage of next block
    next_block_txs = included_txs_map.get(block_num + 1, set())
    if next_block_txs:
        overlap = window_hashes & next_block_txs
        result['mempool_coverage_of_next_block'] = len(overlap) / len(next_block_txs) * 100
    else:
        result['mempool_coverage_of_next_block'] = 0.0
    result['mempool_unique_txs_in_window'] = len(window_hashes)

    # --- Build all 6 variants ---
    for delay in [0, 1, 2]:
        for variant_type in ['topfee', 'censored']:
            variant_name = f'{delay}delay_{variant_type}'

            # IL is always built at block N. Exclude txs already on-chain.
            already_included = set()
            for bn, txs in included_txs_map.items():
                if bn <= block_num:
                    already_included |= txs

            # For censored variants, flag censored transactions at block N.
            # The delay only affects enforcement (and thus inclusion rate check).
            censored_txs = None
            if variant_type == 'censored':
                prev_blk = blocks_df[blocks_df['block_number'] == block_num - 1]
                curr_blk = blocks_df[blocks_df['block_number'] == block_num]

                if len(prev_blk) > 0 and len(curr_blk) > 0:
                    censored_txs = flag_censored_transactions(
                        lifecycle_df=lifecycle_df,
                        current_block_ts=block_ts,
                        current_base_fee=base_fee,
                        prev_block_gas_used=get_block_int(prev_blk.iloc[0], 'gas_used'),
                        prev_block_gas_limit=get_block_int(prev_blk.iloc[0], 'gas_limit'),
                        curr_block_gas_used=int(block_row['gas_used']),
                        curr_block_gas_limit=int(block_row['gas_limit']),
                        all_included_txs=already_included,
                        config=config,
                    )

                    if delay == 0:
                        result['censored_detected_count'] = (
                            len(censored_txs) if censored_txs is not None else 0
                        )

            # Construct IL
            il_df = construct_il_variant(
                lifecycle_df=lifecycle_df,
                variant_type=variant_type,
                delay=delay,
                block_num=block_num,
                block_ts=block_ts,
                base_fee=base_fee,
                blocks_df=blocks_df,
                censored_txs=censored_txs,
                already_included=already_included,
                config=config,
            )

            il_size = int(il_df['tx_size'].sum()) if len(il_df) > 0 else 0
            result[f'{variant_name}_tx_count'] = len(il_df)
            result[f'{variant_name}_size_bytes'] = il_size

            # Inclusion rate and redundant bandwidth.
            # IL is built at block N. With delay D, enforced at N+1+D.
            # Check blocks N+1 through N+1+D for natural inclusion.
            if len(il_df) > 0:
                included_in_range = set()
                for bn in range(block_num + 1, block_num + 2 + delay):
                    included_in_range |= included_txs_map.get(bn, set())
                il_hashes = set(il_df['tx_hash'])
                redundant = il_hashes & included_in_range
                if included_in_range:
                    rate = len(redundant) / len(il_hashes) * 100
                    result[f'{variant_name}_inclusion_rate'] = rate
                else:
                    result[f'{variant_name}_inclusion_rate'] = None

                # Redundant bandwidth: IL bytes wasted on already-included txs
                redundant_size = int(il_df[il_df['tx_hash'].isin(redundant)]['tx_size'].sum())
                result[f'{variant_name}_redundant_bytes'] = redundant_size
                result[f'{variant_name}_useful_bytes'] = il_size - redundant_size
            else:
                result[f'{variant_name}_inclusion_rate'] = None
                result[f'{variant_name}_redundant_bytes'] = 0
                result[f'{variant_name}_useful_bytes'] = 0

    return result


# ---------------------------------------------------------------------------
# Range analysis
# ---------------------------------------------------------------------------

def analyze_block_range(start_block: int, end_block: int, config: dict) -> pd.DataFrame:
    """Run FOCIL analysis across a block range using BN data."""
    log.info("Analyzing blocks %d to %d", start_block, end_block)

    # Padding: max dwell time (default 120s) / 12s per slot ≈ 10 blocks.
    # Need enough lookback so already_included covers all txs that could
    # appear in the mempool window of the first analysis block.
    max_dwell = config['analysis'].get('censorship_max_dwell_time_secs', 120)
    lookback_blocks = max(3, max_dwell // 12 + 1)

    # Fetch blocks with padding for warm-up and forward lookback
    blocks_df = fetch_block_data(start_block - lookback_blocks, end_block + 3, config)
    if len(blocks_df) == 0:
        log.warning("No blocks found for range %d-%d", start_block, end_block)
        return None

    log.info("Got %d blocks (including warm-up and lookback)", len(blocks_df))

    # Time range for mempool query (cover both strategy windows with padding)
    tf_start = config['analysis']['topfee_window_start_secs']
    tf_end = config['analysis']['topfee_window_end_secs']
    cs_start = config['analysis']['censored_window_start_secs']
    cs_end = config['analysis']['censored_window_end_secs']
    min_ts = blocks_df['block_timestamp'].min() + min(tf_start, cs_start) - 2
    max_ts = blocks_df['block_timestamp'].max() + max(tf_end, cs_end) + 2

    # Fetch BN mempool (all statuses)
    log.info("Fetching BlockNative mempool data (all statuses)...")
    bn_raw = fetch_bn_mempool_all(min_ts, max_ts, config)
    log.info("Got %d raw BN events", len(bn_raw))

    if len(bn_raw) == 0:
        log.error("No BlockNative data for this range. "
                  "Ensure block range has BN coverage (Feb-Mar 2025).")
        return None

    # Build lifecycle
    log.info("Building tx lifecycle summaries...")
    lifecycle_df = build_tx_lifecycle(bn_raw)
    log.info("Got %d unique transactions", len(lifecycle_df))

    # Fetch included transactions for cross-validation
    log.info("Fetching included transactions (canonical)...")
    included_txs_map = fetch_included_txs(start_block - lookback_blocks, end_block + 3, config)
    log.info("Got included txs for %d blocks", len(included_txs_map))

    # Warm-up phase (3 blocks)
    log.info("Warm-up phase (3 blocks)...")
    warmup = blocks_df[
        (blocks_df['block_number'] >= start_block - min(3, lookback_blocks)) &
        (blocks_df['block_number'] < start_block)
    ]
    for _, block_row in warmup.iterrows():
        process_single_block(block_row, blocks_df, lifecycle_df,
                             included_txs_map, config, collect_metrics=False)

    # Main analysis
    analysis_blocks = blocks_df[
        (blocks_df['block_number'] >= start_block) &
        (blocks_df['block_number'] < end_block)
    ]

    results = []
    for _, block_row in tqdm(analysis_blocks.iterrows(),
                              total=len(analysis_blocks), desc="Processing"):
        row = process_single_block(block_row, blocks_df, lifecycle_df,
                                    included_txs_map, config, collect_metrics=True)
        if row is not None:
            results.append(row)

    return pd.DataFrame(results) if results else None


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame):
    """Print analysis summary."""
    blocks_per_year = 7200 * 365

    print("\n" + "=" * 70)
    print("BN-FOCIL ANALYSIS SUMMARY")
    print("=" * 70)

    print(f"\nBlocks analyzed: {len(df):,}")
    print(f"Block range: {df['block_number'].min():,} - {df['block_number'].max():,}")

    avg_gas_pct = df['gas_used'].mean() / df['gas_limit'].mean() * 100
    print(f"Average gas usage: {df['gas_used'].mean() / 1e6:.2f}M ({avg_gas_pct:.1f}% of limit)")

    if 'mempool_coverage_of_next_block' in df.columns:
        print(f"Avg mempool coverage of next block: "
              f"{df['mempool_coverage_of_next_block'].mean():.1f}%")

    # --- BN Metadata ---
    print("\n" + "-" * 70)
    print("BLOCKNATIVE MEMPOOL METRICS")
    print("-" * 70)
    print(f"  Avg pending txs in window:   {df['bn_pending_count'].mean():,.0f}")
    print(f"  Avg confirmed in window:     {df['bn_confirmed_in_window'].mean():,.0f}")
    print(f"  Avg replaced (cancel/speed): {df['bn_replaced_count'].mean():.1f}")
    print(f"  Avg stuck txs:               {df['bn_stuck_count'].mean():.1f}")

    tp = df['avg_timepending_ms'].dropna()
    if len(tp) > 0:
        print(f"  Avg time-to-inclusion:       {tp.mean() / 1000:.1f}s")
    bp = df['avg_blockspending'].dropna()
    if len(bp) > 0:
        print(f"  Avg blocks-to-inclusion:     {bp.mean():.2f}")

    # --- Bandwidth Matrix ---
    print("\n" + "=" * 70)
    print("BANDWIDTH MATRIX")
    print("=" * 70)

    for strategy in ['topfee', 'censored']:
        label = "Top Fee" if strategy == 'topfee' else "Censored"
        print(f"\n## {label} Strategy")
        for delay in [0, 1, 2]:
            col_size = f'{delay}delay_{strategy}_size_bytes'
            col_count = f'{delay}delay_{strategy}_tx_count'
            col_rate = f'{delay}delay_{strategy}_inclusion_rate'
            col_useful = f'{delay}delay_{strategy}_useful_bytes'
            col_redundant = f'{delay}delay_{strategy}_redundant_bytes'

            avg_kb = df[col_size].mean() / 1024
            avg_count = df[col_count].mean()
            annual_gb = df[col_size].mean() * blocks_per_year / (1024 ** 3)

            useful_kb = df[col_useful].mean() / 1024 if col_useful in df.columns else 0
            redundant_kb = df[col_redundant].mean() / 1024 if col_redundant in df.columns else 0

            line = (f"  {delay}-delay: {avg_kb:.2f} KiB/block "
                    f"(useful={useful_kb:.2f}, redundant={redundant_kb:.2f}), "
                    f"{avg_count:.1f} txs, {annual_gb:.2f} GB/year")

            if col_rate in df.columns and df[col_rate].notna().any():
                line += f", inclusion={df[col_rate].dropna().mean():.1f}%"
            print(line)

    # --- Delay Effect on Useful Bandwidth ---
    print("\n" + "=" * 70)
    print("DELAY EFFECT ON USEFUL BANDWIDTH")
    print("=" * 70)

    for strategy in ['topfee', 'censored']:
        label = "Top Fee" if strategy == 'topfee' else "Censored"
        col_useful_0 = f'0delay_{strategy}_useful_bytes'
        if col_useful_0 not in df.columns:
            continue
        base = df[col_useful_0].mean()
        if base == 0:
            continue
        print(f"\n## {label} Strategy")
        for delay in [0, 1, 2]:
            col_useful = f'{delay}delay_{strategy}_useful_bytes'
            col_redundant = f'{delay}delay_{strategy}_redundant_bytes'
            useful = df[col_useful].mean()
            redundant = df[col_redundant].mean()
            useful_annual = useful * blocks_per_year / (1024 ** 3)
            redundant_annual = redundant * blocks_per_year / (1024 ** 3)
            pct = (useful / base - 1) * 100 if delay > 0 else 0
            suffix = f" ({pct:+.1f}%)" if delay > 0 else " (baseline)"
            print(f"  {delay}-delay: {useful_annual:.2f} GB/year useful, "
                  f"{redundant_annual:.2f} GB/year redundant{suffix}")

    # --- Censorship ---
    if 'censored_detected_count' in df.columns:
        print("\n" + "=" * 70)
        print("CENSORSHIP DETECTION")
        print("=" * 70)
        avg = df['censored_detected_count'].mean()
        blocks_with = (df['censored_detected_count'] > 0).sum()
        print(f"  Average censored txs/block: {avg:.2f}")
        print(f"  Blocks with censorship: {blocks_with:,} "
              f"({blocks_with / len(df) * 100:.1f}%)")

    # --- Notes ---
    print("\n" + "=" * 70)
    print("NOTES")
    print("=" * 70)
    n = len(df)
    print(f"  Data source: BlockNative (block_native_mempool_transaction)")
    print(f"  TX types included: 0 (legacy), 1 (EIP-2930), 2 (EIP-1559)")
    print(f"  TX types excluded: 3 (EIP-4844 blob)")
    print(f"  TX size: estimated (datasize + {TX_OVERHEAD_BYTES}B overhead)")
    if n < 1000:
        print(f"  WARNING: Small sample ({n} blocks). "
              f"Recommend >= 1,000 blocks (ideally 50,000+).")
    elif n < 10000:
        print(f"  Moderate sample ({n:,} blocks). "
              f"Extend to 50,000 for publication.")
    else:
        print(f"  Sample size: {n:,} blocks.")
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

    start_block = config['analysis'].get('bn_start_block', DEFAULT_START_BLOCK)
    end_block = config['analysis'].get('bn_end_block', DEFAULT_END_BLOCK)

    print("=" * 70)
    print("FOCIL ANALYSIS (BLOCKNATIVE DATA)")
    print("=" * 70)
    print(f"Block range: {start_block:,} - {end_block:,} "
          f"({end_block - start_block} blocks)")
    print(f"Top Fee window: [{config['analysis']['topfee_window_start_secs']}, "
          f"{config['analysis']['topfee_window_end_secs']}] seconds")
    print(f"Censored window: [{config['analysis']['censored_window_start_secs']}, "
          f"{config['analysis']['censored_window_end_secs']}] seconds")
    print(f"IL size cap: {MAX_IL_BYTES:,} bytes ({MAX_IL_BYTES / 1024:.1f} KiB)")
    print(f"TX types: 0, 1, 2 (excluding blob type 3)")

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
    output_file = results_dir / "bn_focil_analysis.parquet"
    df.to_parquet(output_file, index=False)
    print(f"\nResults saved to: {output_file}")

    print_summary(df)


if __name__ == "__main__":
    main()

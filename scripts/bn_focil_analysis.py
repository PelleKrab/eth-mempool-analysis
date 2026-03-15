#!/usr/bin/env python3
"""
FOCIL Analysis using BlockNative Mempool Data

Same FOCIL inclusion list analysis as focil_censorship_analysis.py,
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

Structure:
  2 ILs built per slot (Top Fee + Censored).
  Redundancy evaluated at 3 delay levels (0, 1, 2) per IL.

Reference: EIP-7805 (FOCIL), 8 KiB inclusion list cap.
"""

import argparse
import logging
import sys

import numpy as np
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

from utils import (
    load_config, execute_query, fetch_block_data, fetch_included_txs,
    pack_il, get_block_int, MAX_IL_BYTES, BLOB_TX_TYPE, TX_OVERHEAD_BYTES,
    IL_STRATEGIES, DELAY_LEVELS, VARIANT_NAMES,
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
        timepending,
        blockspending,
        replace
    FROM block_native_mempool_transaction
    WHERE detecttime >= toDateTime({int(min_ts)})
      AND detecttime < toDateTime({int(max_ts)})
      AND network = 'main'
      AND (maxfeepergas IS NOT NULL OR gasprice IS NOT NULL)
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

    # Build lifecycle dataframe
    lifecycle = first_events[['sender', 'nonce', 'event_timestamp', 'max_fee',
                               'priority_fee', 'gas_price', 'tx_type', 'gas_limit',
                               'call_data_size']].copy()
    lifecycle = lifecycle.rename(columns={'event_timestamp': 'first_seen'})

    # Join metadata
    lifecycle = lifecycle.join(best_status, how='left')
    lifecycle = lifecycle.join(confirmed_meta, how='left')
    lifecycle = lifecycle.join(was_confirmed, how='left')
    lifecycle = lifecycle.join(replaced, how='left')

    # Fill booleans
    lifecycle['was_confirmed'] = lifecycle['was_confirmed'].fillna(False)
    lifecycle['was_replaced'] = lifecycle['was_replaced'].fillna(False)

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

    Filters: FOCIL-valid, not blob, not already included.
    Allows types 0, 1, 2.
    """
    candidates = pool_df[
        (pool_df['max_fee'] >= base_fee) &
        (pool_df['tx_type'] != BLOB_TX_TYPE)
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
    3. Not a blob tx (type != 3)
    4. Sufficient dwell time (first_seen + dwell_threshold <= block_ts)
    5. Not replaced (BN cancel/speedup status)
    6. Not included in relevant blocks (canonical cross-validation)
    7. Gas fits in both prev and current block
    """
    if len(lifecycle_df) == 0:
        return pd.DataFrame()

    win_start = config['analysis']['censored_window_start_secs']
    win_end = config['analysis']['censored_window_end_secs']
    fee_pct = config['analysis'].get('censorship_fee_percentile', 0.50)
    min_dwell = config['analysis'].get('censorship_dwell_time_secs', 12)
    max_dwell = config['analysis'].get('censorship_max_dwell_time_secs', 12)

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
        (pool['tx_type'] != BLOB_TX_TYPE) &                             # 3. Not blob
        (pool['dwell_time'] >= min_dwell) &                              # 4a. Sufficient dwell
        (pool['dwell_time'] <= max_dwell) &                              # 4b. Capped at 1 slot
        (~pool['was_replaced']) &                                        # 5. Not replaced
        (~pool['tx_hash'].isin(all_included_txs)) &                     # 6. Not included
        (pool['gas_limit'] <= prev_available) &                          # 7a. Fits prev block
        (pool['gas_limit'] <= curr_available)                            # 7b. Fits curr block
    ].copy()

    return censored


def construct_il(
    lifecycle_df: pd.DataFrame,
    strategy: str,
    block_ts: int,
    base_fee: int,
    censored_txs: pd.DataFrame,
    already_included: set,
    config: dict,
    max_bytes: int = MAX_IL_BYTES,
) -> pd.DataFrame:
    """Build one IL from BN lifecycle data for the given strategy.

    Delay is NOT relevant here — it only affects which blocks we check
    for redundancy after the IL is built.
    """
    if strategy == 'topfee':
        win_start = config['analysis']['topfee_window_start_secs']
        win_end = config['analysis']['topfee_window_end_secs']
        pool = lifecycle_df[
            (lifecycle_df['first_seen'] >= block_ts + win_start) &
            (lifecycle_df['first_seen'] <= block_ts + win_end)
        ].copy()
        return build_topfee_il(pool, base_fee, already_included, max_bytes)

    elif strategy == 'censored':
        if censored_txs is None or len(censored_txs) == 0:
            return pd.DataFrame()
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
        raise ValueError(f"Unknown strategy: {strategy}")


# ---------------------------------------------------------------------------
# Per-block processing
# ---------------------------------------------------------------------------

def _ts_to_block_num(ts: float, blocks_sorted: pd.DataFrame) -> int:
    """Return block_number of the last block produced at or before timestamp ts."""
    idx = int(blocks_sorted['block_timestamp'].searchsorted(ts, side='right')) - 1
    if idx < 0:
        log.debug("_ts_to_block_num: timestamp %.0f precedes all known blocks, clamping to first", ts)
        idx = 0
    return int(blocks_sorted['block_number'].iloc[idx])


def _build_tx_latency_records(
    il_df: pd.DataFrame,
    il_hashes: set,
    next_block_included: set,
    lc_lookup: dict,
    blocks_sorted: pd.DataFrame,
    block_num: int,
    strategy: str,
) -> list:
    """Build per-TX latency records for one IL.

    One record per IL tx.  Uses the 0-delay definition of redundancy (tx in N+1
    = redundant).  For non-redundant confirmed txs, computes delay_from_il via
    blocks_pending + first_seen timestamp → block mapping.

    iterrows() is intentional: ~50 txs/IL is fast; batch sizes are capped at
    BN_BATCH_SIZE (100 blocks) so the total iteration count stays bounded.
    """
    records = []
    redundant_0delay = il_hashes & next_block_included
    for _, tx_row in il_df.iterrows():
        tx_hash = tx_row['tx_hash']
        is_redundant = tx_hash in redundant_0delay
        lc = lc_lookup.get(tx_hash, {})

        # Delay from this IL block to BN-confirmed inclusion block.
        # blocks_pending = total blocks from first_seen to confirmation.
        # first_seen_block = block produced at/before first_seen timestamp.
        # => included_block = first_seen_block + blocks_pending
        # => delay_from_il = included_block - block_num
        delay_from_il = None
        if not is_redundant and lc.get('was_confirmed') and pd.notna(lc.get('blocks_pending')):
            first_seen_block = _ts_to_block_num(lc['first_seen'], blocks_sorted)
            included_block = first_seen_block + int(lc['blocks_pending'])
            delay_from_il = included_block - block_num

        records.append({
            'block_number':           block_num,
            'strategy':               strategy,
            'tx_hash':                tx_hash,
            'is_redundant':           is_redundant,
            'was_confirmed':          lc.get('was_confirmed', None),
            'was_replaced':           lc.get('was_replaced', None),
            'final_status':           lc.get('final_status', None),
            'blocks_pending':         lc.get('blocks_pending', None),
            'delay_from_il':          delay_from_il,
            'tx_size':                int(tx_row.get('tx_size', 0) or 0),
            'effective_priority_fee': float(tx_row.get('effective_priority_fee', 0) or 0),
        })
    return records


def process_single_block(
    block_row: pd.Series,
    blocks_df: pd.DataFrame,
    lifecycle_df: pd.DataFrame,
    included_txs_map: dict,
    config: dict,
    collect_metrics: bool = True,
    lc_lookup: dict | None = None,
    blocks_sorted: pd.DataFrame | None = None,
) -> tuple[dict | None, list]:
    """Process a single block: build 2 ILs, evaluate redundancy at 3 delays.

    Returns (per_block_result, tx_latency_records).  tx_latency_records contains
    one entry per IL tx (both strategies), recording whether it was redundant
    (already in N+1) and — for non-redundant txs — how long until BN confirmed it.
    """
    block_num = int(block_row['block_number'])
    block_ts = int(block_row['block_timestamp'])
    base_fee = int(block_row['base_fee'])

    tx_records: list = []

    if not collect_metrics:
        return None, tx_records

    result = {
        'block_number': block_num,
        'block_timestamp': block_ts,
        'base_fee': base_fee,
        'gas_used': int(block_row['gas_used']),
        'gas_limit': int(block_row['gas_limit']),
        'included_tx_count': int(block_row['included_tx_count']),
    }

    # --- BN-specific metrics ---
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

    # Exclude txs already on-chain at block N
    already_included = set()
    for bn, txs in included_txs_map.items():
        if bn <= block_num:
            already_included |= txs

    # Flag censored transactions once (shared across all delay evaluations)
    censored_txs = None
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
        result['censored_detected_count'] = (
            len(censored_txs) if censored_txs is not None else 0
        )

    # Build 2 ILs (one per strategy)
    il_map = {}
    for strategy in IL_STRATEGIES:
        il_map[strategy] = construct_il(
            lifecycle_df=lifecycle_df,
            strategy=strategy,
            block_ts=block_ts,
            base_fee=base_fee,
            censored_txs=censored_txs,
            already_included=already_included,
            config=config,
        )

    next_block_included = included_txs_map.get(block_num + 1, set())

    # Evaluate redundancy at each delay level
    for strategy in IL_STRATEGIES:
        il_df = il_map[strategy]
        il_size = int(il_df['tx_size'].sum()) if len(il_df) > 0 else 0
        il_hashes = set(il_df['tx_hash']) if len(il_df) > 0 else set()

        for delay in DELAY_LEVELS:
            variant_name = f'{delay}delay_{strategy}'

            # TX count and size are the same for all delays (same IL)
            result[f'{variant_name}_tx_count'] = len(il_df)
            result[f'{variant_name}_size_bytes'] = il_size

            # Inclusion rate: check blocks N+1 through N+1+D
            if il_hashes:
                included_in_range = set()
                for bn in range(block_num + 1, block_num + 2 + delay):
                    included_in_range |= included_txs_map.get(bn, set())
                redundant = il_hashes & included_in_range
                if included_in_range:
                    rate = len(redundant) / len(il_hashes) * 100
                    result[f'{variant_name}_inclusion_rate'] = rate
                else:
                    result[f'{variant_name}_inclusion_rate'] = None

                redundant_size = int(il_df[il_df['tx_hash'].isin(redundant)]['tx_size'].sum())
                result[f'{variant_name}_redundant_bytes'] = redundant_size
                result[f'{variant_name}_useful_bytes'] = il_size - redundant_size
            else:
                result[f'{variant_name}_inclusion_rate'] = None
                result[f'{variant_name}_redundant_bytes'] = 0
                result[f'{variant_name}_useful_bytes'] = 0

        # Per-TX latency records (one entry per tx, not per delay level).
        if lc_lookup is not None and blocks_sorted is not None and len(il_df) > 0:
            tx_records.extend(_build_tx_latency_records(
                il_df, il_hashes, next_block_included, lc_lookup,
                blocks_sorted, block_num, strategy,
            ))

    return result, tx_records


# ---------------------------------------------------------------------------
# Range analysis
# ---------------------------------------------------------------------------

def analyze_block_range(start_block: int, end_block: int, config: dict) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """FOCIL analysis: 2 ILs per slot, redundancy at 3 delay levels (BN data).

    Returns (per_block_df, tx_latency_df).  tx_latency_df has one row per IL tx
    with redundancy flag and, for non-redundant txs, BN-derived delay to inclusion.
    """
    log.info("Analyzing blocks %d to %d", start_block, end_block)

    # Lookback: 3 blocks for warm-up + already_included context.
    lookback_blocks = 3

    # Fetch blocks with padding for warm-up and forward lookback
    blocks_df = fetch_block_data(start_block - lookback_blocks, end_block + 3, config)
    if len(blocks_df) == 0:
        log.warning("No blocks found for range %d-%d", start_block, end_block)
        return None, None

    # canonical_beacon_block has one row per beacon node — keep one per block.
    # Sort by block_timestamp first so the surviving row is deterministic
    # (earliest timestamp wins; arbitrary otherwise due to query row order).
    blocks_df['base_fee'] = pd.to_numeric(blocks_df['base_fee'], errors='coerce')
    blocks_df = blocks_df.sort_values('block_timestamp').drop_duplicates(subset='block_number', keep='first')
    log.info("Got %d unique blocks (including warm-up and lookback)", len(blocks_df))

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
        log.error("No BlockNative data for this range.")
        return None, None

    # Build lifecycle
    log.info("Building tx lifecycle summaries...")
    lifecycle_df = build_tx_lifecycle(bn_raw)
    log.info("Got %d unique transactions", len(lifecycle_df))

    # Fast lookup: tx_hash -> lifecycle row dict (for per-TX latency).
    # Memory: ~1-2 KB per tx * ~10k txs/batch = tens of MB, fine at BN_BATCH_SIZE=100.
    lc_lookup = lifecycle_df.set_index('tx_hash').to_dict('index')

    # Sorted blocks array for O(log n) timestamp → block_number mapping
    blocks_sorted = (blocks_df[['block_number', 'block_timestamp']]
                     .sort_values('block_timestamp')
                     .reset_index(drop=True))

    # Fetch included transactions for cross-validation
    log.info("Fetching included transactions (canonical)...")
    included_txs_map = fetch_included_txs(start_block - lookback_blocks, end_block + 3, config)
    log.info("Got included txs for %d blocks", len(included_txs_map))

    # Main analysis — skip blocks with missing base_fee (NULL cast to 0 in CH)
    zero_bf = (blocks_df['base_fee'].isna() | (blocks_df['base_fee'] == 0))
    if zero_bf.any():
        log.warning("Dropping %d blocks with base_fee=0 (NULL in source)", zero_bf.sum())
        blocks_df = blocks_df[~zero_bf]

    analysis_blocks = blocks_df[
        (blocks_df['block_number'] >= start_block) &
        (blocks_df['block_number'] < end_block)
    ]

    results = []
    all_tx_records = []
    for _, block_row in tqdm(analysis_blocks.iterrows(),
                              total=len(analysis_blocks), desc="Processing"):
        row, tx_recs = process_single_block(
            block_row, blocks_df, lifecycle_df, included_txs_map, config,
            collect_metrics=True, lc_lookup=lc_lookup, blocks_sorted=blocks_sorted,
        )
        if row is not None:
            results.append(row)
        all_tx_records.extend(tx_recs)

    result_df = pd.DataFrame(results) if results else None
    tx_df = pd.DataFrame(all_tx_records) if all_tx_records else None
    return result_df, tx_df


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame):
    """Print analysis summary."""
    blocks_per_year = 7200 * 365

    print("\n" + "=" * 70)
    print("BN-FOCIL ANALYSIS (2 ILs x 3 delay evaluations)")
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

BN_BATCH_SIZE = 100  # blocks per ClickHouse query (~20min of data, safe limit)


def run_chunk(chunk_start: int, chunk_end: int, chunk_id: int, output_dir: Path):
    """Run BN analysis on a single chunk and save results.

    Internally processes in sub-batches of BN_BATCH_SIZE blocks so that
    each ClickHouse query stays within timeout limits, regardless of the
    outer chunk size.
    """
    try:
        log.info(f"[Chunk {chunk_id}] Processing blocks {chunk_start:,} to {chunk_end:,}")

        config = load_config()
        batch_results = []
        batch_tx_records = []

        for batch_start in range(chunk_start, chunk_end, BN_BATCH_SIZE):
            batch_end = min(batch_start + BN_BATCH_SIZE, chunk_end)
            result_df, tx_df = analyze_block_range(batch_start, batch_end, config)
            if result_df is not None and len(result_df) > 0:
                batch_results.append(result_df)
            if tx_df is not None and len(tx_df) > 0:
                batch_tx_records.append(tx_df)

        if not batch_results:
            log.warning(f"[Chunk {chunk_id}] No results produced")
            return None

        result_df = pd.concat(batch_results, ignore_index=True)
        output_file = output_dir / f"bn_chunk_{chunk_id:04d}_{chunk_start}_{chunk_end}.parquet"
        result_df.to_parquet(output_file, index=False)
        log.info(f"[Chunk {chunk_id}] Saved {len(result_df):,} blocks to {output_file.name}")

        if batch_tx_records:
            tx_df = pd.concat(batch_tx_records, ignore_index=True)
            latency_file = output_dir / f"bn_chunk_{chunk_id:04d}_{chunk_start}_{chunk_end}_latency.parquet"
            tx_df.to_parquet(latency_file, index=False)
            log.info(f"[Chunk {chunk_id}] Saved {len(tx_df):,} TX records to {latency_file.name}")

        return output_file

    except Exception as e:
        log.error(f"[Chunk {chunk_id}] Failed: {e}", exc_info=True)
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Batch process BN-FOCIL analysis over large block ranges")
    parser.add_argument("start_block", type=int, help="Starting block number")
    parser.add_argument("end_block", type=int, help="Ending block number")
    parser.add_argument("--chunk-size", type=int, default=1000,
                       help="Blocks per output chunk (default: 1000); "
                            f"queries are internally sub-batched at {BN_BATCH_SIZE} blocks")
    parser.add_argument("--parallel", type=int, default=1,
                       help="Number of parallel workers (default: 1 = sequential)")
    parser.add_argument("--output-dir", type=str, default="results/bn_chunks",
                       help="Directory for chunk outputs")
    parser.add_argument("--resume", action="store_true",
                       help="Skip chunks that already have output files")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    if args.end_block <= args.start_block:
        log.error("end_block must be greater than start_block")
        return 1

    total_blocks = args.end_block - args.start_block
    num_chunks = (total_blocks + args.chunk_size - 1) // args.chunk_size

    log.info("=" * 70)
    log.info("BN-FOCIL BATCH RUNNER")
    log.info("=" * 70)
    log.info(f"Block range: {args.start_block:,} to {args.end_block:,}")
    log.info(f"Total blocks: {total_blocks:,}")
    log.info(f"Chunk size: {args.chunk_size:,}")
    log.info(f"Number of chunks: {num_chunks:,}")
    log.info(f"Parallel workers: {args.parallel}")
    log.info(f"Output directory: {args.output_dir}")

    output_dir = Path(__file__).parent.parent / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    chunks = []
    for chunk_id, chunk_start in enumerate(range(args.start_block, args.end_block, args.chunk_size)):
        chunk_end = min(chunk_start + args.chunk_size, args.end_block)

        output_file = output_dir / f"bn_chunk_{chunk_id:04d}_{chunk_start}_{chunk_end}.parquet"
        if args.resume and output_file.exists():
            log.info(f"[Chunk {chunk_id}] Skipping (already exists): {output_file.name}")
            continue

        chunks.append((chunk_start, chunk_end, chunk_id, output_dir))

    if not chunks:
        log.info("All chunks already processed!")
        return 0

    log.info(f"Processing {len(chunks):,} chunks...")

    completed = 0
    failed = 0

    if args.parallel == 1:
        for chunk_start, chunk_end, chunk_id, out_dir in chunks:
            result = run_chunk(chunk_start, chunk_end, chunk_id, out_dir)
            if result:
                completed += 1
            else:
                failed += 1
    else:
        with ProcessPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(run_chunk, start, end, cid, out_dir): cid
                for start, end, cid, out_dir in chunks
            }

            for future in as_completed(futures):
                chunk_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        completed += 1
                    else:
                        failed += 1
                except Exception as e:
                    log.error(f"[Chunk {chunk_id}] Exception: {e}")
                    failed += 1

    log.info("=" * 70)
    log.info("BATCH PROCESSING COMPLETE")
    log.info("=" * 70)
    log.info(f"Completed: {completed:,} chunks")
    log.info(f"Failed: {failed:,} chunks")
    log.info(f"Output directory: {output_dir}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

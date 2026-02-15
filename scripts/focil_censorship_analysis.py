#!/usr/bin/env python3
"""
FOCIL Censorship Analysis

Analyzes Ethereum transaction censorship patterns using Fork-Choice enforced
Inclusion Lists (FOCIL) methodology. Constructs six inclusion list variants
per block and calculates overlap metrics for bandwidth analysis.

Inclusion List Variants (3 delays x 2 strategies):
  Top Fee Strategy (highest priority transactions):
    L0 Top Fee: Highest fee txs from current block window
    L-1 Top Fee: Highest fee txs from block N-1 window
    L-2 Top Fee: Highest fee txs from block N-2 window

  Censored Strategy (censorship resistance):
    L0 Censored: Transactions censored at current block
    L-1 Censored: Transactions censored at block N-1
    L-2 Censored: Transactions censored at block N-2

Censorship Detection:
  Transactions are flagged as censored when they meet the following criteria:
  1. FOCIL-valid (max_fee >= base_fee)
  2. Competitive effective priority fee (>= 25th percentile of mempool)
  3. Sufficient dwell time (>= 12 seconds in mempool)
  4. Not replaced by user via nonce replacement
  5. Not included in current, previous, or subsequent blocks

Implementation:
  - EIP-7805 compliant (8 KiB size cap per inclusion list)
  - Transactions sorted by effective priority fee (descending)
  - Effective priority fee = min(priority_fee, max_fee - base_fee) per EIP-1559
  - Results saved in Parquet format for analysis

Inclusion Rate (Redundancy) Semantics:
  Measures what % of IL transactions were naturally included without FOCIL.
  The IL is always built at block N. With delay D it is enforced at N+1+D.
  We check blocks N+1 through N+1+D for natural inclusion:
    0-delay: checks N+1 only (built at N, enforced at N+1)
    1-delay: checks N+1, N+2 (built at N, enforced at N+2)
    2-delay: checks N+1, N+2, N+3 (built at N, enforced at N+3)
  Higher redundancy for delayed variants is expected -- transactions have
  more time to be naturally included before enforcement.

Reference:
  https://hackmd.io/@pellekrab/HkzMiXkmZe
"""

import logging

import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from utils import (
    load_config, execute_query, fetch_block_data, fetch_included_txs,
    check_addresses_on_chain, AddressCache, pack_il, get_block_int,
    MAX_IL_BYTES, VARIANT_NAMES,
)

log = logging.getLogger(__name__)


def detect_nonce_replacements(mempool_df: pd.DataFrame, included_txs_map: dict) -> set:
    """Detect replaced transactions using actual inclusion data.

    For each (sender, nonce) pair with multiple transactions:
    - If one was included in a block, mark all others as replaced.
    - If none were included, keep highest-fee tx, mark rest as replaced.

    Returns set of replaced tx_hashes (exclude from censorship detection).
    """
    if 'sender' not in mempool_df.columns or 'nonce' not in mempool_df.columns:
        return set()

    all_included = set()
    for block_txs in included_txs_map.values():
        all_included |= block_txs

    replaced = set()
    for (_sender, _nonce), group in mempool_df.groupby(['sender', 'nonce']):
        if len(group) <= 1:
            continue

        hashes = set(group['tx_hash'])
        included_in_group = hashes & all_included

        if included_in_group:
            final_tx = next(iter(included_in_group))
            replaced.update(hashes - {final_tx})
        else:
            best = group.sort_values('max_fee', ascending=False).iloc[0]['tx_hash']
            replaced.update(hashes - {best})

    return replaced


def construct_il_variant(
    mempool_df: pd.DataFrame,
    variant_type: str,
    delay: int,
    block_num: int,
    block_ts: int,
    base_fee: int,
    blocks_df: pd.DataFrame,
    censored_txs: pd.DataFrame,
    already_included: set,
    active_senders: set,
    config: dict,
    max_bytes: int = MAX_IL_BYTES,
) -> pd.DataFrame:
    """Unified IL construction for all 6 variants.

    For topfee: selects highest effective-priority-fee txs from the mempool
    window of block (N - delay). Filters to EIP-1559 (type 2) only and
    requires an active sender (has on-chain txs in recent blocks).

    For censored: uses pre-flagged censored transactions, revalidated against
    the current block's base fee.

    Excludes txs already confirmed on-chain (already_included).
    """
    variant_name = f"{delay}delay_{variant_type}"

    # Each strategy has its own mempool window
    if variant_type == 'topfee':
        win_start = config['analysis']['topfee_window_start_secs']
        win_end = config['analysis']['topfee_window_end_secs']
    else:
        win_start = config['analysis']['censored_window_start_secs']
        win_end = config['analysis']['censored_window_end_secs']

    # Step 1: Get candidate transactions
    # IL is always built from block N's mempool window. The delay only affects
    # which block the IL is enforced at (and thus the inclusion rate check range).
    if variant_type == 'topfee':
        candidates = mempool_df[
            (mempool_df['seen_timestamp'] >= block_ts + win_start) &
            (mempool_df['seen_timestamp'] <= block_ts + win_end) &
            (mempool_df['max_fee'] >= base_fee)
        ].copy()

        # Filter to EIP-1559 (type 2) only to exclude phantom/spam legacy txs
        if 'tx_type' in candidates.columns:
            candidates = candidates[candidates['tx_type'] == 2]

        # Filter to active senders (have on-chain txs in recent blocks).
        # ~78% of Xatu mempool senders are phantoms with zero on-chain presence
        # whose txs have inflated fees but are never included by any builder.
        if active_senders:
            candidates = candidates[candidates['sender'].isin(active_senders)]

    elif variant_type == 'censored':
        if censored_txs is None or len(censored_txs) == 0:
            return pd.DataFrame()
        candidates = censored_txs[censored_txs['max_fee'] >= base_fee].copy()

    else:
        raise ValueError(f"Unknown variant_type: {variant_type}")

    if len(candidates) == 0:
        return pd.DataFrame()

    # Step 2: Deduplicate and compute effective priority fee
    candidates['effective_priority_fee'] = np.minimum(
        candidates['priority_fee'].astype(int),
        candidates['max_fee'].astype(int) - base_fee,
    )
    candidates = candidates.sort_values('effective_priority_fee', ascending=False)
    candidates = candidates.drop_duplicates(subset='tx_hash', keep='first')

    # Step 3: Exclude txs already confirmed on-chain
    if already_included:
        candidates = candidates[~candidates['tx_hash'].isin(already_included)]

    if len(candidates) == 0:
        return pd.DataFrame()

    # Step 4: Pack into IL
    return pack_il(candidates, max_bytes)


def flag_censored_transactions(
    mempool_df: pd.DataFrame,
    current_block_ts: int,
    current_base_fee: int,
    prev_block_gas_used: int,
    prev_block_gas_limit: int,
    curr_block_gas_used: int,
    curr_block_gas_limit: int,
    replaced_txs: set,
    all_included_txs: set,
    active_senders: set,
    config: dict,
) -> pd.DataFrame:
    """Flag transactions as censored using dual capacity check.

    A transaction is censored if:
    1. FOCIL-valid (max_fee >= base_fee)
    2. Competitive effective priority fee (>= configured percentile)
    3. Dwell time within [min, max] window
    4. Not a nonce replacement
    5. Would have fit in both previous and current block (gas capacity)
    6. Not included in any block from prev through subsequent
    7. From an active sender (has at least one included tx in nearby blocks)

    The active sender filter (7) removes phantom/spam transactions that meet
    fee thresholds but are never actually mined. ~70% of FOCIL-valid mempool
    txs with sufficient dwell time are never included in any of 15 surrounding
    blocks. Requiring the sender to have real on-chain activity filters these
    out without using forward-looking data (sender activity is observable from
    the blocks already produced before the IL is constructed).
    """
    if len(mempool_df) == 0:
        return pd.DataFrame()

    win_start = config['analysis']['censored_window_start_secs']
    win_end = config['analysis']['censored_window_end_secs']
    min_dwell = config['analysis']['censorship_dwell_time_secs']
    max_dwell = config['analysis'].get('censorship_max_dwell_time_secs', 120)
    fee_pct = config['analysis']['censorship_fee_percentile']

    # Fee threshold from FOCIL-valid mempool txs in the censored window.
    pre_block = mempool_df[
        (mempool_df['seen_timestamp'] >= current_block_ts + win_start) &
        (mempool_df['seen_timestamp'] <= current_block_ts + win_end) &
        (mempool_df['max_fee'] >= current_base_fee)
    ]
    if len(pre_block) == 0:
        return pd.DataFrame()

    effective_fees = np.minimum(
        pre_block['priority_fee'].astype(int),
        pre_block['max_fee'].astype(int) - current_base_fee,
    )
    fee_threshold = effective_fees.quantile(fee_pct)

    # Aggregate per-tx lifecycle
    lifecycle = mempool_df.groupby('tx_hash').agg({
        'seen_timestamp': 'min',
        'max_fee': 'first',
        'priority_fee': 'first',
        'tx_size': 'first',
        'gas_limit': 'first',
        'sender': 'first',
    }).reset_index()
    lifecycle.rename(columns={'seen_timestamp': 'first_seen'}, inplace=True)

    # Compute effective priority fee per tx
    lifecycle['effective_priority_fee'] = np.minimum(
        lifecycle['priority_fee'].astype(int),
        lifecycle['max_fee'].astype(int) - current_base_fee,
    )

    dwell = current_block_ts - lifecycle['first_seen']
    gas = pd.to_numeric(lifecycle['gas_limit'], errors='coerce')
    prev_available = prev_block_gas_limit - prev_block_gas_used
    curr_available = curr_block_gas_limit - curr_block_gas_used

    candidates = lifecycle[
        (lifecycle['max_fee'] >= current_base_fee) &
        (lifecycle['effective_priority_fee'] >= fee_threshold) &
        (lifecycle['first_seen'] < current_block_ts) &
        (dwell >= min_dwell) &
        (dwell <= max_dwell) &
        (~lifecycle['tx_hash'].isin(replaced_txs)) &
        (gas <= prev_available) &
        (gas <= curr_available) &
        (~lifecycle['tx_hash'].isin(all_included_txs)) &
        (lifecycle['sender'].isin(active_senders))
    ]

    return candidates


def process_single_block(
    block_row,
    blocks_df: pd.DataFrame,
    mempool_df: pd.DataFrame,
    included_txs_map: dict,
    replaced_txs: set,
    onchain_active_addresses: set,
    config: dict,
    collect_metrics: bool = False,
) -> dict | None:
    """Process a single block: build all 6 IL variants.

    When collect_metrics=True, returns a dict of per-block metrics.
    When False (warm-up), returns None.
    """
    block_num = int(block_row['block_number'])
    block_ts = int(block_row['block_timestamp'])
    base_fee = int(block_row['base_fee'])
    gas_used = get_block_int(block_row, 'gas_used')
    gas_limit = get_block_int(block_row, 'gas_limit')

    # Use union of both strategy windows for general mempool stats
    tf_start = config['analysis']['topfee_window_start_secs']
    tf_end = config['analysis']['topfee_window_end_secs']
    cs_start = config['analysis']['censored_window_start_secs']
    cs_end = config['analysis']['censored_window_end_secs']

    result = None
    if collect_metrics:
        result = {
            'block_number': block_num,
            'block_timestamp': block_ts,
            'base_fee': base_fee,
            'gas_used': gas_used,
            'gas_limit': gas_limit,
            'included_tx_count': int(block_row['included_tx_count']),
        }

        # Mempool coverage of next block
        next_block_txs = included_txs_map.get(block_num + 1, set())
        win_lo = block_ts + min(tf_start, cs_start)
        win_hi = block_ts + max(tf_end, cs_end)
        window_hashes = set(mempool_df[
            (mempool_df['seen_timestamp'] >= win_lo) &
            (mempool_df['seen_timestamp'] <= win_hi)
        ]['tx_hash'].unique())
        overlap = window_hashes & next_block_txs
        result['mempool_coverage_of_next_block'] = (
            len(overlap) / len(next_block_txs) * 100
            if next_block_txs else 0.0
        )
        result['mempool_unique_txs_in_window'] = len(window_hashes)

    # Active address filter: use pre-computed set of addresses with on-chain
    # history (checked against full chain as sender or receiver).
    # Also include senders whose txs were included in earlier batch blocks
    # (catches brand-new addresses that just appeared on-chain).
    all_included_before = set()
    for bn, txs in included_txs_map.items():
        if bn < block_num:
            all_included_before |= txs
    mempool_senders_with_inclusion = set(mempool_df[
        mempool_df['tx_hash'].isin(all_included_before)
    ]['sender'].unique())
    active_senders = mempool_senders_with_inclusion | {
        s for s in mempool_df['sender'].unique()
        if s.lower() in onchain_active_addresses
    }

    # IL is always built at block N. Exclude txs already on-chain.
    already_included = set()
    for bn, txs in included_txs_map.items():
        if bn <= block_num:
            already_included |= txs

    # Build all 6 variants
    for delay in [0, 1, 2]:
        for variant_type in ['topfee', 'censored']:
            variant_name = f'{delay}delay_{variant_type}'

            # For censored variants, flag censored transactions at block N.
            # The delay only affects enforcement (and thus inclusion rate check).
            censored_txs = None
            if variant_type == 'censored':
                prev_blk = blocks_df[blocks_df['block_number'] == block_num - 1]
                curr_blk = blocks_df[blocks_df['block_number'] == block_num]

                if len(prev_blk) > 0 and len(curr_blk) > 0:
                    censored_txs = flag_censored_transactions(
                        mempool_df=mempool_df,
                        current_block_ts=block_ts,
                        current_base_fee=base_fee,
                        prev_block_gas_used=get_block_int(prev_blk.iloc[0], 'gas_used'),
                        prev_block_gas_limit=get_block_int(prev_blk.iloc[0], 'gas_limit'),
                        curr_block_gas_used=gas_used,
                        curr_block_gas_limit=gas_limit,
                        replaced_txs=replaced_txs,
                        all_included_txs=already_included,
                        active_senders=active_senders,
                        config=config,
                    )

                    if collect_metrics and delay == 0:
                        result['censored_detected_count'] = len(censored_txs) if censored_txs is not None else 0

            # Construct IL
            il_df = construct_il_variant(
                mempool_df=mempool_df,
                variant_type=variant_type,
                delay=delay,
                block_num=block_num,
                block_ts=block_ts,
                base_fee=base_fee,
                blocks_df=blocks_df,
                censored_txs=censored_txs,
                already_included=already_included,
                active_senders=active_senders,
                config=config,
            )

            if collect_metrics:
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

                    redundant_size = int(il_df[il_df['tx_hash'].isin(redundant)]['tx_size'].sum())
                    result[f'{variant_name}_redundant_bytes'] = redundant_size
                    result[f'{variant_name}_useful_bytes'] = il_size - redundant_size
                else:
                    result[f'{variant_name}_inclusion_rate'] = None
                    result[f'{variant_name}_redundant_bytes'] = 0
                    result[f'{variant_name}_useful_bytes'] = 0

    return result


def analyze_block_range(start_block: int, end_block: int, config: dict,
                        address_cache: AddressCache | None = None):
    """6-variant FOCIL analysis with verified deduplication.

    Processes blocks sequentially with a 3-block warm-up phase before
    collecting metrics. When address_cache is provided, reuses cached
    on-chain lookups from previous batches.
    """
    log.info("Analyzing blocks %d to %d", start_block, end_block)

    # Padding: max dwell time (default 120s) / 12s per slot ≈ 10 blocks.
    # Need enough lookback so already_included covers all txs that could
    # appear in the mempool window of the first analysis block.
    max_dwell = config['analysis'].get('censorship_max_dwell_time_secs', 120)
    lookback_blocks = max(3, max_dwell // 12 + 1)

    # Fetch blocks (extra padding for warm-up and forward lookback)
    log.info("Fetching block data...")
    blocks_df = fetch_block_data(start_block - lookback_blocks, end_block + 3, config)
    if len(blocks_df) == 0:
        log.warning("No blocks found for range %d-%d", start_block, end_block)
        return None, address_cache
    log.info("Got %d blocks (including warm-up and lookback)", len(blocks_df))

    # Mempool time range (cover both strategy windows with padding)
    tf_start = config['analysis']['topfee_window_start_secs']
    tf_end = config['analysis']['topfee_window_end_secs']
    cs_start = config['analysis']['censored_window_start_secs']
    cs_end = config['analysis']['censored_window_end_secs']
    min_ts = blocks_df['block_timestamp'].min() + min(tf_start, cs_start) - 2
    max_ts = blocks_df['block_timestamp'].max() + max(tf_end, cs_end) + 2

    log.info("Fetching mempool data...")
    mempool_query = f"""
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
    mempool_df = execute_query(mempool_query, config)
    mempool_df['gas_limit'] = pd.to_numeric(mempool_df['gas_limit'], errors='coerce')
    log.info("Got %d mempool transactions", len(mempool_df))

    # Included transactions
    log.info("Fetching included transactions...")
    try:
        included_txs_map = fetch_included_txs(start_block - lookback_blocks, end_block + 3, config)
    except Exception as exc:
        log.warning("Could not fetch inclusion data: %s", exc)
        included_txs_map = {}

    # Nonce replacements
    log.info("Detecting nonce replacements...")
    replaced_txs = detect_nonce_replacements(mempool_df, included_txs_map)
    log.info("Found %d replaced transactions", len(replaced_txs))

    # Active address filter: check mempool senders against on-chain appearances.
    # Uses a persistent cache so addresses checked in earlier batches aren't
    # re-queried — over a full study run this builds complete coverage.
    if address_cache is None:
        address_cache = AddressCache()

    # Feed included tx senders into the cache — if an address sent a tx that
    # got included on-chain, it's definitively active.
    included_senders = set(
        mempool_df[mempool_df['tx_hash'].isin(
            {h for txs in included_txs_map.values() for h in txs}
        )]['sender'].str.lower().unique()
    )
    address_cache.add_active(included_senders)

    all_senders = set(mempool_df['sender'].str.lower().unique())
    log.info("Checking %d unique mempool senders (%d new, %d cached)...",
             len(all_senders), len(address_cache.unchecked(all_senders)),
             len(all_senders) - len(address_cache.unchecked(all_senders)))
    onchain_active_addresses = check_addresses_on_chain(
        all_senders, start_block, config, cache=address_cache,
    )

    # Warm-up phase (3 blocks before start)
    log.info("Warm-up phase (3 blocks)...")
    warmup = blocks_df[
        (blocks_df['block_number'] >= start_block - min(3, lookback_blocks)) &
        (blocks_df['block_number'] < start_block)
    ]
    for _, block_row in warmup.iterrows():
        process_single_block(
            block_row, blocks_df, mempool_df, included_txs_map,
            replaced_txs, onchain_active_addresses, config,
            collect_metrics=False,
        )

    # Main processing
    log.info("Main processing phase...")
    main_blocks = blocks_df[
        (blocks_df['block_number'] >= start_block) &
        (blocks_df['block_number'] < end_block)
    ]

    results = []
    for _, block_row in tqdm(main_blocks.iterrows(), total=len(main_blocks), desc="  Processing"):
        row = process_single_block(
            block_row, blocks_df, mempool_df, included_txs_map,
            replaced_txs, onchain_active_addresses, config,
            collect_metrics=True,
        )
        if row:
            results.append(row)

    result_df = pd.DataFrame(results) if results else None
    return result_df, address_cache


def print_summary(df: pd.DataFrame):
    """Print analysis summary to stdout."""
    blocks_per_year = 7200 * 365

    print("\n" + "=" * 70)
    print("SUMMARY - 6-VARIANT FOCIL ANALYSIS")
    print("=" * 70)

    print(f"\nBlocks analyzed: {len(df):,}")
    avg_gas_pct = df['gas_used'].mean() / df['gas_limit'].mean() * 100
    print(f"Average gas usage: {df['gas_used'].mean() / 1e6:.2f}M ({avg_gas_pct:.1f}% of limit)")

    if 'mempool_coverage_of_next_block' in df.columns:
        print(f"Avg mempool coverage of next block: {df['mempool_coverage_of_next_block'].mean():.1f}%")
        print(f"Avg unique mempool txs in window: {df['mempool_unique_txs_in_window'].mean():.0f}")

    # Bandwidth matrix
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

    # Delay effect on useful bandwidth
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

    # Censorship summary
    if 'censored_detected_count' in df.columns:
        print("\n" + "=" * 70)
        print("CENSORSHIP DETECTION")
        print("=" * 70)
        avg = df['censored_detected_count'].mean()
        blocks_with = (df['censored_detected_count'] > 0).sum()
        print(f"  Average censored txs/block: {avg:.2f}")
        print(f"  Blocks with censorship: {blocks_with:,} ({blocks_with / len(df) * 100:.1f}%)")

    # Statistical notes
    print("\n" + "=" * 70)
    print("STATISTICAL NOTES")
    print("=" * 70)
    n = len(df)
    print(f"  Sample size: {n:,} blocks")
    if n < 1000:
        print("  WARNING: Small sample. Recommend >= 1,000 blocks (ideally 50,000+).")
    elif n < 10000:
        print("  Moderate sample. Results indicative but extend to 50,000 for publication.")
    else:
        print("  Large sample. Results likely statistically significant.")
    print()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S',
    )

    config = load_config()

    print("=" * 70)
    print("FOCIL CENSORSHIP ANALYSIS")
    print("=" * 70)
    print(f"IL size cap: {MAX_IL_BYTES:,} bytes ({MAX_IL_BYTES / 1024:.1f} KiB)")
    print(f"Top Fee window: [{config['analysis']['topfee_window_start_secs']}, "
          f"{config['analysis']['topfee_window_end_secs']}] seconds")
    print(f"Censored window: [{config['analysis']['censored_window_start_secs']}, "
          f"{config['analysis']['censored_window_end_secs']}] seconds")

    start_block = config['analysis']['start_block']
    end_block = config['analysis']['end_block']
    batch_size = config['analysis'].get('batch_size_blocks', 100)

    all_results = []
    address_cache = AddressCache()
    for batch_start in range(start_block, end_block, batch_size):
        batch_end = min(batch_start + batch_size, end_block)
        result_df, address_cache = analyze_block_range(
            batch_start, batch_end, config, address_cache=address_cache,
        )
        if result_df is not None:
            all_results.append(result_df)

    if not all_results:
        log.error("No results produced.")
        return

    df = pd.concat(all_results, ignore_index=True)

    # Save results
    results_dir = Path(__file__).parent.parent / "results"
    results_dir.mkdir(exist_ok=True)
    output_file = results_dir / "focil_censorship_analysis.parquet"
    df.to_parquet(output_file, index=False)
    print(f"\nResults saved to: {output_file}")

    print_summary(df)


if __name__ == "__main__":
    main()

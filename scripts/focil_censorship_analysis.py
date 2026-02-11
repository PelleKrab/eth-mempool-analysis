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
  Since this runs against historical data with no ILs active, we check all
  blocks between IL construction and enforcement:
    0-delay: checks N+1 only (built at N, enforced at N+1)
    1-delay: checks N, N+1 (built at N-1, enforced at N+1)
    2-delay: checks N-1, N, N+1 (built at N-2, enforced at N+1)
  Higher redundancy for delayed variants is expected — transactions have
  more time to be naturally included before enforcement.

Reference:
  https://hackmd.io/@pellekrab/HkzMiXkmZe
"""

import io
import logging
import os
import re
import time

import numpy as np
import pandas as pd
import requests
import yaml
from pathlib import Path
from tqdm import tqdm

log = logging.getLogger(__name__)

# EIP-7805 Constants
MAX_IL_BYTES = 8192  # 8 KiB

VARIANT_NAMES = [
    '0delay_topfee', '0delay_censored',
    '1delay_topfee', '1delay_censored',
    '2delay_topfee', '2delay_censored',
]


def load_config():
    """Load config with environment variable resolution.

    Config values like ${VAR_NAME} or ${VAR_NAME:default} are resolved
    from environment variables. Falls back to .env file in project root.
    """
    project_root = Path(__file__).parent.parent
    env_file = project_root / ".env"

    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    os.environ.setdefault(key.strip(), value.strip())

    config_file = project_root / "config" / "config.yaml"
    with open(config_file) as f:
        raw = f.read()

    def _resolve(match):
        expr = match.group(1)
        if ':' in expr:
            var_name, _, default = expr.partition(':')
            return os.environ.get(var_name, default)
        return os.environ[expr]

    resolved = re.sub(r'\$\{([^}]+)\}', _resolve, raw)
    return yaml.safe_load(resolved)


def execute_query(query: str, config: dict, max_retries: int = 3) -> pd.DataFrame:
    """Execute ClickHouse query via HTTP with retry logic."""
    ch = config['clickhouse']
    for attempt in range(max_retries):
        try:
            response = requests.post(
                ch['url'],
                auth=(ch['user'], ch['password']),
                data=(query + " FORMAT CSVWithNames").encode('utf-8'),
                params={'database': ch['database']},
                timeout=300,
            )
            if response.status_code != 200:
                raise RuntimeError(f"HTTP {response.status_code}: {response.text[:500]}")
            return pd.read_csv(io.StringIO(response.text))
        except (requests.RequestException, RuntimeError) as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                log.warning("Query failed (attempt %d/%d), retrying in %ds: %s",
                            attempt + 1, max_retries, wait, exc)
                time.sleep(wait)
            else:
                raise


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


def get_block_transactions_batch(start_block: int, end_block: int, config: dict) -> dict:
    """Fetch included transactions for a range of blocks.

    Returns dict mapping block_number -> set(tx_hashes).
    """
    log.info("Fetching included transactions for blocks %d to %d", start_block, end_block)

    query = f"""
    SELECT DISTINCT
        block_number,
        transaction_hash
    FROM canonical_execution_transaction
    WHERE block_number >= {start_block}
      AND block_number < {end_block}
    """
    df = execute_query(query, config)

    if len(df) == 0:
        log.warning("No included transactions found for range %d-%d", start_block, end_block)
        return {}

    result = {}
    for block_num, group in df.groupby('block_number'):
        result[int(block_num)] = set(group['transaction_hash'])

    log.info("Got %d blocks with %d included txs", len(result), len(df))
    return result


def pack_il(candidates: pd.DataFrame, max_bytes: int = MAX_IL_BYTES) -> pd.DataFrame:
    """Pack sorted candidates into an inclusion list respecting the size cap.

    Uses vectorized cumsum instead of row-by-row iteration.
    Candidates must already be sorted by priority (descending).
    """
    if len(candidates) == 0:
        return pd.DataFrame()

    valid = candidates[candidates['tx_size'].notna() & (candidates['tx_size'] > 0)].copy()
    if len(valid) == 0:
        return pd.DataFrame()

    valid['_cumsize'] = valid['tx_size'].astype(int).cumsum()
    packed = valid[valid['_cumsize'] <= max_bytes].drop(columns=['_cumsize'])

    return packed if len(packed) > 0 else pd.DataFrame()


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
    config: dict,
    max_bytes: int = MAX_IL_BYTES,
) -> pd.DataFrame:
    """Unified IL construction for all 6 variants.

    For topfee: selects highest effective-priority-fee txs from the mempool
    window of block (N - delay). Filters to EIP-1559 (type 2) only.

    For censored: uses pre-flagged censored transactions, revalidated against
    the current block's base fee.

    Excludes txs already confirmed on-chain (already_included).
    """
    variant_name = f"{delay}delay_{variant_type}"
    window_start = config['analysis']['time_window_start_secs']
    window_end = config['analysis']['time_window_end_secs']

    # Step 1: Get candidate transactions
    if variant_type == 'topfee':
        if delay == 0:
            target_ts = block_ts
        else:
            target_block = blocks_df[blocks_df['block_number'] == block_num - delay]
            if len(target_block) == 0:
                return pd.DataFrame()
            target_ts = int(target_block.iloc[0]['block_timestamp'])

        candidates = mempool_df[
            (mempool_df['seen_timestamp'] >= target_ts + window_start) &
            (mempool_df['seen_timestamp'] <= target_ts + window_end) &
            (mempool_df['max_fee'] >= base_fee)
        ].copy()

        # Filter to EIP-1559 (type 2) only to exclude phantom/spam legacy txs
        if 'tx_type' in candidates.columns:
            candidates = candidates[candidates['tx_type'] == 2]

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

    min_dwell = config['analysis']['censorship_dwell_time_secs']
    max_dwell = config['analysis'].get('censorship_max_dwell_time_secs', 120)
    fee_pct = config['analysis']['censorship_fee_percentile']
    pct_window = config['analysis']['censorship_percentile_window_secs']

    # Fee threshold from FOCIL-valid mempool txs seen before the block.
    # Only include txs with max_fee >= base_fee so the effective priority
    # fee is non-negative — otherwise underpriced txs drag the percentile
    # to meaningless negative values.
    pre_block = mempool_df[
        (mempool_df['seen_timestamp'] >= current_block_ts - pct_window) &
        (mempool_df['seen_timestamp'] <= current_block_ts) &
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


def _get_block_int(block_row, col):
    """Safely extract integer value from a block row, defaulting to 0."""
    val = block_row[col]
    return int(val) if pd.notna(val) else 0


def process_single_block(
    block_row,
    blocks_df: pd.DataFrame,
    mempool_df: pd.DataFrame,
    included_txs_map: dict,
    replaced_txs: set,
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
    gas_used = _get_block_int(block_row, 'gas_used')
    gas_limit = _get_block_int(block_row, 'gas_limit')

    window_start = config['analysis']['time_window_start_secs']
    window_end = config['analysis']['time_window_end_secs']

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
        window_hashes = set(mempool_df[
            (mempool_df['seen_timestamp'] >= block_ts + window_start) &
            (mempool_df['seen_timestamp'] <= block_ts + window_end)
        ]['tx_hash'].unique())
        overlap = window_hashes & next_block_txs
        result['mempool_coverage_of_next_block'] = (
            len(overlap) / len(next_block_txs) * 100
            if next_block_txs else 0.0
        )
        result['mempool_unique_txs_in_window'] = len(window_hashes)

    # Txs already on-chain when IL is built at block N (includes block N itself)
    already_included = set()
    for bn, txs in included_txs_map.items():
        if bn <= block_num:
            already_included |= txs

    # Compute active senders: mempool senders with at least one tx included
    # in any block before the current one. This filters phantom/spam senders
    # without using forward-looking data.
    all_included_before = already_included - included_txs_map.get(block_num, set())
    mempool_senders_with_inclusion = mempool_df[
        mempool_df['tx_hash'].isin(all_included_before)
    ]['sender'].unique()
    active_senders = set(mempool_senders_with_inclusion)

    # Build all 6 variants
    for delay in [0, 1, 2]:
        for variant_type in ['topfee', 'censored']:
            variant_name = f'{delay}delay_{variant_type}'

            # For censored variants, flag censored transactions first
            censored_txs = None
            if variant_type == 'censored':
                target_block_num = block_num - delay
                prev_blk = blocks_df[blocks_df['block_number'] == target_block_num - 1]
                curr_blk = blocks_df[blocks_df['block_number'] == target_block_num]

                if len(prev_blk) > 0 and len(curr_blk) > 0:
                    target = curr_blk.iloc[0]

                    # Gather all included txs from prev through current block
                    all_included = set()
                    for bn in range(target_block_num - 1, block_num + 1):
                        all_included |= included_txs_map.get(bn, set())

                    censored_txs = flag_censored_transactions(
                        mempool_df=mempool_df,
                        current_block_ts=int(target['block_timestamp']),
                        current_base_fee=int(target['base_fee']),
                        prev_block_gas_used=_get_block_int(prev_blk.iloc[0], 'gas_used'),
                        prev_block_gas_limit=_get_block_int(prev_blk.iloc[0], 'gas_limit'),
                        curr_block_gas_used=_get_block_int(target, 'gas_used'),
                        curr_block_gas_limit=_get_block_int(target, 'gas_limit'),
                        replaced_txs=replaced_txs,
                        all_included_txs=all_included,
                        active_senders=active_senders,
                        config=config,
                    )

                    if collect_metrics and delay == 1:
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
                config=config,
            )

            if collect_metrics:
                result[f'{variant_name}_tx_count'] = len(il_df)
                result[f'{variant_name}_size_bytes'] = (
                    int(il_df['tx_size'].sum()) if len(il_df) > 0 else 0
                )

                # Inclusion rate (redundancy): what % of IL txs were included
                # in any block between IL construction and enforcement?
                # A delayed IL is built from block (N-delay)'s mempool window.
                # Between construction and enforcement at N+1, transactions may
                # be naturally included in blocks N-delay+1 through N+1.
                # Checking this full range measures how redundant the IL is
                # against historical block production (no ILs were active).
                if len(il_df) > 0:
                    included_in_range = set()
                    for bn in range(block_num - delay + 1, block_num + 2):
                        included_in_range |= included_txs_map.get(bn, set())
                    if included_in_range:
                        il_hashes = set(il_df['tx_hash'])
                        rate = len(il_hashes & included_in_range) / len(il_hashes) * 100
                        result[f'{variant_name}_inclusion_rate'] = rate
                    else:
                        result[f'{variant_name}_inclusion_rate'] = None
                else:
                    result[f'{variant_name}_inclusion_rate'] = None

    return result


def analyze_block_range(start_block: int, end_block: int, config: dict):
    """6-variant FOCIL analysis with verified deduplication.

    Processes blocks sequentially with a 3-block warm-up phase before
    collecting metrics.
    """
    log.info("Analyzing blocks %d to %d", start_block, end_block)

    # Fetch blocks (extra padding for warm-up and forward lookback)
    log.info("Fetching block data...")
    blocks_query = f"""
    SELECT
        execution_payload_block_number as block_number,
        toUnixTimestamp(slot_start_date_time) as block_timestamp,
        toUInt256(execution_payload_base_fee_per_gas) as base_fee,
        execution_payload_transactions_count as included_tx_count,
        toUInt256(execution_payload_gas_used) as gas_used,
        toUInt256(execution_payload_gas_limit) as gas_limit
    FROM canonical_beacon_block
    WHERE execution_payload_block_number >= {start_block - 3}
      AND execution_payload_block_number < {end_block + 3}
    ORDER BY execution_payload_block_number
    """
    blocks_df = execute_query(blocks_query, config)
    if len(blocks_df) == 0:
        log.warning("No blocks found for range %d-%d", start_block, end_block)
        return None

    blocks_df['gas_used'] = pd.to_numeric(blocks_df['gas_used'], errors='coerce')
    blocks_df['gas_limit'] = pd.to_numeric(blocks_df['gas_limit'], errors='coerce')
    log.info("Got %d blocks (including warm-up and lookback)", len(blocks_df))

    # Mempool time range
    window_start = config['analysis']['time_window_start_secs']
    window_end = config['analysis']['time_window_end_secs']
    min_ts = blocks_df['block_timestamp'].min() - 24 + window_start - 2
    max_ts = blocks_df['block_timestamp'].max() + window_end + 2

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
        included_txs_map = get_block_transactions_batch(start_block - 3, end_block + 3, config)
    except Exception as exc:
        log.warning("Could not fetch inclusion data: %s", exc)
        included_txs_map = {}

    # Nonce replacements
    log.info("Detecting nonce replacements...")
    replaced_txs = detect_nonce_replacements(mempool_df, included_txs_map)
    log.info("Found %d replaced transactions", len(replaced_txs))

    # Warm-up phase (3 blocks before start)
    log.info("Warm-up phase (3 blocks)...")
    warmup = blocks_df[
        (blocks_df['block_number'] >= start_block - 3) &
        (blocks_df['block_number'] < start_block)
    ]
    for _, block_row in warmup.iterrows():
        process_single_block(
            block_row, blocks_df, mempool_df, included_txs_map,
            replaced_txs, config,
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
            replaced_txs, config,
            collect_metrics=True,
        )
        if row:
            results.append(row)

    return pd.DataFrame(results) if results else None


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

            avg_kb = df[col_size].mean() / 1024
            avg_count = df[col_count].mean()
            annual_gb = df[col_size].mean() * blocks_per_year / (1024 ** 3)

            line = f"  {delay}-delay: {avg_kb:.2f} KiB/block, {avg_count:.1f} txs, {annual_gb:.2f} GB/year"
            if col_rate in df.columns and df[col_rate].notna().any():
                line += f", inclusion={df[col_rate].dropna().mean():.1f}%"
            print(line)

    # Delay effect (primary research question)
    print("\n" + "=" * 70)
    print("DELAY EFFECT ON BANDWIDTH")
    print("=" * 70)

    for strategy in ['topfee', 'censored']:
        label = "Top Fee" if strategy == 'topfee' else "Censored"
        base = df[f'0delay_{strategy}_size_bytes'].mean()
        if base == 0:
            continue
        print(f"\n## {label} Strategy")
        for delay in [0, 1, 2]:
            val = df[f'{delay}delay_{strategy}_size_bytes'].mean()
            annual = val * blocks_per_year / (1024 ** 3)
            pct = (val / base - 1) * 100 if delay > 0 else 0
            suffix = f" ({pct:+.1f}%)" if delay > 0 else " (baseline)"
            print(f"  {delay}-delay: {annual:.2f} GB/year{suffix}")

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
    print(f"Time window: [{config['analysis']['time_window_start_secs']}, "
          f"{config['analysis']['time_window_end_secs']}] seconds")

    start_block = config['analysis']['start_block']
    end_block = config['analysis']['end_block']
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
    output_file = results_dir / "focil_censorship_analysis.parquet"
    df.to_parquet(output_file, index=False)
    print(f"\nResults saved to: {output_file}")

    print_summary(df)


if __name__ == "__main__":
    main()

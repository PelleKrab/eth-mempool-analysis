#!/usr/bin/env python3
"""
FOCIL Censorship Analysis

Analyzes Ethereum transaction censorship patterns using Fork-Choice enforced
Inclusion Lists (FOCIL) methodology. Constructs three types of inclusion lists
per block and calculates overlap metrics for bandwidth analysis.

Inclusion List Types:
  L₀: Top-N highest priority fee transactions from current block time window
  L₋₁: Transactions flagged as censored at block N-1
  L₋₂: Transactions flagged as censored at block N-2

Censorship Detection:
  Transactions are flagged as censored when they meet the following criteria:
  1. FOCIL-valid (max_fee >= base_fee)
  2. Competitive priority fee (>= 25th percentile of mempool transactions)
  3. Sufficient dwell time (>= 12 seconds in mempool)
  4. Not replaced by user via nonce replacement
  5. Remain in mempool (not included in block)

Nonce Replacement Handling:
  To avoid false positives, transactions replaced by users are excluded from
  censorship detection. Replacements are identified by grouping transactions
  by (sender, nonce) and treating the highest-fee transaction as final.

Implementation:
  - EIP-7805 compliant (8 KiB size cap per inclusion list)
  - Transactions sorted by priority fee (descending)
  - Results saved in Parquet format for analysis

Reference:
  https://hackmd.io/@pellekrab/HkzMiXkmZe
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import yaml
import requests
from tqdm import tqdm


# EIP-7805 Constants
MAX_IL_BYTES = 8192  # 8 KiB


def load_config():
    """Load ClickHouse config"""
    config_file = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def execute_query(query: str, config: dict) -> pd.DataFrame:
    """Execute ClickHouse query via HTTP"""
    response = requests.post(
        config['clickhouse']['url'],
        auth=(config['clickhouse']['user'], config['clickhouse']['password']),
        data=(query + " FORMAT CSVWithNames").encode('utf-8'),
        params={'database': config['clickhouse']['database']},
        timeout=300
    )

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}: {response.text}")

    import io
    return pd.read_csv(io.StringIO(response.text))


def construct_il(mempool_txs: pd.DataFrame, base_fee: int, max_bytes: int = MAX_IL_BYTES) -> pd.DataFrame:
    """
    Construct inclusion list following EIP-7805:
    - Filter by base fee
    - Sort by priority fee (descending)
    - Select top transactions until size cap
    """
    # Filter: only txs with max_fee >= base_fee
    valid_txs = mempool_txs[mempool_txs['max_fee'] >= base_fee].copy()

    if len(valid_txs) == 0:
        return pd.DataFrame()

    # Sort by priority fee (highest first)
    valid_txs = valid_txs.sort_values('priority_fee', ascending=False)

    # Select transactions until we hit size cap
    il_txs = []
    total_size = 0

    for idx, tx in valid_txs.iterrows():
        # Skip transactions with missing or invalid size data
        if pd.isna(tx['tx_size']) or tx['tx_size'] <= 0:
            continue

        tx_size = int(tx['tx_size'])
        if total_size + tx_size <= max_bytes:
            il_txs.append(tx)
            total_size += tx_size
        else:
            break  # Hit size cap

    if len(il_txs) == 0:
        return pd.DataFrame()

    return pd.DataFrame(il_txs)


def detect_nonce_replacements(mempool_df: pd.DataFrame) -> set:
    """
    Identify transactions that were replaced by the user.
    Returns set of tx_hashes that should NOT be flagged as censored.

    Based on nonce_replacements.sql logic:
    - Partition by (sender, nonce)
    - Rank by fee (highest first), then timestamp (latest first)
    - If multiple versions exist, non-final versions are "replaced"
    """
    if 'sender' not in mempool_df.columns or 'nonce' not in mempool_df.columns:
        return set()

    # Group by (sender, nonce) and filter for groups with multiple transactions
    grouped = mempool_df.groupby(['sender', 'nonce'])

    replaced_hashes = []
    for (sender, nonce), group in grouped:
        if len(group) > 1:
            # Sort by max_fee (descending) and seen_timestamp (descending)
            sorted_group = group.sort_values(['max_fee', 'seen_timestamp'], ascending=[False, False])
            # All transactions except the first (highest fee, latest) are replaced
            replaced_hashes.extend(sorted_group.iloc[1:]['tx_hash'].tolist())

    return set(replaced_hashes)


def flag_censored_transactions(
    mempool_df: pd.DataFrame,
    block_timestamp: int,
    base_fee: int,
    replaced_txs: set,
    config: dict
) -> pd.DataFrame:
    """
    Flag transactions as censored based on configured thresholds.
    All parameters read from config.
    """
    if len(mempool_df) == 0:
        return pd.DataFrame()

    # Load censorship detection parameters from config
    min_dwell_time_secs = config['analysis']['censorship_dwell_time_secs']
    fee_percentile = config['analysis']['censorship_fee_percentile']
    percentile_window_secs = config['analysis']['censorship_percentile_window_secs']

    # Calculate fee percentile from mempool txs seen before block
    pre_block_txs = mempool_df[
        (mempool_df['seen_timestamp'] >= block_timestamp - percentile_window_secs) &
        (mempool_df['seen_timestamp'] <= block_timestamp)
    ]

    if len(pre_block_txs) == 0:
        return pd.DataFrame()

    fee_threshold = pre_block_txs['priority_fee'].quantile(fee_percentile)

    # Step 2: For each transaction, calculate dwell time
    # First seen = min(seen_timestamp) for that tx_hash
    # Last seen = max(seen_timestamp) for that tx_hash
    tx_lifecycle = mempool_df.groupby('tx_hash').agg({
        'seen_timestamp': ['min', 'max'],
        'max_fee': 'first',
        'priority_fee': 'first',
        'tx_size': 'first'
    }).reset_index()
    tx_lifecycle.columns = ['tx_hash', 'first_seen', 'last_seen', 'max_fee',
                             'priority_fee', 'tx_size']

    # Filter for censored candidates
    censored = tx_lifecycle[
        # FOCIL-valid
        (tx_lifecycle['max_fee'] >= base_fee) &
        # Competitive priority fee
        (tx_lifecycle['priority_fee'] >= fee_threshold) &
        # First seen before block
        (tx_lifecycle['first_seen'] < block_timestamp) &
        # Sufficient dwell time
        ((block_timestamp - tx_lifecycle['first_seen']) >= min_dwell_time_secs) &
        # NOT replaced by user
        (~tx_lifecycle['tx_hash'].isin(replaced_txs)) &
        # Still in mempool (not included yet)
        (tx_lifecycle['last_seen'] >= block_timestamp - min_dwell_time_secs)
    ]

    return censored


def construct_censored_il(
    censored_txs: pd.DataFrame,
    max_bytes: int = MAX_IL_BYTES
) -> pd.DataFrame:
    """
    Construct IL containing ONLY censored transactions.
    Sort by priority fee (highest first) to prioritize most valuable censored txs.
    Pack until 8 KiB size cap.
    """
    if len(censored_txs) == 0:
        return pd.DataFrame()

    # Sort censored transactions by priority fee (highest first)
    sorted_txs = censored_txs.sort_values('priority_fee', ascending=False)

    # Pack until size cap
    il_txs = []
    total_size = 0

    for idx, tx in sorted_txs.iterrows():
        # Skip transactions with missing or invalid size data
        if pd.isna(tx['tx_size']) or tx['tx_size'] <= 0:
            continue

        tx_size = int(tx['tx_size'])
        if total_size + tx_size <= max_bytes:
            il_txs.append(tx)
            total_size += tx_size
        else:
            break  # Hit size cap

    if len(il_txs) == 0:
        return pd.DataFrame()

    return pd.DataFrame(il_txs)


def analyze_block_range(start_block: int, end_block: int, config: dict):
    """Analyze a range of blocks with proper L₀, L₋₁, L₋₂ construction"""

    print(f"\nAnalyzing blocks {start_block:,} to {end_block:,}")

    # Fetch block data
    print("  Fetching block data...")
    blocks_query = f"""
    SELECT
        execution_payload_block_number as block_number,
        toUnixTimestamp(slot_start_date_time) as block_timestamp,
        toUInt256(execution_payload_base_fee_per_gas) as base_fee,
        execution_payload_transactions_count as included_tx_count
    FROM canonical_beacon_block
    WHERE execution_payload_block_number >= {start_block}
      AND execution_payload_block_number < {end_block}
    ORDER BY execution_payload_block_number
    """
    blocks_df = execute_query(blocks_query, config)

    if len(blocks_df) == 0:
        print("  No blocks found!")
        return None

    # Calculate overall time range for mempool query
    window_start = config['analysis']['time_window_start_secs']
    window_end = config['analysis']['time_window_end_secs']

    # Need to go back 2 blocks for L₋₂
    min_timestamp = blocks_df['block_timestamp'].min() - 24 + window_start - 2
    max_timestamp = blocks_df['block_timestamp'].max() + window_end + 2

    # Fetch mempool data for entire time range
    print(f"  Fetching mempool data from {min_timestamp} to {max_timestamp}...")
    mempool_query = f"""
    SELECT
        hash as tx_hash,
        `from` as sender,
        nonce,
        toUnixTimestamp(event_date_time) as seen_timestamp,
        toUInt256(gas_fee_cap) as max_fee,
        toUInt256(gas_tip_cap) as priority_fee,
        size as tx_size
    FROM mempool_transaction
    WHERE event_date_time >= toDateTime({int(min_timestamp)})
      AND event_date_time < toDateTime({int(max_timestamp)})
    ORDER BY event_date_time
    """
    mempool_df = execute_query(mempool_query, config)

    print(f"  Got {len(blocks_df)} blocks and {len(mempool_df):,} mempool txs")

    # Detect nonce replacements to avoid false censorship flags
    replaced_txs_set = detect_nonce_replacements(mempool_df)
    print(f"  Found {len(replaced_txs_set):,} replaced transactions (will not flag as censored)")

    # Process each block
    print("  Constructing ILs...")
    results = []

    for idx, block in tqdm(blocks_df.iterrows(), total=len(blocks_df), desc="  Processing"):
        block_num = block['block_number']
        block_ts = block['block_timestamp']
        base_fee = int(block['base_fee'])

        # ===== L₀: IL based on current block (N) timestamp =====
        L0_window_mask = (
            (mempool_df['seen_timestamp'] >= block_ts + window_start) &
            (mempool_df['seen_timestamp'] <= block_ts + window_end)
        )
        L0_mempool = mempool_df[L0_window_mask].copy()
        L0_il = construct_il(L0_mempool, base_fee)

        # ===== L₋₁: IL containing ONLY censored transactions from N-1 =====
        prev_block = blocks_df[blocks_df['block_number'] == block_num - 1]
        L1_censored = pd.DataFrame()  # Initialize
        if len(prev_block) > 0:
            prev_ts = prev_block.iloc[0]['block_timestamp']
            prev_base_fee = int(prev_block.iloc[0]['base_fee'])

            # Flag censored transactions at block N-1
            L1_censored = flag_censored_transactions(
                mempool_df=mempool_df,
                block_timestamp=prev_ts,
                base_fee=prev_base_fee,
                replaced_txs=replaced_txs_set,
                config=config
            )

            # Construct L₋₁ with ONLY censored transactions
            L1_il = construct_censored_il(L1_censored, max_bytes=MAX_IL_BYTES)
        else:
            L1_il = pd.DataFrame()

        # ===== L₋₂: IL containing ONLY censored transactions from N-2 =====
        prev2_block = blocks_df[blocks_df['block_number'] == block_num - 2]
        L2_censored = pd.DataFrame()  # Initialize
        if len(prev2_block) > 0:
            prev2_ts = prev2_block.iloc[0]['block_timestamp']
            prev2_base_fee = int(prev2_block.iloc[0]['base_fee'])

            # Flag censored transactions at block N-2
            L2_censored = flag_censored_transactions(
                mempool_df=mempool_df,
                block_timestamp=prev2_ts,
                base_fee=prev2_base_fee,
                replaced_txs=replaced_txs_set,
                config=config
            )

            # Construct L₋₂ with ONLY censored transactions
            L2_il = construct_censored_il(L2_censored, max_bytes=MAX_IL_BYTES)
        else:
            L2_il = pd.DataFrame()

        # Calculate sizes
        L0_size = L0_il['tx_size'].sum() if len(L0_il) > 0 else 0
        L1_size = L1_il['tx_size'].sum() if len(L1_il) > 0 else 0
        L2_size = L2_il['tx_size'].sum() if len(L2_il) > 0 else 0

        # Calculate overlaps (transaction hashes in both lists)
        L0_hashes = set(L0_il['tx_hash']) if len(L0_il) > 0 else set()
        L1_hashes = set(L1_il['tx_hash']) if len(L1_il) > 0 else set()
        L2_hashes = set(L2_il['tx_hash']) if len(L2_il) > 0 else set()

        L0_L1_intersection = L0_hashes & L1_hashes
        L0_L2_intersection = L0_hashes & L2_hashes

        # Calculate overlap sizes
        if len(L0_il) > 0 and len(L0_L1_intersection) > 0:
            L0_L1_overlap_size = L0_il[L0_il['tx_hash'].isin(L0_L1_intersection)]['tx_size'].sum()
        else:
            L0_L1_overlap_size = 0

        if len(L0_il) > 0 and len(L0_L2_intersection) > 0:
            L0_L2_overlap_size = L0_il[L0_il['tx_hash'].isin(L0_L2_intersection)]['tx_size'].sum()
        else:
            L0_L2_overlap_size = 0

        # Bandwidth calculations
        # With 1-slot delay: validators already have L₋₁, so we only send L₀ \ L₋₁
        L1_effective_size = L0_size - L0_L1_overlap_size
        L1_bandwidth_saved = L0_L1_overlap_size

        # With 2-slot delay: validators already have L₋₂, so we only send L₀ \ L₋₂
        L2_effective_size = L0_size - L0_L2_overlap_size
        L2_bandwidth_saved = L0_L2_overlap_size

        results.append({
            'block_number': block_num,
            'block_timestamp': block_ts,
            'base_fee': base_fee,
            'included_tx_count': block['included_tx_count'],

            # L₀ metrics
            'L0_tx_count': len(L0_il),
            'L0_size_bytes': L0_size,

            # L₋₁ metrics
            'L1_tx_count': len(L1_il),
            'L1_size_bytes': L1_size,
            'L1_censored_tx_count': len(L1_censored),
            'L0_L1_intersection_count': len(L0_L1_intersection),
            'L0_L1_intersection_bytes': L0_L1_overlap_size,
            'L1_effective_bytes': L1_effective_size,
            'L1_bandwidth_saved': L1_bandwidth_saved,
            'L1_savings_pct': (L1_bandwidth_saved / L0_size * 100) if L0_size > 0 else 0,

            # L₋₂ metrics
            'L2_tx_count': len(L2_il),
            'L2_size_bytes': L2_size,
            'L2_censored_tx_count': len(L2_censored),
            'L0_L2_intersection_count': len(L0_L2_intersection),
            'L0_L2_intersection_bytes': L0_L2_overlap_size,
            'L2_effective_bytes': L2_effective_size,
            'L2_bandwidth_saved': L2_bandwidth_saved,
            'L2_savings_pct': (L2_bandwidth_saved / L0_size * 100) if L0_size > 0 else 0,
        })

    return pd.DataFrame(results)


def main():
    config = load_config()

    print("="*70)
    print("FOCIL CENSORSHIP ANALYSIS")
    print("="*70)
    print(f"Inclusion list size cap: {MAX_IL_BYTES:,} bytes ({MAX_IL_BYTES/1024:.1f} KiB)")
    print(f"Time window: {config['analysis']['time_window_start_secs']} to {config['analysis']['time_window_end_secs']} seconds")

    # Load block range and batch size from configuration
    start_block = config['analysis']['start_block']
    end_block = config['analysis']['end_block']
    batch_size = config['analysis'].get('batch_size_blocks', 100)

    all_results = []

    for batch_start in range(start_block, end_block, batch_size):
        batch_end = min(batch_start + batch_size, end_block)
        result = analyze_block_range(batch_start, batch_end, config)
        if result is not None:
            all_results.append(result)

    # Combine results
    df = pd.concat(all_results, ignore_index=True)

    # Remove first 2 blocks (no L₋₂ data)
    df = df[df['block_number'] > df['block_number'].min() + 2]

    # Save results
    results_dir = Path(__file__).parent.parent / "results"
    output_file = results_dir / "focil_censorship_analysis.parquet"
    df.to_parquet(output_file, index=False)
    print(f"\nResults saved to: {output_file}")

    # Print summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    print(f"\nBlocks analyzed: {len(df):,}")

    print(f"\n## L₀ (highest-fee transactions)")
    print(f"Average size: {df['L0_size_bytes'].mean()/1024:.2f} KiB")
    print(f"Average transaction count: {df['L0_tx_count'].mean():.1f}")

    print(f"\n## L₋₁ (censored transactions from N-1)")
    print(f"Total censored transactions detected: {df['L1_censored_tx_count'].mean():.1f}")
    print(f"Average inclusion list size: {df['L1_size_bytes'].mean()/1024:.2f} KiB")
    print(f"Average transactions in IL: {df['L1_tx_count'].mean():.1f}")
    print(f"Overlap with L₀: {df['L0_L1_intersection_bytes'].mean()/1024:.2f} KiB ({df['L1_savings_pct'].mean():.1f}%)")
    print(f"Effective bandwidth: {df['L1_effective_bytes'].mean()/1024:.2f} KiB")
    print(f"Bandwidth saved: {df['L1_bandwidth_saved'].mean()/1024:.2f} KiB per block")

    print(f"\n## L₋₂ (censored transactions from N-2)")
    print(f"Total censored transactions detected: {df['L2_censored_tx_count'].mean():.1f}")
    print(f"Average inclusion list size: {df['L2_size_bytes'].mean()/1024:.2f} KiB")
    print(f"Average transactions in IL: {df['L2_tx_count'].mean():.1f}")
    print(f"Overlap with L₀: {df['L0_L2_intersection_bytes'].mean()/1024:.2f} KiB ({df['L2_savings_pct'].mean():.1f}%)")
    print(f"Effective bandwidth: {df['L2_effective_bytes'].mean()/1024:.2f} KiB")
    print(f"Bandwidth saved: {df['L2_bandwidth_saved'].mean()/1024:.2f} KiB per block")

    print(f"\n## Censorship Detection Summary")
    print(f"Average censored transactions (N-1): {df['L1_censored_tx_count'].mean():.1f}")
    print(f"Average censored transactions (N-2): {df['L2_censored_tx_count'].mean():.1f}")
    print(f"Blocks with censorship (N-1): {(df['L1_censored_tx_count'] > 0).sum():,} ({(df['L1_censored_tx_count'] > 0).sum() / len(df) * 100:.1f}%)")
    print(f"Blocks with censorship (N-2): {(df['L2_censored_tx_count'] > 0).sum():,} ({(df['L2_censored_tx_count'] > 0).sum() / len(df) * 100:.1f}%)")

    print(f"\n## Annual Bandwidth (per validator)")
    blocks_per_year = 7200 * 365
    L0_annual = df['L0_size_bytes'].mean() * blocks_per_year / (1024**3)
    L1_annual = df['L1_effective_bytes'].mean() * blocks_per_year / (1024**3)
    L2_annual = df['L2_effective_bytes'].mean() * blocks_per_year / (1024**3)

    print(f"L₀: {L0_annual:.2f} GB/year")
    print(f"L₋₁: {L1_annual:.2f} GB/year (saves {L0_annual - L1_annual:.2f} GB)")
    print(f"L₋₂: {L2_annual:.2f} GB/year (saves {L0_annual - L2_annual:.2f} GB)")

    print("\n" + "="*70)


if __name__ == "__main__":
    main()

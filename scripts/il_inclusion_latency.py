#!/usr/bin/env python3
"""
IL Inclusion Latency Analysis

For each transaction placed in either IL (Top Fee or Censored), tracks how many
blocks it takes to eventually be included on-chain, or whether it is never
included within the observation window.

Methodology:
  - ILs are built using identical logic to focil_censorship_analysis.py
  - For each IL tx, we look forward up to MAX_LATENCY_BLOCKS (default 20) to
    find the first block that contains it
  - Txs not found in that window are marked as "never included"

Output:
  - results/il_latency_raw.parquet: one row per (block, strategy, tx_hash)
  - Console: CDF and summary statistics by strategy

Usage:
  python scripts/il_inclusion_latency.py [--start BLOCK] [--end BLOCK]
                                          [--max-latency N] [--out FILE]
"""

import argparse
import logging
import sys

import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm

# Ensure the scripts/ directory is on the path when this script is run from
# any working directory (e.g., project root via `python scripts/il_inclusion_latency.py`)
sys.path.insert(0, str(Path(__file__).parent))

from utils import (
    load_config, fetch_block_data, fetch_included_txs,
    check_addresses_on_chain, AddressCache, execute_query, get_block_int,
)
from focil_censorship_analysis import (
    construct_il, flag_censored_transactions, detect_nonce_replacements,
)

log = logging.getLogger(__name__)

DEFAULT_MAX_LATENCY = 20  # blocks to look forward


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def analyze_latency(
    start_block: int,
    end_block: int,
    config: dict,
    max_latency_blocks: int = DEFAULT_MAX_LATENCY,
    address_cache: AddressCache | None = None,
) -> tuple[pd.DataFrame, AddressCache]:
    """Build ILs for each block in [start_block, end_block) and track how long
    each IL tx takes to be included.

    Returns (records_df, address_cache) where records_df has columns:
      block_number, strategy, tx_hash, included_at_block,
      inclusion_delay_blocks, tx_size, effective_priority_fee
    """
    log.info("Latency analysis: blocks %d to %d (forward window: %d blocks)",
             start_block, end_block, max_latency_blocks)

    lookback = 3
    fetch_end = end_block + max_latency_blocks

    log.info("Fetching block data (%d to %d)...", start_block - lookback, fetch_end)
    blocks_df = fetch_block_data(start_block - lookback, fetch_end, config)
    if len(blocks_df) == 0:
        log.warning("No blocks found")
        return pd.DataFrame(), address_cache

    # Mempool time window
    tf_start = config['analysis']['topfee_window_start_secs']
    tf_end   = config['analysis']['topfee_window_end_secs']
    cs_start = config['analysis']['censored_window_start_secs']
    cs_end   = config['analysis']['censored_window_end_secs']

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
      AND gas_fee_cap IS NOT NULL
    ORDER BY event_date_time
    """
    mempool_df = execute_query(mempool_query, config)
    mempool_df['gas_limit'] = pd.to_numeric(mempool_df['gas_limit'], errors='coerce')
    log.info("Got %d mempool rows", len(mempool_df))

    log.info("Fetching included txs (%d to %d)...", start_block - lookback, fetch_end)
    included_txs_map = fetch_included_txs(start_block - lookback, fetch_end, config)

    replaced_txs = detect_nonce_replacements(mempool_df, included_txs_map)
    log.info("Found %d replaced transactions", len(replaced_txs))

    # Active sender filter
    if address_cache is None:
        address_cache = AddressCache()
    included_senders = set(
        mempool_df[mempool_df['tx_hash'].isin(
            {h for txs in included_txs_map.values() for h in txs}
        )]['sender'].dropna().str.lower().unique()
    )
    address_cache.add_active(included_senders)
    all_senders = set(mempool_df['sender'].dropna().str.lower().unique())
    onchain_active = check_addresses_on_chain(
        all_senders, start_block, config, cache=address_cache,
    )

    # Drop blocks with base_fee == 0 (NULL in source)
    blocks_df['base_fee'] = pd.to_numeric(blocks_df['base_fee'], errors='coerce')
    blocks_df = blocks_df[blocks_df['base_fee'].notna() & (blocks_df['base_fee'] > 0)]

    main_blocks = blocks_df[
        (blocks_df['block_number'] >= start_block) &
        (blocks_df['block_number'] < end_block)
    ]

    records = []

    for _, block_row in tqdm(main_blocks.iterrows(), total=len(main_blocks), desc="  Processing"):
        block_num = int(block_row['block_number'])
        block_ts  = int(block_row['block_timestamp'])
        base_fee  = int(block_row['base_fee'])
        gas_used  = get_block_int(block_row, 'gas_used')
        gas_limit = get_block_int(block_row, 'gas_limit')

        # Already included at or before block N
        already_included = set()
        for bn, txs in included_txs_map.items():
            if bn <= block_num:
                already_included |= txs

        # Active senders (same logic as main analysis)
        all_included_before = set()
        for bn, txs in included_txs_map.items():
            if bn < block_num:
                all_included_before |= txs
        senders_via_inclusion = set(
            mempool_df[mempool_df['tx_hash'].isin(all_included_before)
                      ]['sender'].dropna().unique()
        )
        active_senders = senders_via_inclusion | {
            s for s in mempool_df['sender'].dropna().unique()
            if s.lower() in onchain_active
        }

        # Censored transactions for this block
        censored_txs = None
        prev_blk = blocks_df[blocks_df['block_number'] == block_num - 1]
        if len(prev_blk) > 0:
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

        for strategy in ('topfee', 'censored'):
            il_df = construct_il(
                mempool_df=mempool_df,
                strategy=strategy,
                block_ts=block_ts,
                base_fee=base_fee,
                censored_txs=censored_txs,
                already_included=already_included,
                active_senders=active_senders,
                config=config,
            )

            if len(il_df) == 0:
                continue

            # Build a hash->first_included_block map for the forward window.
            # For each future block, mark hashes not yet assigned.
            il_hashes = set(il_df['tx_hash'])
            hash_to_block: dict[str, int] = {}
            remaining = il_hashes.copy()
            for lookahead in range(1, max_latency_blocks + 1):
                if not remaining:
                    break
                found = remaining & included_txs_map.get(block_num + lookahead, set())
                for h in found:
                    hash_to_block[h] = block_num + lookahead
                remaining -= found

            for _, tx_row in il_df.iterrows():
                tx_hash = tx_row['tx_hash']
                included_at = hash_to_block.get(tx_hash)
                records.append({
                    'block_number':           block_num,
                    'strategy':               strategy,
                    'tx_hash':                tx_hash,
                    'included_at_block':      included_at,
                    'inclusion_delay_blocks': (included_at - block_num) if included_at is not None else None,
                    'tx_size':                int(tx_row.get('tx_size', 0) or 0),
                    'effective_priority_fee': float(tx_row.get('effective_priority_fee', 0) or 0),
                })

    df = pd.DataFrame(records)
    return df, address_cache


# ---------------------------------------------------------------------------
# Summary printing
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame, max_latency_blocks: int):
    print("\n" + "=" * 65)
    print("IL INCLUSION LATENCY SUMMARY")
    print("=" * 65)

    if len(df) == 0:
        print("No data.")
        return

    # Deduplicate: same tx can appear in ILs for multiple blocks (rolling window)
    # For latency purposes treat each (block_number, strategy, tx_hash) as one obs.
    df = df.drop_duplicates(subset=['block_number', 'strategy', 'tx_hash'])

    for strategy in ('topfee', 'censored'):
        label = "Top Fee" if strategy == 'topfee' else "Censored"
        sub = df[df['strategy'] == strategy]
        if len(sub) == 0:
            continue

        n_total   = len(sub)
        n_included = sub['included_at_block'].notna().sum()
        n_never    = n_total - n_included
        pct_never  = n_never / n_total * 100

        print(f"\n### {label} IL")
        print(f"  Total IL tx-slot pairs : {n_total:,}")
        print(f"  Included within {max_latency_blocks} blocks: {n_included:,} ({n_included / n_total * 100:.1f}%)")
        print(f"  Never included          : {n_never:,} ({pct_never:.1f}%)")

        if n_included > 0:
            delays = sub['inclusion_delay_blocks'].dropna().astype(int)
            print(f"\n  Inclusion delay distribution (included txs only):")
            for d in range(1, min(max_latency_blocks + 1, 11)):
                cnt = (delays <= d).sum()
                pct = cnt / n_included * 100
                bar = "#" * int(pct / 2)
                print(f"    <= {d:2d} blocks: {cnt:5d} ({pct:5.1f}%)  {bar}")
            if max_latency_blocks > 10:
                for d in [15, 20]:
                    if d <= max_latency_blocks:
                        cnt = (delays <= d).sum()
                        pct = cnt / n_included * 100
                        bar = "#" * int(pct / 2)
                        print(f"    <= {d:2d} blocks: {cnt:5d} ({pct:5.1f}%)  {bar}")

            print(f"\n  Percentiles (blocks to inclusion):")
            for p in [25, 50, 75, 90, 95, 99]:
                print(f"    p{p:3d}: {np.percentile(delays, p):.1f} blocks")
            print(f"    mean: {delays.mean():.2f} blocks")
            print(f"    max:  {delays.max()} blocks")

    # Cross-strategy overlap: how many txs appear in BOTH ILs for the same block?
    topfee_set   = set(df[df['strategy'] == 'topfee' ].apply(lambda r: (r['block_number'], r['tx_hash']), axis=1))
    censored_set = set(df[df['strategy'] == 'censored'].apply(lambda r: (r['block_number'], r['tx_hash']), axis=1))
    overlap = topfee_set & censored_set
    if topfee_set and censored_set:
        print(f"\n### IL Overlap")
        print(f"  (block, tx) pairs in both ILs: {len(overlap):,} / "
              f"{len(topfee_set | censored_set):,} unique "
              f"({len(overlap) / len(topfee_set | censored_set) * 100:.1f}%)")

    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
        datefmt='%H:%M:%S',
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start',       type=int,  help='Start block (overrides config)')
    parser.add_argument('--end',         type=int,  help='End block (overrides config)')
    parser.add_argument('--max-latency', type=int,  default=DEFAULT_MAX_LATENCY,
                        help=f'Max blocks to look forward (default {DEFAULT_MAX_LATENCY})')
    parser.add_argument('--out',         type=str,  default=None,
                        help='Output parquet path (default: results/il_latency_raw.parquet)')
    parser.add_argument('--batch-size',  type=int,  default=None,
                        help='Blocks per batch (default: from config)')
    args = parser.parse_args()

    config = load_config()

    start_block  = args.start or config['analysis']['start_block']
    end_block    = args.end   or config['analysis']['end_block']
    batch_size   = args.batch_size or config['analysis'].get('batch_size_blocks', 100)
    max_latency  = args.max_latency
    output_path  = args.out or str(
        Path(__file__).parent.parent / "results" / "il_latency_raw.parquet"
    )

    print("=" * 65)
    print("IL INCLUSION LATENCY ANALYSIS")
    print("=" * 65)
    print(f"Block range   : {start_block:,} – {end_block:,}  ({end_block - start_block:,} blocks)")
    print(f"Forward window: {max_latency} blocks")
    print(f"Output        : {output_path}")

    all_records = []
    address_cache = AddressCache()

    for batch_start in range(start_block, end_block, batch_size):
        batch_end = min(batch_start + batch_size, end_block)
        log.info("Batch %d-%d", batch_start, batch_end)
        df_batch, address_cache = analyze_latency(
            batch_start, batch_end, config,
            max_latency_blocks=max_latency,
            address_cache=address_cache,
        )
        if df_batch is not None and len(df_batch) > 0:
            all_records.append(df_batch)

    if not all_records:
        log.error("No results produced.")
        return

    df = pd.concat(all_records, ignore_index=True)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    print(f"\nSaved {len(df):,} rows to {out}")

    print_summary(df, max_latency)


if __name__ == "__main__":
    main()

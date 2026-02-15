#!/usr/bin/env python3
"""
Shared utilities for FOCIL analysis scripts.

Provides config loading, ClickHouse query execution, common data fetching,
and IL packing logic used across all analysis scripts.
"""

import io
import logging
import os
import re
import time

import pandas as pd
import requests
import yaml
from pathlib import Path

log = logging.getLogger(__name__)

# EIP-7805 Constants
MAX_IL_BYTES = 8192  # 8 KiB
BLOB_TX_TYPE = 3     # EIP-4844
TX_OVERHEAD_BYTES = 125  # Signature + nonce + gas fields + addresses (for BN size estimation)

VARIANT_NAMES = [
    '0delay_topfee', '0delay_censored',
    '1delay_topfee', '1delay_censored',
    '2delay_topfee', '2delay_censored',
]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> dict:
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


# ---------------------------------------------------------------------------
# ClickHouse query execution
# ---------------------------------------------------------------------------

def execute_query(query: str, config: dict, max_retries: int = 3) -> pd.DataFrame:
    """Execute ClickHouse query via HTTP with retry logic.

    Returns a DataFrame parsed from CSVWithNames output.
    """
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
            return pd.read_csv(io.StringIO(response.text), na_values=['\\N'])
        except (requests.RequestException, RuntimeError) as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                log.warning("Query failed (attempt %d/%d), retrying in %ds: %s",
                            attempt + 1, max_retries, wait, exc)
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# Common data fetching
# ---------------------------------------------------------------------------

def fetch_block_data(start_block: int, end_block: int, config: dict) -> pd.DataFrame:
    """Fetch block metadata from canonical_beacon_block."""
    query = f"""
    SELECT
        execution_payload_block_number as block_number,
        toUnixTimestamp(slot_start_date_time) as block_timestamp,
        toUInt256(execution_payload_base_fee_per_gas) as base_fee,
        execution_payload_transactions_count as included_tx_count,
        toUInt256(execution_payload_gas_used) as gas_used,
        toUInt256(execution_payload_gas_limit) as gas_limit
    FROM canonical_beacon_block
    WHERE execution_payload_block_number >= {start_block}
      AND execution_payload_block_number < {end_block}
    ORDER BY execution_payload_block_number
    """
    df = execute_query(query, config)
    df['gas_used'] = pd.to_numeric(df['gas_used'], errors='coerce')
    df['gas_limit'] = pd.to_numeric(df['gas_limit'], errors='coerce')
    return df


def fetch_included_txs(start_block: int, end_block: int, config: dict) -> dict:
    """Fetch included transactions for cross-validation.

    Returns dict: block_number -> set(tx_hashes).
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

    log.info("Got included txs for %d blocks (%d total)", len(result), len(df))
    return result


class AddressCache:
    """Persistent cache of on-chain address lookup results.

    Tracks which addresses have been checked and which are active,
    so subsequent batches only query genuinely new addresses.
    Also absorbs senders from included transactions automatically.
    """

    def __init__(self):
        self.active: set = set()    # addresses confirmed on-chain
        self.checked: set = set()   # all addresses we've ever looked up

    def add_active(self, addresses):
        """Mark addresses as known-active (e.g. from included txs)."""
        self.active |= set(addresses)
        self.checked |= set(addresses)

    def filter_active(self, addresses: set) -> set:
        """Return the subset of addresses that are known-active."""
        return addresses & self.active

    def unchecked(self, addresses: set) -> set:
        """Return addresses we haven't looked up yet."""
        return addresses - self.checked


def check_addresses_on_chain(addresses: set, ref_block: int, config: dict,
                             cache: AddressCache | None = None,
                             lookback_blocks: int = 100_000) -> set:
    """Check which addresses have appeared on-chain.

    Uses canonical_execution_address_appearances with a block-range filter
    (address columns aren't in the sort key, so full-chain scans timeout).

    When a cache is provided, only queries addresses not previously checked.
    Results are stored in the cache so later batches benefit from earlier
    lookups — over a full study run this effectively builds up complete
    coverage without needing a single massive query.

    Returns the full set of active addresses (cached + newly discovered).
    """
    if cache is None:
        cache = AddressCache()

    if not addresses:
        return cache.filter_active(addresses)

    # Only query addresses we haven't seen before
    new_addrs = cache.unchecked(addresses)
    already_known_active = cache.filter_active(addresses)

    if not new_addrs:
        log.info("All %d addresses already cached (%d active)",
                 len(addresses), len(already_known_active))
        return already_known_active

    start_block = max(0, ref_block - lookback_blocks)
    chunk_size = 1000
    addr_list = list(new_addrs)
    newly_active = set()

    log.info("Checking %d new addresses against on-chain appearances "
             "(blocks %d-%d, %dk lookback) [%d cached, %d already active]...",
             len(addr_list), start_block, ref_block, lookback_blocks // 1000,
             len(cache.checked), len(already_known_active))

    for i in range(0, len(addr_list), chunk_size):
        chunk = addr_list[i:i + chunk_size]
        # Validate hex addresses to prevent SQL injection
        safe = [a for a in chunk if re.fullmatch(r'0x[0-9a-fA-F]{40}', a)]
        if not safe:
            continue
        in_clause = ", ".join(f"'{a}'" for a in safe)
        query = f"""
        SELECT DISTINCT address
        FROM canonical_execution_address_appearances
        WHERE block_number >= {start_block}
          AND block_number < {ref_block}
          AND address IN ({in_clause})
          AND relationship = 'tx_from'
        """
        df = execute_query(query, config)
        if len(df) > 0:
            newly_active |= set(df['address'])
        log.info("  Chunk %d/%d: %d newly active",
                 i // chunk_size + 1,
                 (len(addr_list) + chunk_size - 1) // chunk_size,
                 len(newly_active))

    # Update cache
    cache.checked |= new_addrs
    cache.active |= newly_active

    total_active = already_known_active | newly_active
    log.info("Found %d / %d new addresses active; %d / %d total active "
             "(cache now: %d checked, %d active)",
             len(newly_active), len(new_addrs),
             len(total_active), len(addresses),
             len(cache.checked), len(cache.active))
    return total_active


# ---------------------------------------------------------------------------
# IL packing
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_block_int(block_row, col: str) -> int:
    """Safely extract an integer from a block row, defaulting to 0."""
    val = block_row.get(col, 0) if hasattr(block_row, 'get') else block_row[col]
    try:
        return int(val) if pd.notna(val) else 0
    except (ValueError, TypeError):
        return 0

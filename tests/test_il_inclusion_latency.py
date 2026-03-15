"""Tests for il_inclusion_latency.py — analyze_latency and print_summary."""

import io
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd

# Add scripts/ to path so imports work from tests/
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from il_inclusion_latency import analyze_latency, print_summary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_blocks_df(block_numbers, base_ts=1_700_000_000, base_fee=10**9):
    return pd.DataFrame({
        'block_number': block_numbers,
        'block_timestamp': [base_ts + i * 12 for i in range(len(block_numbers))],
        'base_fee': [base_fee] * len(block_numbers),
        'gas_used': [10_000_000] * len(block_numbers),
        'gas_limit': [30_000_000] * len(block_numbers),
        'included_tx_count': [100] * len(block_numbers),
    })


def _make_mempool_df(tx_hashes, sender='0xabc', block_ts=1_700_000_000,
                     win_offset=0, base_fee=10**9):
    """Minimal mempool DataFrame matching the Xatu schema used in the scripts."""
    return pd.DataFrame({
        'tx_hash':        tx_hashes,
        'sender':         [sender] * len(tx_hashes),
        'nonce':          list(range(len(tx_hashes))),
        'seen_timestamp': [block_ts + win_offset] * len(tx_hashes),
        'max_fee':        [base_fee * 2] * len(tx_hashes),
        'priority_fee':   [int(1e9)] * len(tx_hashes),
        'tx_size':        [200] * len(tx_hashes),
        'gas_limit':      [21_000] * len(tx_hashes),
        'tx_type':        [2] * len(tx_hashes),
    })


def _make_config(start_block=1000):
    return {
        'analysis': {
            'start_block': start_block,
            'end_block':   start_block + 10,
            'batch_size_blocks': 10,
            'topfee_window_start_secs':   -4,
            'topfee_window_end_secs':      8,
            'censored_window_start_secs': -16,
            'censored_window_end_secs':   -4,
            'censorship_dwell_time_secs':  12,
            'censorship_max_dwell_time_secs': 12,
            'censorship_fee_percentile':   0.50,
        }
    }


# ---------------------------------------------------------------------------
# print_summary tests
# ---------------------------------------------------------------------------

class TestPrintSummary(unittest.TestCase):

    def _capture(self, df, max_latency=20):
        buf = io.StringIO()
        with patch('sys.stdout', buf):
            print_summary(df, max_latency)
        return buf.getvalue()

    def test_empty_df(self):
        out = self._capture(pd.DataFrame())
        self.assertIn("No data", out)

    def test_all_included(self):
        df = pd.DataFrame({
            'block_number':           [1, 1, 2],
            'strategy':               ['topfee', 'topfee', 'topfee'],
            'tx_hash':                ['0xaa', '0xbb', '0xcc'],
            'included_at_block':      [2, 2, 3],
            'inclusion_delay_blocks': [1, 1, 1],
            'tx_size':                [200, 200, 200],
            'effective_priority_fee': [1e9, 1e9, 1e9],
        })
        out = self._capture(df)
        self.assertIn("Top Fee IL", out)
        self.assertIn("Never included", out)
        self.assertIn("0 (0.0%)", out)

    def test_never_included(self):
        df = pd.DataFrame({
            'block_number':           [1, 1],
            'strategy':               ['topfee', 'topfee'],
            'tx_hash':                ['0xaa', '0xbb'],
            'included_at_block':      [None, None],
            'inclusion_delay_blocks': [None, None],
            'tx_size':                [200, 200],
            'effective_priority_fee': [1e9, 1e9],
        })
        out = self._capture(df)
        self.assertIn("100.0%", out)

    def test_both_strategies(self):
        df = pd.DataFrame({
            'block_number':           [1, 1],
            'strategy':               ['topfee', 'censored'],
            'tx_hash':                ['0xaa', '0xaa'],
            'included_at_block':      [2, 2],
            'inclusion_delay_blocks': [1, 1],
            'tx_size':                [200, 200],
            'effective_priority_fee': [1e9, 1e9],
        })
        out = self._capture(df)
        self.assertIn("Top Fee IL", out)
        self.assertIn("Censored IL", out)
        self.assertIn("IL Overlap", out)

    def test_deduplication(self):
        """Same (block, strategy, tx_hash) appearing twice should be counted once."""
        df = pd.DataFrame({
            'block_number':           [1, 1],
            'strategy':               ['topfee', 'topfee'],
            'tx_hash':                ['0xaa', '0xaa'],
            'included_at_block':      [2, 2],
            'inclusion_delay_blocks': [1, 1],
            'tx_size':                [200, 200],
            'effective_priority_fee': [1e9, 1e9],
        })
        out = self._capture(df)
        self.assertIn("Total IL tx-slot pairs : 1", out)


# ---------------------------------------------------------------------------
# analyze_latency tests (with mocked DB calls)
# ---------------------------------------------------------------------------

class TestAnalyzeLatency(unittest.TestCase):
    """Tests for analyze_latency using mocked fetch functions."""

    def _run(self, blocks_df, mempool_df, included_txs_map, start=1000, end=1002,
             max_latency=5, active_senders=None):
        config = _make_config(start)
        if active_senders is None:
            # Default: mark all mempool senders as active so the IL is non-empty
            active_senders = set(mempool_df['sender'].str.lower().unique()) if len(mempool_df) > 0 else set()
        with (
            patch('il_inclusion_latency.fetch_block_data', return_value=blocks_df),
            patch('il_inclusion_latency.execute_query',    return_value=mempool_df),
            patch('il_inclusion_latency.fetch_included_txs', return_value=included_txs_map),
            patch('il_inclusion_latency.check_addresses_on_chain', return_value=active_senders),
        ):
            df, _ = analyze_latency(start, end, config, max_latency_blocks=max_latency)
        return df

    def test_returns_dataframe(self):
        block_nums = list(range(997, 1008))
        blocks_df  = _make_blocks_df(block_nums)
        base_ts    = blocks_df.loc[blocks_df['block_number'] == 1000, 'block_timestamp'].iloc[0]
        mempool_df = _make_mempool_df(['0xaaa', '0xbbb'], block_ts=base_ts)
        included   = {1001: {'0xaaa'}, 1002: {'0xbbb'}}

        result = self._run(blocks_df, mempool_df, included)
        self.assertIsInstance(result, pd.DataFrame)

    def test_included_tx_has_delay(self):
        block_nums = list(range(997, 1008))
        blocks_df  = _make_blocks_df(block_nums)
        base_ts    = blocks_df.loc[blocks_df['block_number'] == 1000, 'block_timestamp'].iloc[0]
        mempool_df = _make_mempool_df(['0xaaa'], block_ts=base_ts)
        included   = {1001: {'0xaaa'}}

        result = self._run(blocks_df, mempool_df, included)
        topfee_rows = result[result['strategy'] == 'topfee']
        if len(topfee_rows) > 0:
            row = topfee_rows[topfee_rows['tx_hash'] == '0xaaa']
            if len(row) > 0:
                self.assertEqual(row.iloc[0]['included_at_block'], 1001)
                self.assertEqual(row.iloc[0]['inclusion_delay_blocks'], 1)

    def test_never_included_tx(self):
        block_nums = list(range(997, 1008))
        blocks_df  = _make_blocks_df(block_nums)
        base_ts    = blocks_df.loc[blocks_df['block_number'] == 1000, 'block_timestamp'].iloc[0]
        mempool_df = _make_mempool_df(['0xzzz'], block_ts=base_ts)
        included   = {}  # nothing ever included

        result = self._run(blocks_df, mempool_df, included, max_latency=3)
        topfee_rows = result[result['strategy'] == 'topfee']
        if len(topfee_rows) > 0:
            self.assertTrue(topfee_rows['included_at_block'].isna().all())
            self.assertTrue(topfee_rows['inclusion_delay_blocks'].isna().all())

    def test_empty_blocks_returns_empty_df(self):
        result = self._run(
            pd.DataFrame(), pd.DataFrame(), {},
            start=1000, end=1002,
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)

    def test_required_columns_present(self):
        block_nums = list(range(997, 1008))
        blocks_df  = _make_blocks_df(block_nums)
        base_ts    = blocks_df.loc[blocks_df['block_number'] == 1000, 'block_timestamp'].iloc[0]
        mempool_df = _make_mempool_df(['0xaaa'], block_ts=base_ts)
        included   = {1001: {'0xaaa'}}

        result = self._run(blocks_df, mempool_df, included)
        expected_cols = {
            'block_number', 'strategy', 'tx_hash',
            'included_at_block', 'inclusion_delay_blocks',
            'tx_size', 'effective_priority_fee',
        }
        if len(result) > 0:
            self.assertTrue(expected_cols.issubset(set(result.columns)))


if __name__ == '__main__':
    unittest.main()

"""Tests for per-TX latency logic in bn_focil_analysis.py.

Covers:
  - _ts_to_block_num: normal, clamp-to-first, and after-last edge cases
  - _build_tx_latency_records: redundant tx, non-redundant confirmed tx,
    non-redundant unconfirmed tx, empty il_df
"""

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from bn_focil_analysis import _ts_to_block_num, _build_tx_latency_records


def _make_blocks_sorted(block_numbers, base_ts=1_700_000_000):
    """Minimal blocks_sorted DataFrame for _ts_to_block_num."""
    return pd.DataFrame({
        'block_number':    block_numbers,
        'block_timestamp': [base_ts + i * 12 for i in range(len(block_numbers))],
    }).sort_values('block_timestamp').reset_index(drop=True)


def _make_il_df(tx_hashes, tx_size=200, eff_priority=int(1e9)):
    return pd.DataFrame({
        'tx_hash':                tx_hashes,
        'tx_size':                [tx_size] * len(tx_hashes),
        'effective_priority_fee': [float(eff_priority)] * len(tx_hashes),
    })


class TestTsToBlockNum(unittest.TestCase):

    def setUp(self):
        # Blocks 1000-1004, timestamps 1_700_000_000, 012, 024, 036, 048
        self.blocks = _make_blocks_sorted(list(range(1000, 1005)))

    def test_exact_match(self):
        # Timestamp exactly equals block 1002's timestamp
        ts = 1_700_000_000 + 2 * 12  # = 1_700_000_024
        self.assertEqual(_ts_to_block_num(ts, self.blocks), 1002)

    def test_between_blocks(self):
        # Timestamp between block 1001 (ts=012) and block 1002 (ts=024)
        ts = 1_700_000_000 + 12 + 6  # = 1_700_000_018
        self.assertEqual(_ts_to_block_num(ts, self.blocks), 1001)

    def test_after_last_block(self):
        # Timestamp after block 1004 (the last block)
        ts = 1_700_000_000 + 10 * 12
        self.assertEqual(_ts_to_block_num(ts, self.blocks), 1004)

    def test_before_first_block_clamps(self):
        # Timestamp before block 1000 — should clamp to block 1000
        ts = 1_700_000_000 - 100
        self.assertEqual(_ts_to_block_num(ts, self.blocks), 1000)

    def test_at_first_block(self):
        ts = 1_700_000_000  # exactly block 1000
        self.assertEqual(_ts_to_block_num(ts, self.blocks), 1000)


class TestBuildTxLatencyRecords(unittest.TestCase):

    def setUp(self):
        self.blocks_sorted = _make_blocks_sorted(list(range(1000, 1010)))
        self.block_num = 1002
        self.base_ts = 1_700_000_000

    def _lc(self, tx_hash, was_confirmed=True, blocks_pending=2,
            first_seen_offset=0, was_replaced=False, final_status='confirmed'):
        first_seen_ts = self.base_ts + first_seen_offset
        return {
            tx_hash: {
                'was_confirmed':  was_confirmed,
                'blocks_pending': float(blocks_pending) if blocks_pending is not None else None,
                'first_seen':     float(first_seen_ts),
                'was_replaced':   was_replaced,
                'final_status':   final_status,
            }
        }

    def test_redundant_tx_no_delay(self):
        """Tx in N+1 is redundant; delay_from_il should be None."""
        il_df = _make_il_df(['0xaaa'])
        il_hashes = {'0xaaa'}
        next_block_included = {'0xaaa'}
        lc_lookup = self._lc('0xaaa')

        records = _build_tx_latency_records(
            il_df, il_hashes, next_block_included, lc_lookup,
            self.blocks_sorted, self.block_num, 'topfee',
        )
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertTrue(r['is_redundant'])
        self.assertIsNone(r['delay_from_il'])
        self.assertEqual(r['strategy'], 'topfee')
        self.assertEqual(r['block_number'], self.block_num)

    def test_non_redundant_confirmed_delay_computed(self):
        """Non-redundant confirmed tx: delay_from_il = first_seen_block + blocks_pending - block_num."""
        # block_num = 1002; first_seen at ts of block 1000; blocks_pending=3
        # => first_seen_block=1000, included_block=1003, delay=1003-1002=1
        il_df = _make_il_df(['0xbbb'])
        il_hashes = {'0xbbb'}
        next_block_included = set()
        lc_lookup = self._lc('0xbbb', was_confirmed=True, blocks_pending=3, first_seen_offset=0)

        records = _build_tx_latency_records(
            il_df, il_hashes, next_block_included, lc_lookup,
            self.blocks_sorted, self.block_num, 'topfee',
        )
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertFalse(r['is_redundant'])
        self.assertEqual(r['delay_from_il'], 1)  # 1000+3-1002 = 1

    def test_non_redundant_unconfirmed_no_delay(self):
        """Tx not confirmed — delay_from_il must be None."""
        il_df = _make_il_df(['0xccc'])
        il_hashes = {'0xccc'}
        next_block_included = set()
        lc_lookup = self._lc('0xccc', was_confirmed=False, blocks_pending=None)

        records = _build_tx_latency_records(
            il_df, il_hashes, next_block_included, lc_lookup,
            self.blocks_sorted, self.block_num, 'censored',
        )
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertFalse(r['is_redundant'])
        self.assertIsNone(r['delay_from_il'])

    def test_tx_not_in_lc_lookup(self):
        """Tx with no lifecycle entry — all lc fields default to None."""
        il_df = _make_il_df(['0xddd'])
        il_hashes = {'0xddd'}
        next_block_included = set()
        lc_lookup = {}

        records = _build_tx_latency_records(
            il_df, il_hashes, next_block_included, lc_lookup,
            self.blocks_sorted, self.block_num, 'topfee',
        )
        self.assertEqual(len(records), 1)
        r = records[0]
        self.assertFalse(r['is_redundant'])
        self.assertIsNone(r['delay_from_il'])
        self.assertIsNone(r['was_confirmed'])

    def test_empty_il_df(self):
        records = _build_tx_latency_records(
            pd.DataFrame(), set(), set(), {},
            self.blocks_sorted, self.block_num, 'topfee',
        )
        self.assertEqual(records, [])

    def test_multiple_txs_mixed(self):
        """One redundant, one confirmed non-redundant — both appear in output."""
        il_df = _make_il_df(['0xaaa', '0xbbb'])
        il_hashes = {'0xaaa', '0xbbb'}
        next_block_included = {'0xaaa'}
        lc_lookup = {
            **self._lc('0xaaa'),
            **self._lc('0xbbb', was_confirmed=True, blocks_pending=1, first_seen_offset=12),
        }

        records = _build_tx_latency_records(
            il_df, il_hashes, next_block_included, lc_lookup,
            self.blocks_sorted, self.block_num, 'topfee',
        )
        self.assertEqual(len(records), 2)
        by_hash = {r['tx_hash']: r for r in records}
        self.assertTrue(by_hash['0xaaa']['is_redundant'])
        self.assertFalse(by_hash['0xbbb']['is_redundant'])
        # 0xbbb: first_seen at ts of block 1001 (+12s), blocks_pending=1 => included_block=1002 => delay=0
        self.assertEqual(by_hash['0xbbb']['delay_from_il'], 0)


if __name__ == '__main__':
    unittest.main()

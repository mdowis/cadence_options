"""Tests for cadence/trade_ledger.py."""

import json
import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.trade_ledger import TradeLedger
from cadence.position_tracker import TrackedPosition


def _tracked_position(**overrides):
    defaults = dict(
        tag="cadence-abc",
        symbol="SPY",
        expiration="2026-05-30",
        dte_at_entry=45,
        contracts=2,
        entry_credit=2.40,
        entry_credit_mid=2.50,
        entry_underlying_price=450.0,
        iv_rank_at_entry=42.5,
        entry_time=time.time() - 86400,  # 1 day ago
        short_put_symbol="SPY260530P00435000",
        long_put_symbol="SPY260530P00425000",
        short_call_symbol="SPY260530C00465000",
        long_call_symbol="SPY260530C00475000",
        short_put_strike=435,
        long_put_strike=425,
        short_call_strike=465,
        long_call_strike=475,
    )
    defaults.update(overrides)
    return TrackedPosition(**defaults)


class TestTradeLedgerRecord(unittest.TestCase):

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".jsonl")
        os.close(fd)
        os.unlink(self.path)
        self.ledger = TradeLedger(path=self.path)

    def tearDown(self):
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def test_record_writes_jsonl_line(self):
        t = _tracked_position()
        rec = self.ledger.record_close(
            t, pnl_cents=15000, close_debit=1.65,
            close_underlying_price=452.5, exit_reason="profit_target",
        )
        with open(self.path) as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 1)
        loaded = json.loads(lines[0])
        self.assertEqual(loaded["tag"], "cadence-abc")
        self.assertEqual(loaded["pnl_cents"], 15000)
        self.assertEqual(loaded["pnl_dollars"], 150.0)
        self.assertEqual(loaded["exit_reason"], "profit_target")
        self.assertTrue(loaded["win"])

    def test_appends_multiple_records(self):
        t = _tracked_position()
        self.ledger.record_close(t, pnl_cents=15000, exit_reason="profit_target")
        self.ledger.record_close(t, pnl_cents=-30000, exit_reason="loss_stop")
        records = self.ledger.read_all()
        self.assertEqual(len(records), 2)

    def test_captures_entry_context(self):
        t = _tracked_position()
        rec = self.ledger.record_close(t, pnl_cents=15000)
        self.assertEqual(rec["entry_credit"], 2.40)
        self.assertEqual(rec["entry_credit_mid"], 2.50)
        self.assertEqual(rec["entry_underlying_price"], 450.0)
        self.assertEqual(rec["iv_rank_at_entry"], 42.5)
        self.assertEqual(rec["dte_at_entry"], 45)

    def test_computes_underlying_move(self):
        t = _tracked_position(entry_underlying_price=450.0)
        rec = self.ledger.record_close(
            t, pnl_cents=15000, close_underlying_price=455.0)
        self.assertAlmostEqual(rec["underlying_move"], 5.0, places=2)
        self.assertAlmostEqual(rec["underlying_move_pct"], 5.0/450.0*100, places=3)

    def test_underlying_move_none_when_missing(self):
        t = _tracked_position(entry_underlying_price=None)
        rec = self.ledger.record_close(t, pnl_cents=15000,
                                        close_underlying_price=455.0)
        self.assertIsNone(rec["underlying_move"])

    def test_return_on_risk(self):
        # entry_credit=2.40, wing=10 -> max_loss_per_share=7.60
        # contracts=2 -> risk_dollars = 7.60*100*2 = $1520
        # pnl=$150 -> return = 150/1520*100 = 9.87%
        t = _tracked_position()
        rec = self.ledger.record_close(t, pnl_cents=15000)
        self.assertAlmostEqual(rec["return_on_risk_pct"], 150/1520*100, places=2)

    def test_duration(self):
        t = _tracked_position(entry_time=time.time() - 86400 * 3)  # 3 days
        rec = self.ledger.record_close(t, pnl_cents=15000)
        self.assertAlmostEqual(rec["duration_days"], 3.0, places=1)

    def test_loss_records_negative_pnl(self):
        t = _tracked_position()
        rec = self.ledger.record_close(t, pnl_cents=-50000,
                                        exit_reason="loss_stop")
        self.assertEqual(rec["pnl_dollars"], -500.0)
        self.assertFalse(rec["win"])


class TestTradeLedgerStats(unittest.TestCase):

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".jsonl")
        os.close(fd)
        os.unlink(self.path)
        self.ledger = TradeLedger(path=self.path)

    def tearDown(self):
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def test_empty_stats(self):
        stats = self.ledger.summary_stats()
        self.assertEqual(stats["n"], 0)
        self.assertEqual(stats["wins"], 0)
        self.assertEqual(stats["losses"], 0)
        self.assertEqual(stats["win_rate"], 0.0)
        self.assertEqual(stats["total_pnl"], 0.0)

    def test_aggregate_stats(self):
        t = _tracked_position()
        # 3 wins of $150, 1 loss of $400
        for _ in range(3):
            self.ledger.record_close(t, pnl_cents=15000,
                                      exit_reason="profit_target")
        self.ledger.record_close(t, pnl_cents=-40000,
                                  exit_reason="loss_stop")
        stats = self.ledger.summary_stats()
        self.assertEqual(stats["n"], 4)
        self.assertEqual(stats["wins"], 3)
        self.assertEqual(stats["losses"], 1)
        self.assertEqual(stats["win_rate"], 75.0)
        self.assertEqual(stats["avg_win"], 150.0)
        self.assertEqual(stats["avg_loss"], -400.0)
        self.assertEqual(stats["total_pnl"], 50.0)  # 3*150 - 400

    def test_by_exit_reason(self):
        t = _tracked_position()
        self.ledger.record_close(t, pnl_cents=10000,
                                  exit_reason="profit_target")
        self.ledger.record_close(t, pnl_cents=-20000,
                                  exit_reason="loss_stop")
        self.ledger.record_close(t, pnl_cents=5000,
                                  exit_reason="time_stop")
        stats = self.ledger.summary_stats()
        self.assertEqual(stats["by_exit_reason"]["profit_target"]["count"], 1)
        self.assertEqual(stats["by_exit_reason"]["profit_target"]["wins"], 1)
        self.assertEqual(stats["by_exit_reason"]["loss_stop"]["count"], 1)
        self.assertEqual(stats["by_exit_reason"]["loss_stop"]["wins"], 0)
        self.assertEqual(stats["by_exit_reason"]["time_stop"]["count"], 1)


class TestPurgeUnresolved(unittest.TestCase):

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".jsonl")
        os.close(fd)
        os.unlink(self.path)
        self.ledger = TradeLedger(path=self.path)

    def tearDown(self):
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def test_purge_removes_unresolved(self):
        t = _tracked_position()
        self.ledger.record_close(t, pnl_cents=10000,
                                  exit_reason="profit_target")
        self.ledger.record_close(t, pnl_cents=0, exit_reason="UNRESOLVED")
        self.ledger.record_close(t, pnl_cents=-5000,
                                  exit_reason="loss_stop")
        self.ledger.record_close(t, pnl_cents=0, exit_reason="UNRESOLVED")
        self.assertEqual(len(self.ledger.read_all()), 4)

        removed = self.ledger.purge_unresolved()

        self.assertEqual(removed, 2)
        remaining = self.ledger.read_all()
        self.assertEqual(len(remaining), 2)
        reasons = [r["exit_reason"] for r in remaining]
        self.assertIn("profit_target", reasons)
        self.assertIn("loss_stop", reasons)
        self.assertNotIn("UNRESOLVED", reasons)

    def test_purge_no_unresolved_is_noop(self):
        t = _tracked_position()
        self.ledger.record_close(t, pnl_cents=10000,
                                  exit_reason="profit_target")
        removed = self.ledger.purge_unresolved()
        self.assertEqual(removed, 0)
        self.assertEqual(len(self.ledger.read_all()), 1)

    def test_stats_exclude_unresolved_from_win_loss(self):
        """UNRESOLVED records do not inflate loss counts. They're
        reported separately in the 'unresolved' field."""
        t = _tracked_position()
        self.ledger.record_close(t, pnl_cents=10000,
                                  exit_reason="profit_target")
        self.ledger.record_close(t, pnl_cents=0, exit_reason="UNRESOLVED")
        self.ledger.record_close(t, pnl_cents=0, exit_reason="UNRESOLVED")
        stats = self.ledger.summary_stats()
        self.assertEqual(stats["n"], 1)  # only real trades
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["losses"], 0)
        self.assertEqual(stats["unresolved"], 2)
        self.assertEqual(stats["total_records"], 3)
        self.assertEqual(stats["win_rate"], 100.0)


class TestTradeLedgerNoPath(unittest.TestCase):
    """Ledger with path=None should not write but should still build records."""

    def test_no_path_no_write(self):
        ledger = TradeLedger(path=None)
        t = _tracked_position()
        rec = ledger.record_close(t, pnl_cents=15000)
        # Record returned; no file written
        self.assertEqual(rec["pnl_cents"], 15000)
        self.assertEqual(ledger.read_all(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

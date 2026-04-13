"""Tests for cadence/position_tracker.py."""

import json
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.position_tracker import (
    PositionTracker, TrackedPosition, _order_created_after,
)


class FakeCandidate:
    def __init__(self):
        self.symbol = "SPY"
        self.expiration = "2026-05-30"
        self.dte = 45
        self.short_put_symbol = "SPY260530P00435000"
        self.short_put_strike = 435
        self.long_put_symbol = "SPY260530P00425000"
        self.long_put_strike = 425
        self.short_call_symbol = "SPY260530C00465000"
        self.short_call_strike = 465
        self.long_call_symbol = "SPY260530C00475000"
        self.long_call_strike = 475
        self.credit = 2.40


class TestTrackedPosition(unittest.TestCase):

    def test_dict_round_trip(self):
        pos = TrackedPosition(
            tag="cadence-abc", symbol="SPY", expiration="2026-05-30",
            dte_at_entry=45, contracts=1, entry_credit=2.40,
            entry_time=1700000000.0,
            short_put_symbol="SPY260530P00435000",
            long_put_symbol="SPY260530P00425000",
            short_call_symbol="SPY260530C00465000",
            long_call_symbol="SPY260530C00475000",
            short_put_strike=435, long_put_strike=425,
            short_call_strike=465, long_call_strike=475,
        )
        d = pos.to_dict()
        restored = TrackedPosition.from_dict(d)
        self.assertEqual(restored.tag, "cadence-abc")
        self.assertEqual(restored.entry_credit, 2.40)

    def test_leg_symbols(self):
        pos = TrackedPosition(
            tag="t1", symbol="SPY", expiration="2026-05-30",
            dte_at_entry=45, contracts=1, entry_credit=2.40,
            entry_time=0,
            short_put_symbol="A", long_put_symbol="B",
            short_call_symbol="C", long_call_symbol="D",
            short_put_strike=1, long_put_strike=2,
            short_call_strike=3, long_call_strike=4,
        )
        self.assertEqual(pos.leg_symbols(), ("A", "B", "C", "D"))


class TestPositionTracker(unittest.TestCase):

    def setUp(self):
        fd, self.state_path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.unlink(self.state_path)  # start clean
        self.tracker = PositionTracker(state_file=self.state_path)

    def tearDown(self):
        try:
            os.unlink(self.state_path)
        except OSError:
            pass

    def test_record_entry(self):
        c = FakeCandidate()
        self.tracker.record_entry(c, tag="t1", contracts=1)
        open_pos = self.tracker.get_open()
        self.assertEqual(len(open_pos), 1)
        self.assertEqual(open_pos[0].tag, "t1")
        self.assertEqual(open_pos[0].entry_credit, 2.40)

    def test_persistence(self):
        c = FakeCandidate()
        self.tracker.record_entry(c, tag="t1")
        # New instance reads from disk
        t2 = PositionTracker(state_file=self.state_path)
        self.assertEqual(len(t2.get_open()), 1)
        self.assertEqual(t2.get_open()[0].tag, "t1")

    def test_detect_closes_all_legs_missing(self):
        c = FakeCandidate()
        self.tracker.record_entry(c, tag="t1")
        # Broker returns no positions
        closed = self.tracker.detect_closes([])
        self.assertEqual(len(closed), 1)
        self.assertEqual(closed[0].tag, "t1")

    def test_detect_closes_all_legs_present_not_closed(self):
        c = FakeCandidate()
        self.tracker.record_entry(c, tag="t1")
        broker = [
            {"symbol": "SPY260530P00435000"},
            {"symbol": "SPY260530P00425000"},
            {"symbol": "SPY260530C00465000"},
            {"symbol": "SPY260530C00475000"},
        ]
        closed = self.tracker.detect_closes(broker)
        self.assertEqual(len(closed), 0)

    def test_detect_closes_partial_leg_not_closed(self):
        """If ANY leg still appears at broker, position is not closed."""
        c = FakeCandidate()
        self.tracker.record_entry(c, tag="t1")
        broker = [{"symbol": "SPY260530P00435000"}]
        closed = self.tracker.detect_closes(broker)
        self.assertEqual(len(closed), 0)

    def test_remove(self):
        c = FakeCandidate()
        self.tracker.record_entry(c, tag="t1")
        self.assertEqual(len(self.tracker.get_open()), 1)
        self.tracker.remove("t1")
        self.assertEqual(len(self.tracker.get_open()), 0)


class TestComputeRealizedPnL(unittest.TestCase):

    def _tracker_with_entry(self, entry_time=1700000000.0):
        fd, path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.unlink(path)
        t = PositionTracker(state_file=path)
        c = FakeCandidate()
        t.record_entry(c, tag="cadence-abc", contracts=1, entry_time=entry_time)
        return t, path

    def test_no_orders_returns_error(self):
        t, path = self._tracker_with_entry()
        try:
            trader = MagicMock()
            trader.get_orders.return_value = []
            pos = t.get_by_tag("cadence-abc")
            pnl, detail = t.compute_realized_pnl_cents(pos, trader)
            self.assertIsNone(pnl)
            self.assertIn("no filled", detail)
        finally:
            os.unlink(path)

    def test_close_debit_subtracted_from_credit(self):
        """Entry credit 2.40, close debit 0.80 -> P&L = (2.40 - 0.80) * 100 = $160 = 16000 cents."""
        t, path = self._tracker_with_entry(entry_time=1700000000.0)
        try:
            trader = MagicMock()
            trader.get_orders.return_value = [
                # Close order -- later timestamp, filled, tagged
                {
                    "tag": "cadence-abc",
                    "status": "filled",
                    "type": "debit",
                    "class": "multileg",
                    "avg_fill_price": 0.80,
                    "create_date": "2026-04-14T10:00:00.000Z",
                },
            ]
            pos = t.get_by_tag("cadence-abc")
            pnl, detail = t.compute_realized_pnl_cents(pos, trader)
            # (2.40 - 0.80) * 100 (per-contract * 100 shares) * 100 (cents)
            # = 1.60 * 100 * 1 * 100 = 16000
            self.assertEqual(pnl, 16000)
            self.assertIn("P&L=$160.00", detail)
        finally:
            os.unlink(path)

    def test_api_error_returns_none(self):
        t, path = self._tracker_with_entry()
        try:
            trader = MagicMock()
            trader.get_orders.side_effect = RuntimeError("api down")
            pos = t.get_by_tag("cadence-abc")
            pnl, detail = t.compute_realized_pnl_cents(pos, trader)
            self.assertIsNone(pnl)
            self.assertIn("api down", detail)
        finally:
            os.unlink(path)


class TestOrderCreatedAfter(unittest.TestCase):

    def test_after(self):
        # 2026-04-14 is after 2026-04-13 timestamp
        import calendar
        ts = calendar.timegm((2026, 4, 13, 12, 0, 0, 0, 0, 0))
        o = {"create_date": "2026-04-14T10:00:00.000Z"}
        self.assertTrue(_order_created_after(o, ts))

    def test_before(self):
        import calendar
        ts = calendar.timegm((2026, 4, 15, 12, 0, 0, 0, 0, 0))
        o = {"create_date": "2026-04-14T10:00:00.000Z"}
        self.assertFalse(_order_created_after(o, ts))

    def test_missing_date(self):
        self.assertFalse(_order_created_after({}, 0))

    def test_malformed_date(self):
        self.assertFalse(_order_created_after({"create_date": "garbage"}, 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)

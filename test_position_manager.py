"""Tests for position_manager.py."""

import os
import sys
import unittest
from datetime import date

sys.path.insert(0, os.path.dirname(__file__))
from position_manager import PositionManager, ExitReason


class TestPositionManager(unittest.TestCase):

    def setUp(self):
        self.pm = PositionManager(profit_target_pct=50, time_stop_dte=21,
                                   loss_stop_multiplier=2.0)

    def test_profit_target_hit(self):
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 0.80,  # P&L = 2.00 - 0.80 = 1.20 = 60%
            "expiration": "2026-06-30",
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0].reason, ExitReason.PROFIT_TARGET)

    def test_profit_target_not_hit(self):
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 1.50,  # P&L = 0.50 = 25%
            "expiration": "2026-06-30",
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 0)

    def test_time_stop_hit(self):
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 1.80,
            "expiration": "2026-04-30",  # 18 DTE from Apr 12
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0].reason, ExitReason.TIME_STOP)

    def test_time_stop_with_dte_field(self):
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 1.80,
            "dte": 15,
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0].reason, ExitReason.TIME_STOP)

    def test_loss_stop_hit(self):
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 6.50,  # P&L = -4.50, loss = 4.50 >= 2x2.00=4.00
            "expiration": "2026-06-30",
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0].reason, ExitReason.LOSS_STOP)

    def test_loss_stop_not_hit(self):
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 3.50,  # P&L = -1.50, loss = 1.50 < 2x2.00=4.00
            "expiration": "2026-06-30",
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 0)

    def test_multiple_positions(self):
        positions = [
            {"id": "p1", "entry_credit": 2.00, "current_debit": 0.80, "expiration": "2026-06-30"},
            {"id": "p2", "entry_credit": 2.00, "current_debit": 1.80, "expiration": "2026-06-30"},
            {"id": "p3", "entry_credit": 2.00, "current_debit": 6.50, "expiration": "2026-06-30"},
        ]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 2)  # p1 profit, p3 loss
        reasons = {e.position_id: e.reason for e in exits}
        self.assertEqual(reasons["p1"], ExitReason.PROFIT_TARGET)
        self.assertEqual(reasons["p3"], ExitReason.LOSS_STOP)

    def test_no_credit_skipped(self):
        positions = [{"id": "p1", "entry_credit": 0, "current_debit": 1.00}]
        exits = self.pm.check_for_exits(positions)
        self.assertEqual(len(exits), 0)

    def test_exit_action_to_dict(self):
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 0.80,
            "expiration": "2026-06-30",
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        d = exits[0].to_dict()
        self.assertEqual(d["reason"], "profit_target")

    def test_priority_profit_over_time(self):
        """Profit target should trigger before time stop."""
        positions = [{
            "id": "pos1",
            "entry_credit": 2.00,
            "current_debit": 0.50,  # 75% profit
            "expiration": "2026-04-20",  # 8 DTE
        }]
        exits = self.pm.check_for_exits(positions, today=date(2026, 4, 12))
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0].reason, ExitReason.PROFIT_TARGET)


if __name__ == "__main__":
    unittest.main(verbosity=2)

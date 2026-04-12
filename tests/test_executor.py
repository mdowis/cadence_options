"""Tests for executor.py."""

import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.executor import (
    build_iron_condor_legs,
    build_close_legs,
    execute_candidate,
    _validate_leg_count,
)
from cadence.risk_manager import RiskManager, RiskConfig, RiskAction


class FakeCandidate:
    def __init__(self):
        self.symbol = "SPY"
        self.expiration = "2026-05-30"
        self.short_put_symbol = "SPY260530P00435000"
        self.short_put_strike = 435
        self.long_put_symbol = "SPY260530P00425000"
        self.long_put_strike = 425
        self.short_call_symbol = "SPY260530C00465000"
        self.short_call_strike = 465
        self.long_call_symbol = "SPY260530C00475000"
        self.long_call_strike = 475
        self.credit = 2.50
        self.max_loss = 7.50
        self.iv_rank = 50
        self.return_pct = 33.3


class TestBuildLegs(unittest.TestCase):

    def test_iron_condor_legs(self):
        c = FakeCandidate()
        legs = build_iron_condor_legs(c, contracts=1)
        self.assertEqual(len(legs), 4)
        # Short put sell, long put buy, short call sell, long call buy
        self.assertEqual(legs[0], ("SPY260530P00435000", "sell_to_open", 1))
        self.assertEqual(legs[1], ("SPY260530P00425000", "buy_to_open", 1))
        self.assertEqual(legs[2], ("SPY260530C00465000", "sell_to_open", 1))
        self.assertEqual(legs[3], ("SPY260530C00475000", "buy_to_open", 1))

    def test_multiple_contracts(self):
        c = FakeCandidate()
        legs = build_iron_condor_legs(c, contracts=3)
        for _, _, qty in legs:
            self.assertEqual(qty, 3)

    def test_close_legs(self):
        position = {
            "short_put_symbol": "SPY260530P00435000",
            "long_put_symbol": "SPY260530P00425000",
            "short_call_symbol": "SPY260530C00465000",
            "long_call_symbol": "SPY260530C00475000",
        }
        legs = build_close_legs(position)
        self.assertEqual(len(legs), 4)
        self.assertEqual(legs[0][1], "buy_to_close")
        self.assertEqual(legs[1][1], "sell_to_close")
        self.assertEqual(legs[2][1], "buy_to_close")
        self.assertEqual(legs[3][1], "sell_to_close")


class TestValidateLegCount(unittest.TestCase):

    def test_4_legs_valid(self):
        _validate_leg_count([1, 2, 3, 4])  # no error

    def test_2_legs_valid(self):
        _validate_leg_count([1, 2])  # no error

    def test_1_leg_rejected(self):
        with self.assertRaises(ValueError):
            _validate_leg_count([1])

    def test_3_legs_rejected(self):
        with self.assertRaises(ValueError):
            _validate_leg_count([1, 2, 3])


class TestExecuteCandidate(unittest.TestCase):

    def setUp(self):
        self.trader = MagicMock()
        self.config = RiskConfig()
        self.risk_mgr = RiskManager(self.config, starting_equity_cents=1000000)
        self.candidate = FakeCandidate()

    def test_dry_run_success(self):
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=True
        )
        self.assertTrue(ok)
        self.assertIn("[DRY RUN]", detail)
        # Should NOT call trader.place_multileg_order in dry run
        self.trader.place_multileg_order.assert_not_called()
        # Should NOT call get_account_balances in dry run
        self.trader.get_account_balances.assert_not_called()

    def test_live_success(self):
        self.trader.get_account_balances.return_value = {
            "balances": {"total_equity": 10000.00, "total_cash": 8000.00}
        }
        self.trader.place_multileg_order.return_value = {
            "order": {"id": 12345, "status": "ok"}
        }
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=False
        )
        self.assertTrue(ok)
        self.assertIn("12345", detail)
        self.trader.get_account_balances.assert_called_once()

    def test_live_blocked_by_risk(self):
        self.risk_mgr.activate_kill_switch("test block")
        self.trader.get_account_balances.return_value = {
            "balances": {"total_equity": 10000.00, "total_cash": 8000.00}
        }
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=False
        )
        self.assertFalse(ok)
        self.assertIn("risk manager", detail.lower())

    def test_live_order_failure(self):
        self.trader.get_account_balances.return_value = {
            "balances": {"total_equity": 10000.00, "total_cash": 8000.00}
        }
        self.trader.place_multileg_order.side_effect = Exception("API error")
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=False
        )
        self.assertFalse(ok)
        self.assertIn("failed", detail.lower())

    def test_regression5_pre_trade_fresh_balance(self):
        """Regression test 5: executor must fetch fresh balance, not cached."""
        call_count = [0]
        def mock_balances():
            call_count[0] += 1
            equity = 10000 if call_count[0] == 1 else 9500
            return {"balances": {"total_equity": equity, "total_cash": equity}}

        self.trader.get_account_balances.side_effect = lambda: mock_balances()
        self.trader.place_multileg_order.return_value = {
            "order": {"id": 1, "status": "ok"}
        }

        # First trade
        execute_candidate(self.trader, self.risk_mgr, self.candidate, dry_run=False)
        # Second trade gets different balance
        execute_candidate(self.trader, self.risk_mgr, self.candidate, dry_run=False)

        self.assertEqual(self.trader.get_account_balances.call_count, 2,
                         "Must fetch fresh balance on every trade, not use cached")


if __name__ == "__main__":
    unittest.main(verbosity=2)

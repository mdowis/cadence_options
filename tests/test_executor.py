"""Tests for executor.py."""

import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.executor import (
    build_iron_condor_legs,
    build_close_legs,
    build_close_legs_from_tracked,
    execute_candidate,
    execute_close,
    compute_close_debit,
    _validate_leg_count,
    _format_order_summary,
)
from cadence.position_tracker import TrackedPosition
from cadence.risk_manager import RiskManager, RiskConfig, RiskAction


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


class TestFormatOrderSummary(unittest.TestCase):

    def test_human_readable_summary(self):
        c = FakeCandidate()
        s = _format_order_summary(c)
        # Expect: "SPY IC 45DTE 425/435P 465/475C $2.50cr"
        self.assertIn("SPY", s)
        self.assertIn("45DTE", s)
        self.assertIn("425/435P", s)
        self.assertIn("465/475C", s)
        self.assertIn("$2.50cr", s)

    def test_contracts_suffix(self):
        c = FakeCandidate()
        s = _format_order_summary(c, contracts=3)
        self.assertIn("x3", s)

    def test_single_contract_no_suffix(self):
        c = FakeCandidate()
        s = _format_order_summary(c, contracts=1)
        self.assertNotIn(" x1", s)

    def test_no_raw_option_symbols(self):
        """Regression: the old detail string dumped full OCC symbols
        like 'SPY260530P00435000' which were unreadable."""
        c = FakeCandidate()
        s = _format_order_summary(c)
        self.assertNotIn("SPY260530", s)


class TestExecuteCandidate(unittest.TestCase):

    def setUp(self):
        self.trader = MagicMock()
        self.config = RiskConfig()
        self.risk_mgr = RiskManager(self.config, starting_equity_cents=1000000)
        self.candidate = FakeCandidate()

    def test_dry_run_success(self):
        # Dry-run pre-syncs balance for paper/live equivalence, so the
        # mock must return a usable shape.
        self.trader.get_account_balances.return_value = {
            "balances": {"total_equity": 10000.00, "total_cash": 8000.00}
        }
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=True
        )
        self.assertTrue(ok)
        self.assertIn("[DRY RUN]", detail)
        # Still must NOT call place_multileg_order in dry run
        self.trader.place_multileg_order.assert_not_called()
        # DOES call get_account_balances (pre-trade sync even in dry-run)
        self.trader.get_account_balances.assert_called_once()

    def test_dry_run_continues_on_sync_failure(self):
        """A bad broker response in dry-run logs a warning but still runs."""
        self.trader.get_account_balances.side_effect = RuntimeError("api down")
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=True
        )
        self.assertTrue(ok)
        self.assertIn("[DRY RUN]", detail)

    def test_live_aborts_on_sync_failure(self):
        """A bad broker response in live mode aborts the trade."""
        self.trader.get_account_balances.side_effect = RuntimeError("api down")
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=False
        )
        self.assertFalse(ok)
        self.assertIn("sync failed", detail.lower())
        self.trader.place_multileg_order.assert_not_called()

    def test_sync_rejects_malformed_balance_response(self):
        """Non-dict response should not corrupt equity state."""
        self.trader.get_account_balances.return_value = "not a dict"
        ok, detail = execute_candidate(
            self.trader, self.risk_mgr, self.candidate, dry_run=False
        )
        self.assertFalse(ok)

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


def _tracked_position():
    return TrackedPosition(
        tag="cadence-t1", symbol="SPY", expiration="2026-05-30",
        dte_at_entry=45, contracts=1, entry_credit=2.40,
        entry_time=1700000000.0,
        short_put_symbol="SPY260530P00435000",
        long_put_symbol="SPY260530P00425000",
        short_call_symbol="SPY260530C00465000",
        long_call_symbol="SPY260530C00475000",
        short_put_strike=435, long_put_strike=425,
        short_call_strike=465, long_call_strike=475,
    )


class TestBuildCloseLegsFromTracked(unittest.TestCase):

    def test_produces_four_close_legs(self):
        t = _tracked_position()
        legs = build_close_legs_from_tracked(t)
        self.assertEqual(len(legs), 4)
        self.assertEqual(legs[0], ("SPY260530P00435000", "buy_to_close", 1))
        self.assertEqual(legs[1], ("SPY260530P00425000", "sell_to_close", 1))
        self.assertEqual(legs[2], ("SPY260530C00465000", "buy_to_close", 1))
        self.assertEqual(legs[3], ("SPY260530C00475000", "sell_to_close", 1))

    def test_contracts_preserved(self):
        t = _tracked_position()
        t.contracts = 5
        legs = build_close_legs_from_tracked(t)
        for _, _, qty in legs:
            self.assertEqual(qty, 5)


class TestComputeCloseDebit(unittest.TestCase):

    def _chain_option(self, sym, bid, ask):
        return {"symbol": sym, "bid": bid, "ask": ask}

    def test_standard_close_debit(self):
        trader = MagicMock()
        trader.get_option_chain.return_value = [
            self._chain_option("SPY260530P00435000", 0.40, 0.50),
            self._chain_option("SPY260530P00425000", 0.10, 0.20),
            self._chain_option("SPY260530C00465000", 0.30, 0.40),
            self._chain_option("SPY260530C00475000", 0.05, 0.15),
        ]
        t = _tracked_position()
        debit, chain = compute_close_debit(trader, t)
        # debit = short_put_ask(0.50) + short_call_ask(0.40)
        #       - long_put_bid(0.10) - long_call_bid(0.05) = 0.75
        self.assertAlmostEqual(debit, 0.75, places=2)

    def test_chain_fetch_failure_returns_none(self):
        trader = MagicMock()
        trader.get_option_chain.side_effect = RuntimeError("fail")
        t = _tracked_position()
        debit, chain = compute_close_debit(trader, t)
        self.assertIsNone(debit)

    def test_missing_legs_in_chain(self):
        trader = MagicMock()
        trader.get_option_chain.return_value = []  # nothing matches
        t = _tracked_position()
        debit, _ = compute_close_debit(trader, t)
        # All four legs missing -> returns None (can't price)
        self.assertIsNone(debit)


class TestExecuteClose(unittest.TestCase):

    def test_dry_run_doesnt_submit(self):
        trader = MagicMock()
        t = _tracked_position()
        ok, detail = execute_close(trader, t, limit_debit=0.80,
                                    dry_run=True, reason="profit_target")
        self.assertTrue(ok)
        self.assertIn("[DRY RUN]", detail)
        self.assertIn("profit_target", detail)
        trader.place_multileg_order.assert_not_called()

    def test_live_submits_debit_order_with_tag(self):
        trader = MagicMock()
        trader.place_multileg_order.return_value = {
            "order": {"id": 42, "status": "ok"}
        }
        t = _tracked_position()
        ok, detail = execute_close(trader, t, limit_debit=0.80,
                                    dry_run=False, reason="profit_target")
        self.assertTrue(ok)
        self.assertIn("42", detail)
        # The close must reuse the tag so close detection + P&L calc match
        kwargs = trader.place_multileg_order.call_args.kwargs
        self.assertEqual(kwargs["tag"], t.tag)
        self.assertEqual(kwargs["order_type"], "debit")
        self.assertEqual(kwargs["price"], 0.80)

    def test_live_failure_returns_false(self):
        trader = MagicMock()
        trader.place_multileg_order.side_effect = RuntimeError("rejected")
        t = _tracked_position()
        ok, detail = execute_close(trader, t, limit_debit=0.80,
                                    dry_run=False, reason="loss_stop")
        self.assertFalse(ok)
        self.assertIn("failed", detail.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)

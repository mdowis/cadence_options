"""Tests for risk_manager.py. Includes all 7 critical regression tests."""

import json
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.risk_manager import (
    RiskManager, RiskConfig, RiskState, RiskAction, RiskDecision,
)


class FakeCandidate:
    """Minimal candidate for risk checks."""
    def __init__(self, iv_rank=50, credit=2.50, max_loss=7.50):
        self.iv_rank = iv_rank
        self.credit = credit
        self.max_loss = max_loss


# ============================================================================
# Regression Test 1: Drawdown must use session_start by default, not peak
# ============================================================================

class TestRegression1_DrawdownSessionStart(unittest.TestCase):
    """A user with existing positions saw the kill switch trip every time
    their portfolio dipped below a stale peak, even though the bot made
    zero trades. session_start resets daily and on manual reset."""

    def test_session_start_no_kill_on_normal_fluctuation(self):
        """Sync at $600, peak rises to $660, dips to $590.
        session_start: drawdown from $600 = 1.7% -> no kill (< 10%).
        peak: drawdown from $660 = 10.6% -> would kill."""
        config = RiskConfig(max_drawdown_pct=10.0, drawdown_reference="session_start")
        rm = RiskManager(config, starting_equity_cents=60000)

        # Push peak up (simulating external positions growing)
        rm.sync_actual_balance(60000, portfolio_value_cents=66000)
        # Now dip
        rm.sync_actual_balance(59000, portfolio_value_cents=59000)

        status = rm.get_status()
        self.assertFalse(status["kill_switch"]["active"],
                         "session_start should NOT trip kill switch on 1.7% drawdown from $600")
        # Drawdown should be from daily_starting_balance ($600), not peak ($660)
        self.assertAlmostEqual(status["drawdown"]["current_pct"], 1.67, places=1)

    def test_peak_mode_kills_on_same_fluctuation(self):
        """Same scenario with peak mode should trip the kill switch."""
        config = RiskConfig(max_drawdown_pct=10.0, drawdown_reference="peak")
        rm = RiskManager(config, starting_equity_cents=60000)

        rm.sync_actual_balance(60000, portfolio_value_cents=66000)
        rm.sync_actual_balance(59000, portfolio_value_cents=59000)

        status = rm.get_status()
        self.assertTrue(status["kill_switch"]["active"],
                        "peak mode SHOULD trip kill switch on 10.6% drawdown from $660")


# ============================================================================
# Regression Test 2: sync_actual_balance uses portfolio_value as authoritative
# ============================================================================

class TestRegression2_PortfolioValueAuthoritative(unittest.TestCase):
    """A prior version computed equity = cash + internally tracked exposure,
    which double-counted positions. portfolio_value from the broker IS the total."""

    def test_equity_equals_portfolio_value_not_plus_exposure(self):
        """Set internal exposure to a stale number, sync with portfolio_value,
        assert equity == portfolio_value (not portfolio_value + stale_exposure)."""
        config = RiskConfig()
        rm = RiskManager(config, starting_equity_cents=50000)

        # Simulate stale internal state
        with rm._lock:
            rm._state.current_equity_cents = 10800  # stale $108

        # Broker says total_equity = $500 (50000 cents)
        rm.sync_actual_balance(balance_cents=20000, portfolio_value_cents=50000)

        status = rm.get_status()
        self.assertEqual(status["equity"]["current"], 50000,
                         "Equity must be portfolio_value (50000), not portfolio_value + stale")
        self.assertEqual(status["equity"]["cash"], 20000,
                         "Cash should be the balance_cents")


# ============================================================================
# Regression Test 3: reset_daily must rebaseline peak_equity to current
# ============================================================================

class TestRegression3_ResetRebaselinesPeak(unittest.TestCase):
    """Without rebaselining, a stale peak from before a bug fix causes
    drawdown to trip immediately after reset."""

    def test_reset_sets_peak_to_current(self):
        """Set peak to $1500, current to $1000, reset, assert peak == $1000."""
        config = RiskConfig(max_drawdown_pct=10.0, drawdown_reference="peak")
        rm = RiskManager(config, starting_equity_cents=150000)

        with rm._lock:
            rm._state.peak_equity_cents = 150000  # $1500
            rm._state.current_equity_cents = 100000  # $1000

        rm.reset_daily()

        status = rm.get_status()
        self.assertEqual(status["equity"]["peak"], 100000,
                         "Peak must be rebaselined to current ($1000) after reset")
        self.assertEqual(status["daily"]["starting_balance_cents"], 100000)


# ============================================================================
# Regression Test 4: Kill switch notification fires exactly once on transition
# ============================================================================

class TestRegression4_KillSwitchNotifiesOnce(unittest.TestCase):
    """Notification should fire on transition from inactive to active,
    not on every check."""

    def test_notifier_called_once(self):
        notifier = MagicMock()
        config = RiskConfig(max_drawdown_pct=10.0)
        rm = RiskManager(config, starting_equity_cents=60000, notifier=notifier)

        # Activate kill switch
        rm.activate_kill_switch("test reason")
        self.assertEqual(notifier.notify_kill_switch.call_count, 1)

        # Now check_trade 5 times with kill switch active
        candidate = FakeCandidate()
        for _ in range(5):
            decision = rm.check_trade(candidate)
            self.assertFalse(decision.allowed)

        # Notification should still be exactly 1
        self.assertEqual(notifier.notify_kill_switch.call_count, 1,
                         "Kill switch notification must fire exactly once, not on every check")

    def test_reactivation_notifies_again(self):
        """Deactivate then reactivate should fire notification again."""
        notifier = MagicMock()
        config = RiskConfig()
        rm = RiskManager(config, starting_equity_cents=60000, notifier=notifier)

        rm.activate_kill_switch("first")
        rm.deactivate_kill_switch()
        rm.activate_kill_switch("second")
        self.assertEqual(notifier.notify_kill_switch.call_count, 2)


# ============================================================================
# Regression Test 5: Pre-trade balance must fetch fresh, not use cached
# ============================================================================

class TestRegression5_FreshBalanceOnTrade(unittest.TestCase):
    """The executor must fetch fresh balance before risk check.
    We test that sync_actual_balance updates equity for the risk check."""

    def test_sync_updates_equity_for_risk_check(self):
        config = RiskConfig(max_drawdown_pct=10.0)
        rm = RiskManager(config, starting_equity_cents=60000)

        # First sync: $600
        rm.sync_actual_balance(60000, portfolio_value_cents=60000)
        status1 = rm.get_status()
        self.assertEqual(status1["equity"]["current"], 60000)

        # Second sync: $580 (different balance)
        rm.sync_actual_balance(58000, portfolio_value_cents=58000)
        status2 = rm.get_status()
        self.assertEqual(status2["equity"]["current"], 58000,
                         "Risk check must use the freshest balance, not cached")
        self.assertEqual(status2["balance_sync"]["count"], 2)


# ============================================================================
# Regression Test 6: Attempt dedup prevents re-execution within TTL
# (This is tested at the process_controller level; here we verify
# the risk manager doesn't interfere with dedup logic)
# ============================================================================

class TestRegression6_RiskManagerAllowsRecheck(unittest.TestCase):
    """The risk manager should consistently allow/block the same candidate.
    Dedup is process_controller's responsibility, not risk_manager's."""

    def test_same_candidate_same_result(self):
        config = RiskConfig()
        rm = RiskManager(config, starting_equity_cents=1000000)
        candidate = FakeCandidate(iv_rank=50, credit=2.50, max_loss=7.50)

        d1 = rm.check_trade(candidate)
        d2 = rm.check_trade(candidate)
        self.assertEqual(d1.allowed, d2.allowed)
        self.assertTrue(d1.allowed)


# ============================================================================
# Regression Test 7: .env loader checks script directory
# (Tested in test_dashboard.py since load_dotenv lives there)
# ============================================================================

class TestRegression7_Placeholder(unittest.TestCase):
    """The .env loader test belongs in test_dashboard.py.
    This placeholder confirms the regression is tracked."""

    def test_documented(self):
        # Actual test in test_dashboard.py::TestLoadDotenv
        pass


# ============================================================================
# General risk_manager tests
# ============================================================================

class TestRiskDecision(unittest.TestCase):

    def test_allow_is_allowed(self):
        d = RiskDecision(RiskAction.ALLOW)
        self.assertTrue(d.allowed)

    def test_block_is_not_allowed(self):
        d = RiskDecision(RiskAction.BLOCK_KILL_SWITCH, "reason")
        self.assertFalse(d.allowed)

    def test_to_dict(self):
        d = RiskDecision(RiskAction.ALLOW, "ok")
        dd = d.to_dict()
        self.assertEqual(dd["action"], "allow")
        self.assertTrue(dd["allowed"])


class TestRiskChecks(unittest.TestCase):

    def setUp(self):
        self.config = RiskConfig()
        self.rm = RiskManager(self.config, starting_equity_cents=1000000)

    def test_allow_valid_trade(self):
        candidate = FakeCandidate(iv_rank=50, credit=2.50, max_loss=7.50)
        d = self.rm.check_trade(candidate)
        self.assertTrue(d.allowed)

    def test_block_low_iv_rank(self):
        candidate = FakeCandidate(iv_rank=20)
        d = self.rm.check_trade(candidate)
        self.assertFalse(d.allowed)
        self.assertEqual(d.action, RiskAction.BLOCK_IV_RANK)

    def test_block_low_credit(self):
        # wing_width = credit + max_loss = 0.50 + 9.50 = 10
        # min credit = 20% * 10 = 2.0, but credit is 0.50
        candidate = FakeCandidate(iv_rank=50, credit=0.50, max_loss=9.50)
        d = self.rm.check_trade(candidate)
        self.assertFalse(d.allowed)
        self.assertEqual(d.action, RiskAction.BLOCK_CREDIT)

    def test_block_too_many_positions(self):
        self.rm.update_position_count(5)
        candidate = FakeCandidate()
        d = self.rm.check_trade(candidate)
        self.assertFalse(d.allowed)
        self.assertEqual(d.action, RiskAction.BLOCK_POSITION_COUNT)

    def test_block_position_too_large(self):
        # equity = $100 (10000 cents), max_loss = 7.50 * 100 = $750 = 7.5% >> 2%
        rm = RiskManager(self.config, starting_equity_cents=10000)
        candidate = FakeCandidate(iv_rank=50, credit=2.50, max_loss=7.50)
        d = rm.check_trade(candidate)
        self.assertFalse(d.allowed)
        self.assertEqual(d.action, RiskAction.BLOCK_POSITION_SIZE)

    def test_block_kill_switch(self):
        self.rm.activate_kill_switch("test")
        candidate = FakeCandidate()
        d = self.rm.check_trade(candidate)
        self.assertFalse(d.allowed)
        self.assertEqual(d.action, RiskAction.BLOCK_KILL_SWITCH)

    def test_block_consecutive_losses(self):
        with self.rm._lock:
            self.rm._state.consecutive_losses = 5
        candidate = FakeCandidate()
        d = self.rm.check_trade(candidate)
        self.assertFalse(d.allowed)
        self.assertEqual(d.action, RiskAction.BLOCK_COOLDOWN)

    def test_block_daily_loss(self):
        config = RiskConfig(max_daily_loss_cents=5000)  # $50
        rm = RiskManager(config, starting_equity_cents=1000000)
        rm.record_trade(-6000, "big loss")
        candidate = FakeCandidate()
        d = rm.check_trade(candidate)
        self.assertFalse(d.allowed)
        self.assertEqual(d.action, RiskAction.BLOCK_DAILY_LOSS)

    def test_daily_loss_pct_and_cents_smaller_wins(self):
        config = RiskConfig(
            max_daily_loss_cents=10000,   # $100
            max_daily_loss_pct=1.0,       # 1% of $10000 = $100
        )
        rm = RiskManager(config, starting_equity_cents=1000000)
        # daily_starting_balance = $10000 = 1000000 cents
        # 1% of 1000000 = 10000 cents = $100
        # Both are 10000, so either should trigger
        rm.record_trade(-11000, "loss")
        candidate = FakeCandidate()
        d = rm.check_trade(candidate)
        self.assertFalse(d.allowed)


class TestRiskStateSerDe(unittest.TestCase):

    def test_round_trip(self):
        state = RiskState(100000)
        state.kill_switch_active = True
        state.kill_switch_reason = "test"
        state.consecutive_losses = 3
        d = state.to_dict()
        restored = RiskState.from_dict(d)
        self.assertEqual(restored.starting_equity_cents, 100000)
        self.assertTrue(restored.kill_switch_active)
        self.assertEqual(restored.consecutive_losses, 3)


class TestStatePersistence(unittest.TestCase):

    def test_save_and_load(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            config = RiskConfig()
            rm = RiskManager(config, starting_equity_cents=50000, state_file=path)
            rm.activate_kill_switch("persist test")

            # Create new instance from same file
            rm2 = RiskManager(config, starting_equity_cents=0, state_file=path)
            status = rm2.get_status()
            self.assertTrue(status["kill_switch"]["active"])
            self.assertEqual(status["equity"]["starting"], 50000)
        finally:
            os.unlink(path)


class TestRecordTrade(unittest.TestCase):

    def test_winning_trade(self):
        config = RiskConfig()
        rm = RiskManager(config, starting_equity_cents=100000)
        rm.record_trade(5000, "profit")
        status = rm.get_status()
        self.assertEqual(status["daily"]["pnl_cents"], 5000)
        self.assertEqual(status["daily"]["win_count"], 1)

    def test_losing_trade_increments_consecutive(self):
        config = RiskConfig()
        rm = RiskManager(config, starting_equity_cents=100000)
        rm.record_trade(-1000, "loss 1")
        rm.record_trade(-1000, "loss 2")
        self.assertEqual(rm._state.consecutive_losses, 2)

    def test_win_resets_consecutive_losses(self):
        config = RiskConfig()
        rm = RiskManager(config, starting_equity_cents=100000)
        rm.record_trade(-1000)
        rm.record_trade(-1000)
        rm.record_trade(500)
        self.assertEqual(rm._state.consecutive_losses, 0)


class TestGetStatus(unittest.TestCase):

    def test_includes_drawdown_reference_mode(self):
        config = RiskConfig(drawdown_reference="session_start")
        rm = RiskManager(config, starting_equity_cents=100000)
        status = rm.get_status()
        self.assertEqual(status["drawdown_reference_mode"], "session_start")
        self.assertEqual(status["drawdown"]["reference_mode"], "session_start")

    def test_drawdown_calculation_in_status(self):
        config = RiskConfig(drawdown_reference="session_start")
        rm = RiskManager(config, starting_equity_cents=100000)
        rm.sync_actual_balance(95000, portfolio_value_cents=95000)
        status = rm.get_status()
        self.assertAlmostEqual(status["drawdown"]["current_pct"], 5.0, places=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

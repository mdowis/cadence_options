"""Tests for kelly.py position sizing."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.kelly import (
    compute_kelly_fraction,
    fractional_kelly,
    compute_kelly_from_history,
    recommended_position_risk_pct,
    DEFAULT_WIN_RATE,
    DEFAULT_AVG_WIN_PCT,
    DEFAULT_AVG_LOSS_PCT,
    MIN_TRADES_FOR_EMPIRICAL,
)


class TestComputeKellyFraction(unittest.TestCase):

    def test_classic_coin_flip_with_edge(self):
        # 60% win, equal payoff -> f* = 0.2
        # (0.6*1 - 0.4*1) / 1 = 0.2
        self.assertAlmostEqual(compute_kelly_fraction(0.6, 1.0, 1.0), 0.2)

    def test_no_edge_zero(self):
        # 50/50 equal payoff -> zero edge
        self.assertAlmostEqual(compute_kelly_fraction(0.5, 1.0, 1.0), 0.0)

    def test_negative_edge_returns_negative(self):
        self.assertLess(compute_kelly_fraction(0.4, 1.0, 1.0), 0)

    def test_typical_iron_condor(self):
        # 75% win, avg_win=30, avg_loss=70 (user-specified units)
        # (0.75 * 30 - 0.25 * 70) / 30 = 5 / 30 = 0.1667
        result = compute_kelly_fraction(0.75, 30, 70)
        self.assertAlmostEqual(result, 5.0 / 30.0, places=4)

    def test_asymmetric_big_win_rare_loss(self):
        # 90% win, win $100, lose $10:
        # (0.9*100 - 0.1*10) / 100 = 89/100 = 0.89
        self.assertAlmostEqual(compute_kelly_fraction(0.9, 100, 10), 0.89)

    def test_zero_avg_win_returns_zero(self):
        self.assertEqual(compute_kelly_fraction(0.9, 0, 100), 0.0)

    def test_negative_avg_win_returns_zero(self):
        self.assertEqual(compute_kelly_fraction(0.9, -10, 50), 0.0)

    def test_explicit_loss_rate(self):
        # If loss_rate is supplied, use it (instead of 1-win_rate)
        result = compute_kelly_fraction(0.7, 10, 5, loss_rate=0.3)
        expected = (0.7 * 10 - 0.3 * 5) / 10
        self.assertAlmostEqual(result, expected)


class TestFractionalKelly(unittest.TestCase):

    def test_half_kelly(self):
        self.assertAlmostEqual(fractional_kelly(0.2, 0.5), 0.1)

    def test_quarter_kelly_default(self):
        self.assertAlmostEqual(fractional_kelly(0.2), 0.05)

    def test_eighth_kelly(self):
        self.assertAlmostEqual(fractional_kelly(0.2, 0.125), 0.025)

    def test_negative_full_kelly_clamps_to_zero(self):
        self.assertEqual(fractional_kelly(-0.1, 0.5), 0.0)

    def test_zero_clamps_to_zero(self):
        self.assertEqual(fractional_kelly(0.0), 0.0)


class TestComputeKellyFromHistory(unittest.TestCase):

    def test_empty_history_uses_defaults(self):
        result = compute_kelly_from_history([])
        self.assertEqual(result["sample_size"], 0)
        self.assertTrue(result["using_defaults"])
        self.assertEqual(result["win_rate"], DEFAULT_WIN_RATE)

    def test_below_min_trades_uses_defaults(self):
        history = [{"pnl_cents": 100}] * 5
        result = compute_kelly_from_history(history, min_trades=10)
        self.assertTrue(result["using_defaults"])
        self.assertEqual(result["sample_size"], 5)

    def test_sufficient_history_computes_from_data(self):
        # 8 wins of $100, 2 losses of $500
        # win_rate = 0.8, avg_win = 100, avg_loss = 500
        # f* = (0.8*100 - 0.2*500) / 100 = (80-100)/100 = -0.2
        history = [{"pnl_cents": 100} for _ in range(8)]
        history += [{"pnl_cents": -500} for _ in range(2)]
        result = compute_kelly_from_history(history, min_trades=10)
        self.assertFalse(result["using_defaults"])
        self.assertEqual(result["sample_size"], 10)
        self.assertAlmostEqual(result["win_rate"], 0.8)
        self.assertAlmostEqual(result["kelly_fraction"], -0.2, places=4)

    def test_positive_edge_history(self):
        # 7 wins of $200, 3 losses of $100
        # win_rate = 0.7, avg_win = 200, avg_loss = 100
        # f* = (0.7*200 - 0.3*100) / 200 = (140-30)/200 = 0.55
        history = [{"pnl_cents": 200}] * 7 + [{"pnl_cents": -100}] * 3
        result = compute_kelly_from_history(history, min_trades=10)
        self.assertAlmostEqual(result["kelly_fraction"], 0.55, places=4)
        self.assertEqual(result["avg_win_cents"], 200)
        self.assertEqual(result["avg_loss_cents"], 100)

    def test_all_wins_avg_loss_zero(self):
        history = [{"pnl_cents": 100}] * 12
        result = compute_kelly_from_history(history, min_trades=10)
        # win_rate = 1.0, avg_loss = 0 -> f* = (1.0 * 100 - 0 * 0)/100 = 1.0
        self.assertEqual(result["win_rate"], 1.0)
        self.assertEqual(result["avg_loss_cents"], 0)
        self.assertAlmostEqual(result["kelly_fraction"], 1.0)

    def test_all_losses(self):
        history = [{"pnl_cents": -100}] * 12
        result = compute_kelly_from_history(history, min_trades=10)
        self.assertEqual(result["win_rate"], 0.0)
        # f* = (0 - 1.0 * 100) / avg_win where avg_win=0 -> returns 0
        self.assertEqual(result["kelly_fraction"], 0.0)

    def test_ignores_entries_without_pnl(self):
        history = [
            {"pnl_cents": 100},
            {"detail": "no pnl here"},  # should be ignored
            {"pnl_cents": -50},
        ]
        result = compute_kelly_from_history(history, min_trades=2)
        self.assertEqual(result["sample_size"], 2)


class TestRecommendedPositionRiskPct(unittest.TestCase):

    def test_default_quarter_kelly(self):
        # Empty history -> defaults (75%, 30%, 70%) -> Kelly = 5/30 = 0.1667
        # Quarter = 0.0417 = 4.17%
        result = recommended_position_risk_pct([])
        self.assertAlmostEqual(result["kelly_fraction"], 5.0 / 30.0, places=3)
        self.assertAlmostEqual(result["fractional_kelly_pct"],
                               (5.0 / 30.0) * 0.25 * 100, places=3)
        self.assertEqual(result["fraction_of_full"], 0.25)
        self.assertTrue(result["using_defaults"])

    def test_absolute_cap_clips(self):
        # Kelly would recommend 4.17%, but cap at 2%
        result = recommended_position_risk_pct([], absolute_cap_pct=2.0)
        self.assertAlmostEqual(result["recommended_pct"], 2.0)

    def test_absolute_cap_not_clipping_when_kelly_lower(self):
        # Negative-edge history gives 0%, cap at 2% -> recommendation 0.
        # Need enough trades to defeat the defaults fallback.
        history = [{"pnl_cents": -100}] * 25
        result = recommended_position_risk_pct(
            history, absolute_cap_pct=2.0, min_trades=10)
        self.assertEqual(result["recommended_pct"], 0.0)

    def test_half_kelly(self):
        result = recommended_position_risk_pct([], fraction_of_full=0.5)
        # Full Kelly ~16.67%, half ~8.33%
        self.assertAlmostEqual(result["fractional_kelly_pct"],
                               (5.0 / 30.0) * 0.5 * 100, places=3)


class TestRiskManagerKellyIntegration(unittest.TestCase):
    """Verify Kelly path integrates with RiskManager.check_trade."""

    def setUp(self):
        from cadence.risk_manager import RiskManager, RiskConfig
        self.RiskManager = RiskManager
        self.RiskConfig = RiskConfig

    def _candidate(self, max_loss=7.50, credit=2.50, iv_rank=50):
        class C:
            pass
        c = C()
        c.max_loss = max_loss
        c.credit = credit
        c.iv_rank = iv_rank
        return c

    def test_kelly_disabled_uses_manual_cap_only(self):
        cfg = self.RiskConfig(use_kelly=False,
                              max_risk_per_position_pct=2.0)
        # $100k equity, max_loss=$7.50/share = $750/contract = 0.75%
        rm = self.RiskManager(cfg, starting_equity_cents=10000000)
        d = rm.check_trade(self._candidate(), contracts=1)
        self.assertTrue(d.allowed, "0.75% risk should pass 2% cap")

    def test_kelly_enabled_no_history_uses_default(self):
        """With use_kelly=True and empty history, defaults give
        quarter-Kelly ~4.17%, clipped by the 2% manual cap to 2%."""
        cfg = self.RiskConfig(use_kelly=True,
                              max_risk_per_position_pct=2.0,
                              kelly_fraction_of_full=0.25)
        rm = self.RiskManager(cfg, starting_equity_cents=10000000)
        # Same 0.75% position -- still passes
        d = rm.check_trade(self._candidate(), contracts=1)
        self.assertTrue(d.allowed)

    def test_kelly_blocks_when_history_is_unprofitable(self):
        """With a losing track record, Kelly drops to ~0% and blocks all trades."""
        cfg = self.RiskConfig(use_kelly=True,
                              max_risk_per_position_pct=5.0,
                              kelly_fraction_of_full=0.5)
        rm = self.RiskManager(cfg, starting_equity_cents=10000000)
        # Feed 25 losses into trade_history (bypass record_trade consecutive
        # loss block for this test by directly appending)
        with rm._lock:
            for _ in range(25):
                rm._state.trade_history.append({"pnl_cents": -5000, "detail": "loss"})
        d = rm.check_trade(self._candidate(), contracts=1)
        # Kelly = 0 -> effective cap = 0 -> any positive risk is blocked
        self.assertFalse(d.allowed)

    def test_get_status_includes_kelly(self):
        cfg = self.RiskConfig()
        rm = self.RiskManager(cfg, starting_equity_cents=10000000)
        status = rm.get_status()
        self.assertIn("kelly", status)
        k = status["kelly"]
        self.assertIn("enabled", k)
        self.assertIn("full_kelly_fraction", k)
        self.assertIn("effective_cap_pct", k)
        self.assertIn("manual_cap_pct", k)
        # Default: Kelly disabled, effective = manual cap
        self.assertFalse(k["enabled"])
        self.assertEqual(k["effective_cap_pct"], k["manual_cap_pct"])

    def test_get_status_kelly_enabled_uses_min(self):
        cfg = self.RiskConfig(use_kelly=True,
                              max_risk_per_position_pct=2.0,
                              kelly_fraction_of_full=0.25)
        rm = self.RiskManager(cfg, starting_equity_cents=10000000)
        status = rm.get_status()
        k = status["kelly"]
        self.assertTrue(k["enabled"])
        # Defaults give quarter-Kelly ~4.17%; clipped to manual 2%
        self.assertLessEqual(k["effective_cap_pct"], 2.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

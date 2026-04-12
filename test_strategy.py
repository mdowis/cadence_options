"""Tests for strategy.py with mocked option chain data."""

import os
import sys
import unittest
from datetime import date
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(__file__))
from strategy import (
    StrategyConfig,
    IronCondorCandidate,
    find_iron_condor_candidates,
    _pick_expiration,
    _find_strike_by_delta,
    _find_option_at_strike,
)


def make_option(symbol, strike, option_type, bid, ask, delta):
    """Create a mock option dict matching Tradier's format."""
    return {
        "symbol": symbol,
        "strike": strike,
        "option_type": option_type,
        "bid": bid,
        "ask": ask,
        "greeks": {"delta": delta, "gamma": 0.01, "theta": -0.05, "vega": 0.15, "mid_iv": 0.25},
    }


def build_mock_chain():
    """Build a realistic mock chain with puts and calls around SPY ~450."""
    chain = []
    # Puts (negative deltas, lower strikes)
    chain.append(make_option("SPY260530P00420000", 420, "put", 0.80, 1.00, -0.08))
    chain.append(make_option("SPY260530P00425000", 425, "put", 1.10, 1.20, -0.10))
    chain.append(make_option("SPY260530P00430000", 430, "put", 1.80, 2.00, -0.13))
    chain.append(make_option("SPY260530P00435000", 435, "put", 2.50, 2.70, -0.16))  # ~16 delta
    chain.append(make_option("SPY260530P00440000", 440, "put", 3.20, 3.40, -0.20))
    chain.append(make_option("SPY260530P00445000", 445, "put", 4.00, 4.20, -0.25))
    # Calls (positive deltas, higher strikes)
    chain.append(make_option("SPY260530C00455000", 455, "call", 4.00, 4.20, 0.25))
    chain.append(make_option("SPY260530C00460000", 460, "call", 3.20, 3.40, 0.20))
    chain.append(make_option("SPY260530C00465000", 465, "call", 2.50, 2.70, 0.16))  # ~16 delta
    chain.append(make_option("SPY260530C00470000", 470, "call", 1.80, 2.00, 0.13))
    chain.append(make_option("SPY260530C00475000", 475, "call", 1.10, 1.20, 0.10))
    chain.append(make_option("SPY260530C00480000", 480, "call", 0.80, 1.00, 0.08))
    return chain


class TestPickExpiration(unittest.TestCase):

    def test_picks_closest_to_target(self):
        exps = ["2026-05-01", "2026-05-30", "2026-06-20"]
        today = date(2026, 4, 12)
        result = _pick_expiration(exps, 45, today)
        # May 30 = 48 DTE, closest to 45
        self.assertEqual(result, ("2026-05-30", 48))

    def test_skips_past_expirations(self):
        exps = ["2026-04-01", "2026-05-30"]
        today = date(2026, 4, 12)
        result = _pick_expiration(exps, 45, today)
        self.assertEqual(result[0], "2026-05-30")

    def test_empty_expirations(self):
        self.assertIsNone(_pick_expiration([], 45))

    def test_all_expired(self):
        exps = ["2026-01-01", "2026-02-01"]
        today = date(2026, 4, 12)
        self.assertIsNone(_pick_expiration(exps, 45, today))


class TestFindStrikeByDelta(unittest.TestCase):

    def test_find_put_at_16_delta(self):
        chain = build_mock_chain()
        puts = [o for o in chain if o["option_type"] == "put"]
        result = _find_strike_by_delta(puts, 16, "put")
        self.assertEqual(result["strike"], 435)  # -0.16 delta

    def test_find_call_at_16_delta(self):
        chain = build_mock_chain()
        calls = [o for o in chain if o["option_type"] == "call"]
        result = _find_strike_by_delta(calls, 16, "call")
        self.assertEqual(result["strike"], 465)  # +0.16 delta

    def test_no_greeks_skipped(self):
        options = [{"strike": 430, "option_type": "put"}]  # no greeks key
        result = _find_strike_by_delta(options, 16, "put")
        self.assertIsNone(result)


class TestFindOptionAtStrike(unittest.TestCase):

    def test_exact_match(self):
        chain = build_mock_chain()
        puts = [o for o in chain if o["option_type"] == "put"]
        result = _find_option_at_strike(puts, 425)
        self.assertIsNotNone(result)
        self.assertEqual(result["strike"], 425)

    def test_no_match(self):
        chain = build_mock_chain()
        puts = [o for o in chain if o["option_type"] == "put"]
        result = _find_option_at_strike(puts, 999)
        self.assertIsNone(result)


class TestFindIronCondorCandidates(unittest.TestCase):

    def setUp(self):
        self.config = StrategyConfig(
            target_dte=45,
            dte_tolerance_low=40,
            dte_tolerance_high=50,
            target_delta=16,
            wing_width=10,
            min_iv_rank=30,
            min_credit_pct_of_width=20,
        )
        self.today = date(2026, 4, 12)
        self.mock_trader = MagicMock()
        self.mock_trader.get_expirations.return_value = ["2026-05-30"]
        self.mock_trader.get_option_chain.return_value = build_mock_chain()

    def test_finds_candidate(self):
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=50, today=self.today
        )
        self.assertEqual(len(candidates), 1)
        c = candidates[0]
        self.assertEqual(c.symbol, "SPY")
        self.assertEqual(c.short_put_strike, 435)
        self.assertEqual(c.long_put_strike, 425)   # 435 - 10
        self.assertEqual(c.short_call_strike, 465)
        self.assertEqual(c.long_call_strike, 475)   # 465 + 10
        # Credit = short_put_bid(2.50) + short_call_bid(2.50) - long_put_ask(1.20) - long_call_ask(1.20) = 2.60
        self.assertAlmostEqual(c.credit, 2.60, places=2)
        # Max loss = 10 - 2.60 = 7.40
        self.assertAlmostEqual(c.max_loss, 7.40, places=2)
        self.assertGreater(c.return_pct, 0)

    def test_iv_rank_too_low(self):
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=10, today=self.today
        )
        self.assertEqual(len(candidates), 0)

    def test_dte_outside_tolerance(self):
        # Expiration too far out
        self.mock_trader.get_expirations.return_value = ["2026-08-30"]
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=50, today=self.today
        )
        self.assertEqual(len(candidates), 0)

    def test_no_expirations(self):
        self.mock_trader.get_expirations.return_value = []
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=50, today=self.today
        )
        self.assertEqual(len(candidates), 0)

    def test_empty_chain(self):
        self.mock_trader.get_option_chain.return_value = []
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=50, today=self.today
        )
        self.assertEqual(len(candidates), 0)

    def test_credit_below_minimum(self):
        # Override with very low bids
        chain = build_mock_chain()
        for opt in chain:
            opt["bid"] = 0.05
        self.mock_trader.get_option_chain.return_value = chain
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=50, today=self.today
        )
        self.assertEqual(len(candidates), 0)

    def test_fingerprint(self):
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=50, today=self.today
        )
        fp = candidates[0].fingerprint()
        self.assertIn("SPY", fp)
        self.assertIn("435", fp)
        self.assertIn("465", fp)

    def test_to_dict(self):
        candidates = find_iron_condor_candidates(
            self.mock_trader, "SPY", self.config, iv_rank=50, today=self.today
        )
        d = candidates[0].to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["symbol"], "SPY")


if __name__ == "__main__":
    unittest.main(verbosity=2)

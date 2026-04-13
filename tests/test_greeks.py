"""Tests for portfolio Greek aggregation."""

import os
import sys
import unittest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.greeks import (
    aggregate_portfolio_greeks,
    _parse_occ_symbol,
    _position_quantity,
    _safe_float,
)


class TestParseOccSymbol(unittest.TestCase):

    def test_spy_put(self):
        root, exp, opt_type, strike = _parse_occ_symbol("SPY260530P00435000")
        self.assertEqual(root, "SPY")
        self.assertEqual(exp, "2026-05-30")
        self.assertEqual(opt_type, "put")
        self.assertEqual(strike, 435.0)

    def test_qqq_call(self):
        root, exp, opt_type, strike = _parse_occ_symbol("QQQ260628C00500000")
        self.assertEqual(root, "QQQ")
        self.assertEqual(exp, "2026-06-28")
        self.assertEqual(opt_type, "call")
        self.assertEqual(strike, 500.0)

    def test_fractional_strike(self):
        # 567.5 strike -> "00567500"
        root, exp, opt_type, strike = _parse_occ_symbol("QQQ260530P00567500")
        self.assertEqual(strike, 567.5)

    def test_invalid_returns_none(self):
        self.assertIsNone(_parse_occ_symbol(""))
        self.assertIsNone(_parse_occ_symbol("SPY"))
        self.assertIsNone(_parse_occ_symbol(None))

    def test_invalid_type_char(self):
        # 'X' instead of 'C'/'P' -> None
        self.assertIsNone(_parse_occ_symbol("SPY260530X00435000"))

    def test_invalid_month(self):
        # Month 13 -> None
        self.assertIsNone(_parse_occ_symbol("SPY261330P00435000"))


class TestPositionQuantity(unittest.TestCase):

    def test_positive(self):
        self.assertEqual(_position_quantity({"quantity": 2}), 2.0)

    def test_negative(self):
        self.assertEqual(_position_quantity({"quantity": -3}), -3.0)

    def test_string_quantity(self):
        self.assertEqual(_position_quantity({"quantity": "5"}), 5.0)

    def test_missing(self):
        self.assertEqual(_position_quantity({}), 0)


class TestSafeFloat(unittest.TestCase):

    def test_float(self):
        self.assertEqual(_safe_float(1.5), 1.5)

    def test_none(self):
        self.assertEqual(_safe_float(None), 0.0)

    def test_none_with_default(self):
        self.assertEqual(_safe_float(None, 99), 99)

    def test_string_float(self):
        self.assertEqual(_safe_float("1.5"), 1.5)

    def test_invalid(self):
        self.assertEqual(_safe_float("junk"), 0.0)


class TestAggregatePortfolioGreeks(unittest.TestCase):

    def _make_chain_option(self, sym, strike, opt_type, delta, gamma=0.01,
                           vega=0.10, theta=-0.05):
        return {
            "symbol": sym,
            "strike": strike,
            "option_type": opt_type,
            "greeks": {"delta": delta, "gamma": gamma,
                       "vega": vega, "theta": theta},
        }

    def test_empty_positions(self):
        trader = MagicMock()
        result = aggregate_portfolio_greeks(trader, [])
        self.assertEqual(result, {"delta_cents": 0, "gamma_cents": 0,
                                   "vega_cents": 0, "theta_cents": 0})
        trader.get_option_chain.assert_not_called()

    def test_single_short_put(self):
        """Short 1 SPY 435 put with delta -0.16, spot 450.
        Short position: qty = -1.
        Dollar delta = -0.16 * -1 * 100 * 450 = +7200 ($7,200 in cents = 720000).
        """
        trader = MagicMock()
        trader.get_option_chain.return_value = [
            self._make_chain_option("SPY260530P00435000", 435, "put", -0.16),
        ]
        trader.get_quote.return_value = {"last": 450.0}
        positions = [{"symbol": "SPY260530P00435000", "quantity": -1}]
        result = aggregate_portfolio_greeks(trader, positions)
        self.assertEqual(result["delta_cents"], 720000)

    def test_iron_condor_delta_near_zero(self):
        """Full iron condor should be approximately delta-neutral."""
        trader = MagicMock()
        # Short -16 put, long -8 put, short +16 call, long +8 call
        # Position P&L from underlying moves nets out at the center
        trader.get_option_chain.return_value = [
            self._make_chain_option("SPY260530P00425000", 425, "put", -0.08),
            self._make_chain_option("SPY260530P00435000", 435, "put", -0.16),
            self._make_chain_option("SPY260530C00465000", 465, "call", 0.16),
            self._make_chain_option("SPY260530C00475000", 475, "call", 0.08),
        ]
        trader.get_quote.return_value = {"last": 450.0}
        positions = [
            {"symbol": "SPY260530P00435000", "quantity": -1},  # short put
            {"symbol": "SPY260530P00425000", "quantity": 1},   # long put
            {"symbol": "SPY260530C00465000", "quantity": -1},  # short call
            {"symbol": "SPY260530C00475000", "quantity": 1},   # long call
        ]
        result = aggregate_portfolio_greeks(trader, positions)
        # Net share-delta: +0.16 - 0.08 - 0.16 + 0.08 = 0
        self.assertEqual(result["delta_cents"], 0)

    def test_vega_aggregation(self):
        """Net short vega from an iron condor (selling more vega than buying)."""
        trader = MagicMock()
        trader.get_option_chain.return_value = [
            self._make_chain_option("SPY260530P00435000", 435, "put", -0.16,
                                    vega=0.20),  # higher vega near ATM
            self._make_chain_option("SPY260530P00425000", 425, "put", -0.08,
                                    vega=0.10),  # lower vega further OTM
        ]
        trader.get_quote.return_value = {"last": 450.0}
        positions = [
            {"symbol": "SPY260530P00435000", "quantity": -1},  # short high-vega
            {"symbol": "SPY260530P00425000", "quantity": 1},   # long low-vega
        ]
        result = aggregate_portfolio_greeks(trader, positions)
        # Net position vega = -0.20*100 + 0.10*100 = -10 per 1% IV
        # In cents: -10 * 100 = -1000
        self.assertEqual(result["vega_cents"], -1000)

    def test_theta_positive_for_short_position(self):
        """Short options collect theta (positive for the seller)."""
        trader = MagicMock()
        trader.get_option_chain.return_value = [
            self._make_chain_option("SPY260530P00435000", 435, "put", -0.16,
                                    theta=-0.05),  # Tradier theta is negative
        ]
        trader.get_quote.return_value = {"last": 450.0}
        positions = [{"symbol": "SPY260530P00435000", "quantity": -1}]
        result = aggregate_portfolio_greeks(trader, positions)
        # Short position: qty = -1. theta * qty * 100 = -0.05 * -1 * 100 = +5
        # In cents: 500
        self.assertEqual(result["theta_cents"], 500)

    def test_multiple_expirations_fetch_each_chain(self):
        """Two positions in different expirations should trigger two
        chain fetches."""
        trader = MagicMock()
        def chain_side_effect(sym, exp, greeks=True):
            if exp == "2026-05-30":
                return [self._make_chain_option(
                    "SPY260530P00435000", 435, "put", -0.16)]
            if exp == "2026-06-27":
                return [self._make_chain_option(
                    "SPY260627P00430000", 430, "put", -0.14)]
            return []
        trader.get_option_chain.side_effect = chain_side_effect
        trader.get_quote.return_value = {"last": 450.0}
        positions = [
            {"symbol": "SPY260530P00435000", "quantity": -1},
            {"symbol": "SPY260627P00430000", "quantity": -1},
        ]
        aggregate_portfolio_greeks(trader, positions)
        self.assertEqual(trader.get_option_chain.call_count, 2)

    def test_unknown_symbol_in_chain_is_skipped(self):
        """If a position's OCC symbol isn't in the chain response,
        skip it rather than crash."""
        trader = MagicMock()
        trader.get_option_chain.return_value = [
            # Chain contains different strike than the position
            self._make_chain_option("SPY260530P00440000", 440, "put", -0.20),
        ]
        trader.get_quote.return_value = {"last": 450.0}
        positions = [{"symbol": "SPY260530P00435000", "quantity": -1}]
        result = aggregate_portfolio_greeks(trader, positions)
        # No match -> zero aggregated
        self.assertEqual(result["delta_cents"], 0)

    def test_malformed_symbol_skipped(self):
        trader = MagicMock()
        positions = [{"symbol": "GARBAGE", "quantity": -1}]
        result = aggregate_portfolio_greeks(trader, positions)
        self.assertEqual(result["delta_cents"], 0)
        trader.get_option_chain.assert_not_called()

    def test_chain_fetch_failure_does_not_crash(self):
        trader = MagicMock()
        trader.get_option_chain.side_effect = RuntimeError("API down")
        trader.get_quote.return_value = {"last": 450.0}
        positions = [{"symbol": "SPY260530P00435000", "quantity": -1}]
        result = aggregate_portfolio_greeks(trader, positions)
        self.assertEqual(result["delta_cents"], 0)

    def test_zero_quantity_skipped(self):
        trader = MagicMock()
        trader.get_option_chain.return_value = [
            self._make_chain_option("SPY260530P00435000", 435, "put", -0.16),
        ]
        trader.get_quote.return_value = {"last": 450.0}
        positions = [{"symbol": "SPY260530P00435000", "quantity": 0}]
        result = aggregate_portfolio_greeks(trader, positions)
        self.assertEqual(result["delta_cents"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

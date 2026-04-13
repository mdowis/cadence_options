"""Tests for iv_rank.py."""

import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.iv_rank import (
    compute_iv_rank,
    compute_iv_percentile,
    get_cached_iv_rank,
    clear_cache,
    get_iv_rank_from_index,
    get_atm_iv_from_chain,
    IVHistoryStore,
    VOLATILITY_INDEX_SYMBOLS,
)


class TestIVRank(unittest.TestCase):

    def test_basic_rank(self):
        # current=20, min=10, max=30 -> (20-10)/(30-10)*100 = 50
        self.assertAlmostEqual(compute_iv_rank(20, [10, 15, 25, 30]), 50.0)

    def test_rank_at_min(self):
        self.assertAlmostEqual(compute_iv_rank(10, [10, 20, 30]), 0.0)

    def test_rank_at_max(self):
        self.assertAlmostEqual(compute_iv_rank(30, [10, 20, 30]), 100.0)

    def test_rank_above_max(self):
        # Can exceed 100 if current > historical max
        self.assertAlmostEqual(compute_iv_rank(40, [10, 20, 30]), 150.0)

    def test_rank_empty_history(self):
        self.assertEqual(compute_iv_rank(20, []), 0.0)

    def test_rank_flat_history(self):
        self.assertEqual(compute_iv_rank(20, [20, 20, 20]), 0.0)

    def test_percentile_basic(self):
        # 3 of 5 values below 25
        self.assertAlmostEqual(compute_iv_percentile(25, [10, 15, 20, 30, 35]), 60.0)

    def test_percentile_all_below(self):
        self.assertAlmostEqual(compute_iv_percentile(100, [10, 20, 30]), 100.0)

    def test_percentile_none_below(self):
        self.assertAlmostEqual(compute_iv_percentile(5, [10, 20, 30]), 0.0)

    def test_percentile_empty(self):
        self.assertEqual(compute_iv_percentile(20, []), 0.0)


class TestIVRankCache(unittest.TestCase):

    def setUp(self):
        clear_cache()

    def test_cache_returns_same_value(self):
        history = [10, 20, 30]
        r1 = get_cached_iv_rank("SPY", 20, history)
        r2 = get_cached_iv_rank("SPY", 20, history)
        self.assertEqual(r1, r2)

    def test_cache_uses_cached_value(self):
        history = [10, 20, 30]
        r1 = get_cached_iv_rank("SPY", 20, history)
        # Even with different inputs, should return cached
        r2 = get_cached_iv_rank("SPY", 25, [10, 20, 30, 40])
        self.assertEqual(r1, r2)

    def test_cache_expires(self):
        history = [10, 20, 30]
        r1 = get_cached_iv_rank("SPY", 20, history)
        self.assertAlmostEqual(r1, 50.0)
        # Expire the cache by patching time far in the future
        future = time.time() + 3700
        with patch("cadence.iv_rank.time.time", return_value=future):
            # Different current_iv and wider range -> different rank
            r2 = get_cached_iv_rank("SPY", 15, [10, 40])
        # 15 in [10..40] = (15-10)/(40-10)*100 = 16.67
        self.assertAlmostEqual(r2, 16.67, places=1)

    def test_different_symbols_independent(self):
        r1 = get_cached_iv_rank("SPY", 20, [10, 30])
        r2 = get_cached_iv_rank("QQQ", 25, [10, 30])
        self.assertAlmostEqual(r1, 50.0)
        self.assertAlmostEqual(r2, 75.0)


class TestVolatilityIndexMapping(unittest.TestCase):

    def test_spy_maps_to_vix(self):
        self.assertEqual(VOLATILITY_INDEX_SYMBOLS["SPY"], "VIX")

    def test_qqq_maps_to_vxn(self):
        self.assertEqual(VOLATILITY_INDEX_SYMBOLS["QQQ"], "VXN")

    def test_iwm_maps_to_rvx(self):
        self.assertEqual(VOLATILITY_INDEX_SYMBOLS["IWM"], "RVX")


class TestGetIVRankFromIndex(unittest.TestCase):

    def test_unsupported_symbol_returns_none(self):
        trader = MagicMock()
        result = get_iv_rank_from_index(trader, "TSLA")
        self.assertIsNone(result)

    def test_spy_fetches_vix(self):
        trader = MagicMock()
        trader.get_quote.return_value = {"last": 16.5}
        trader.get_history.return_value = [
            {"close": 12.0}, {"close": 14.0}, {"close": 16.0},
            {"close": 18.0}, {"close": 22.0},
        ]
        # current=16.5 in [12..22] = (16.5-12)/(22-12)*100 = 45%
        result = get_iv_rank_from_index(trader, "SPY")
        self.assertAlmostEqual(result["rank"], 45.0, places=1)
        self.assertEqual(result["current"], 16.5)
        self.assertEqual(result["min"], 12.0)
        self.assertEqual(result["max"], 22.0)
        self.assertEqual(result["source"], "VIX")
        self.assertEqual(result["history_points"], 5)
        # Verify it queried VIX, not SPY
        trader.get_quote.assert_called_once_with("VIX")
        trader.get_history.assert_called_once()
        self.assertEqual(trader.get_history.call_args[0][0], "VIX")

    def test_qqq_fetches_vxn(self):
        trader = MagicMock()
        trader.get_quote.return_value = {"last": 20.0}
        trader.get_history.return_value = [{"close": 15.0}, {"close": 25.0}]
        result = get_iv_rank_from_index(trader, "QQQ")
        self.assertEqual(result["source"], "VXN")
        trader.get_quote.assert_called_once_with("VXN")

    def test_no_quote_falls_back_to_latest_close(self):
        """When the live quote is unusable, fall back to the latest
        history close so IV rank is still computable."""
        trader = MagicMock()
        trader.get_quote.return_value = {}  # no usable fields
        trader.get_history.return_value = [
            {"close": 12.0}, {"close": 18.0}, {"close": 20.0},
        ]
        result = get_iv_rank_from_index(trader, "SPY")
        # current = 20 (latest close), range [12..20] -> rank = 100
        self.assertEqual(result["current"], 20.0)
        self.assertAlmostEqual(result["rank"], 100.0, places=1)
        self.assertIn("latest close", result["source"])

    def test_empty_history_returns_error_dict(self):
        trader = MagicMock()
        trader.get_quote.return_value = {"last": 18.0}
        trader.get_history.return_value = []
        result = get_iv_rank_from_index(trader, "SPY")
        self.assertEqual(result["rank"], 0.0)
        self.assertIn("error", result)
        self.assertIn("no history", result["error"])

    def test_api_error_returns_error_dict(self):
        trader = MagicMock()
        trader.get_history.side_effect = RuntimeError("API down")
        result = get_iv_rank_from_index(trader, "SPY")
        self.assertEqual(result["rank"], 0.0)
        self.assertIn("API down", result["error"])

    def test_quote_price_missing_last_but_has_close(self):
        """If `last` is null but `close` is present, use close."""
        trader = MagicMock()
        trader.get_quote.return_value = {"last": None, "close": 16.5}
        trader.get_history.return_value = [{"close": 10.0}, {"close": 20.0}]
        result = get_iv_rank_from_index(trader, "SPY")
        self.assertEqual(result["current"], 16.5)
        self.assertAlmostEqual(result["rank"], 65.0, places=1)
        self.assertNotIn("latest close", result["source"])

    def test_bid_ask_midpoint_fallback(self):
        """If only bid/ask are available, use midpoint."""
        trader = MagicMock()
        trader.get_quote.return_value = {"bid": 15.0, "ask": 17.0}
        trader.get_history.return_value = [{"close": 10.0}, {"close": 20.0}]
        result = get_iv_rank_from_index(trader, "SPY")
        self.assertEqual(result["current"], 16.0)


class TestGetAtmIVFromChain(unittest.TestCase):

    def _make_option(self, strike, option_type, iv):
        return {
            "strike": strike,
            "option_type": option_type,
            "greeks": {"mid_iv": iv},
        }

    def test_averages_atm_call_and_put(self):
        chain = [
            self._make_option(440, "put", 0.20),
            self._make_option(450, "put", 0.18),   # ATM put
            self._make_option(450, "call", 0.22),  # ATM call
            self._make_option(460, "call", 0.25),
        ]
        iv = get_atm_iv_from_chain(chain, spot_price=450)
        self.assertAlmostEqual(iv, 0.20)

    def test_picks_closest_strike(self):
        chain = [
            self._make_option(445, "put", 0.17),   # closer to 448
            self._make_option(455, "put", 0.19),
            self._make_option(445, "call", 0.23),
            self._make_option(455, "call", 0.21),
        ]
        iv = get_atm_iv_from_chain(chain, spot_price=448)
        self.assertAlmostEqual(iv, 0.20)  # avg of 0.17 and 0.23

    def test_empty_chain_returns_none(self):
        self.assertIsNone(get_atm_iv_from_chain([], 450))

    def test_no_spot_returns_none(self):
        chain = [self._make_option(450, "call", 0.2)]
        self.assertIsNone(get_atm_iv_from_chain(chain, 0))

    def test_no_greeks_returns_none(self):
        chain = [
            {"strike": 450, "option_type": "put"},
            {"strike": 450, "option_type": "call"},
        ]
        self.assertIsNone(get_atm_iv_from_chain(chain, 450))


class TestIVHistoryStore(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(
            suffix=".json", delete=False, mode="w")
        self.tmp.close()
        self.path = self.tmp.name

    def tearDown(self):
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def test_record_persists(self):
        store = IVHistoryStore(self.path)
        store.record_daily_iv("SPY", 0.20)

        # New instance reads from file
        store2 = IVHistoryStore(self.path)
        with store2._lock:
            series = store2._data["SPY"]
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0][1], 0.20)

    def test_record_same_day_overwrites(self):
        store = IVHistoryStore(self.path)
        store.record_daily_iv("SPY", 0.20)
        store.record_daily_iv("SPY", 0.25)
        with store._lock:
            series = store._data["SPY"]
        self.assertEqual(len(series), 1)
        self.assertEqual(series[0][1], 0.25)

    def test_insufficient_history(self):
        store = IVHistoryStore(self.path)
        store.record_daily_iv("TSLA", 0.35)
        result = store.get_iv_rank("TSLA", 0.40, min_points=20)
        self.assertEqual(result["rank"], 0.0)
        self.assertIn("insufficient", result["error"])

    def test_sufficient_history_computes_rank(self):
        store = IVHistoryStore(self.path)
        # Manually populate enough history
        with store._lock:
            store._data["TSLA"] = [
                ["2026-01-01", 0.20], ["2026-01-02", 0.25],
                ["2026-01-03", 0.30], ["2026-01-04", 0.35],
                ["2026-01-05", 0.40], ["2026-01-06", 0.45],
                ["2026-01-07", 0.50], ["2026-01-08", 0.55],
                ["2026-01-09", 0.60], ["2026-01-10", 0.65],
            ]
        result = store.get_iv_rank("TSLA", 0.42, min_points=10)
        # 0.42 in [0.20..0.65] = (0.42-0.20)/(0.65-0.20)*100 = ~48.9%
        self.assertAlmostEqual(result["rank"], 48.9, places=0)
        self.assertEqual(result["source"], "local")

    def test_missing_state_file(self):
        """Loading from nonexistent file should be graceful."""
        store = IVHistoryStore("/nonexistent/path/ivhistory.json")
        self.assertEqual(store._data, {})


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Tests for iv_rank.py."""

import os
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.iv_rank import (
    compute_iv_rank,
    compute_iv_percentile,
    get_cached_iv_rank,
    clear_cache,
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


if __name__ == "__main__":
    unittest.main(verbosity=2)

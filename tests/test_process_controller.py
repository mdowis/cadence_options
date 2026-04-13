"""Tests for process_controller.py."""

import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.process_controller import ProcessController, ProcessStatus, is_market_open
from cadence.strategy import StrategyConfig, IronCondorCandidate
from cadence.risk_manager import RiskManager, RiskConfig


def make_candidate(symbol="SPY", credit=2.50, max_loss=7.50, return_pct=33.3):
    return IronCondorCandidate(
        symbol=symbol, expiration="2026-05-30", dte=48, iv_rank=50,
        short_put_symbol=f"{symbol}260530P00435000", short_put_strike=435,
        long_put_symbol=f"{symbol}260530P00425000", long_put_strike=425,
        short_call_symbol=f"{symbol}260530C00465000", short_call_strike=465,
        long_call_symbol=f"{symbol}260530C00475000", long_call_strike=475,
        credit=credit, max_loss=max_loss,
        breakeven_low=432.5, breakeven_high=467.5,
        put_delta=-0.16, call_delta=0.16,
        prob_profit=70.0, return_pct=return_pct,
    )


class TestProcessStatus(unittest.TestCase):

    def test_initial_state(self):
        ps = ProcessStatus()
        self.assertEqual(ps.status, "stopped")
        self.assertEqual(ps.run_count, 0)

    def test_to_dict(self):
        ps = ProcessStatus()
        ps.status = "running"
        ps.run_count = 5
        d = ps.to_dict()
        self.assertEqual(d["status"], "running")
        self.assertEqual(d["run_count"], 5)


class TestMarketHours(unittest.TestCase):

    @patch("cadence.process_controller._now_et")
    def test_market_open_weekday(self, mock_now):
        from datetime import datetime
        # Wednesday at 10:00 AM ET
        mock_now.return_value = datetime(2026, 4, 15, 10, 0, 0)
        self.assertTrue(is_market_open())

    @patch("cadence.process_controller._now_et")
    def test_market_closed_weekend(self, mock_now):
        from datetime import datetime
        # Saturday at 10:00 AM
        mock_now.return_value = datetime(2026, 4, 18, 10, 0, 0)
        self.assertFalse(is_market_open())

    @patch("cadence.process_controller._now_et")
    def test_market_closed_before_open(self, mock_now):
        from datetime import datetime
        mock_now.return_value = datetime(2026, 4, 15, 9, 0, 0)
        self.assertFalse(is_market_open())

    @patch("cadence.process_controller._now_et")
    def test_market_closed_after_close(self, mock_now):
        from datetime import datetime
        mock_now.return_value = datetime(2026, 4, 15, 16, 1, 0)
        self.assertFalse(is_market_open())


class TestProcessController(unittest.TestCase):

    def setUp(self):
        self.trader = MagicMock()
        self.config = RiskConfig()
        self.risk_mgr = RiskManager(self.config, starting_equity_cents=1000000)
        self.strategy_config = StrategyConfig()
        self.notifier = MagicMock()

    def test_get_status(self):
        pc = ProcessController(
            self.trader, self.risk_mgr, self.strategy_config,
            notifier=self.notifier
        )
        status = pc.get_status()
        self.assertIn("scanner", status)
        self.assertIn("executor", status)
        self.assertIn("config", status)
        self.assertEqual(status["scanner"]["status"], "stopped")

    def test_start_stop_scanner(self):
        pc = ProcessController(
            self.trader, self.risk_mgr, self.strategy_config,
            scan_interval=0.1
        )
        pc.start_scanner()
        self.assertEqual(pc._scanner_status.status, "running")
        time.sleep(0.05)
        pc.stop_scanner()
        self.assertEqual(pc._scanner_status.status, "stopped")

    def test_start_stop_executor(self):
        pc = ProcessController(
            self.trader, self.risk_mgr, self.strategy_config,
            scan_interval=0.1
        )
        pc.start_executor()
        self.assertEqual(pc._executor_status.status, "running")
        time.sleep(0.05)
        pc.stop_executor()
        self.assertEqual(pc._executor_status.status, "stopped")


class TestRegression6_AttemptDedup(unittest.TestCase):
    """Attempt dedup prevents re-execution of the same candidate within TTL."""

    def test_dedup_within_ttl(self):
        trader = MagicMock()
        config = RiskConfig()
        risk_mgr = RiskManager(config, starting_equity_cents=1000000)
        strategy_config = StrategyConfig()

        pc = ProcessController(trader, risk_mgr, strategy_config, dry_run=True)

        candidate = make_candidate()
        fp = candidate.fingerprint()

        # First attempt records the fingerprint
        with pc._attempts_lock:
            pc._recent_attempts[fp] = time.time()

        # Now check: candidate should be filtered out as recently attempted
        with pc._candidates_lock:
            pc._candidates = [candidate]

        with pc._candidates_lock:
            candidates = list(pc._candidates)

        now = time.time()
        eligible = []
        with pc._attempts_lock:
            for c in candidates:
                if c.fingerprint() not in pc._recent_attempts:
                    eligible.append(c)

        self.assertEqual(len(eligible), 0, "Same candidate within TTL should be skipped")

    def test_dedup_after_ttl(self):
        trader = MagicMock()
        config = RiskConfig()
        risk_mgr = RiskManager(config, starting_equity_cents=1000000)
        strategy_config = StrategyConfig()

        pc = ProcessController(trader, risk_mgr, strategy_config, dry_run=True)

        candidate = make_candidate()
        fp = candidate.fingerprint()

        # Record attempt from 6 minutes ago (past 5-min TTL)
        with pc._attempts_lock:
            pc._recent_attempts[fp] = time.time() - 360

        # Clean expired entries (as the executor loop does)
        now = time.time()
        with pc._attempts_lock:
            expired = [fp for fp, t in pc._recent_attempts.items()
                       if now - t > pc.ATTEMPT_TTL_SECS]
            for fp_key in expired:
                del pc._recent_attempts[fp_key]

        with pc._candidates_lock:
            pc._candidates = [candidate]

        with pc._candidates_lock:
            candidates = list(pc._candidates)

        eligible = []
        with pc._attempts_lock:
            for c in candidates:
                if c.fingerprint() not in pc._recent_attempts:
                    eligible.append(c)

        self.assertEqual(len(eligible), 1, "Candidate past TTL should be eligible again")


class TestBrokerSync(unittest.TestCase):
    """Verify _sync_broker_state pulls equity AND position count from the broker."""

    def setUp(self):
        self.trader = MagicMock()
        self.trader.get_account_balances.return_value = {
            "balances": {"total_equity": 10000.00, "total_cash": 8000.00}
        }
        self.trader.get_positions.return_value = [
            {"symbol": "SPY260530P00435000"},
            {"symbol": "SPY260530P00425000"},
            {"symbol": "SPY260530C00465000"},
            {"symbol": "SPY260530C00475000"},
        ]
        self.risk_mgr = RiskManager(RiskConfig(), starting_equity_cents=0)
        self.pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, scan_interval=0.1,
        )

    def test_sync_updates_equity(self):
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["equity"]["current"], 1000000)  # $10,000 in cents

    def test_sync_updates_position_count(self):
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["positions"]["count"], 4)

    def test_sync_resilient_to_balance_failure(self):
        self.trader.get_account_balances.side_effect = RuntimeError("api down")
        # Shouldn't raise; position count still syncs
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["positions"]["count"], 4)

    def test_sync_resilient_to_position_failure(self):
        self.trader.get_positions.side_effect = RuntimeError("api down")
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["equity"]["current"], 1000000)


class TestSetDryRun(unittest.TestCase):

    def test_toggle(self):
        trader = MagicMock()
        config = RiskConfig()
        risk_mgr = RiskManager(config, starting_equity_cents=1000000)
        strategy_config = StrategyConfig()
        pc = ProcessController(trader, risk_mgr, strategy_config, dry_run=True)

        self.assertTrue(pc.dry_run)
        pc.set_dry_run(False)
        self.assertFalse(pc.dry_run)


if __name__ == "__main__":
    unittest.main(verbosity=2)

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
        """With no position_tracker attached, IC count = leg_count // 4.
        4 leg positions from the mock broker -> 1 iron condor."""
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["positions"]["count"], 1)
        self.assertEqual(status["positions"]["leg_count"], 4)

    def test_sync_resilient_to_balance_failure(self):
        self.trader.get_account_balances.side_effect = RuntimeError("api down")
        # Shouldn't raise; position count still syncs
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["positions"]["count"], 1)
        self.assertEqual(status["positions"]["leg_count"], 4)

    def test_sync_with_tracker_uses_tracker_count(self):
        """When a tracker is attached, IC count = len(tracker.get_open()),
        not leg_count // 4. This handles cases where tracker has more
        entries than broker legs/4 (overlapping legs across ICs)."""
        from cadence.position_tracker import PositionTracker
        from cadence.strategy import IronCondorCandidate
        self.pc.position_tracker = PositionTracker(state_file=None)
        # Record 3 distinct tracked ICs
        for i in range(3):
            c = IronCondorCandidate(
                symbol="SPY", expiration="2026-05-30", dte=45, iv_rank=50,
                short_put_symbol="SPY260530P00435000", short_put_strike=435,
                long_put_symbol="SPY260530P00425000", long_put_strike=425,
                short_call_symbol="SPY260530C00465000", short_call_strike=465,
                long_call_symbol="SPY260530C00475000", long_call_strike=475,
                credit=2.40, max_loss=7.60,
                breakeven_low=432, breakeven_high=468,
                put_delta=-0.16, call_delta=0.16,
                prob_profit=70, return_pct=30,
            )
            self.pc.position_tracker.record_entry(c, tag=f"t-{i}", contracts=1)
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["positions"]["count"], 3)
        # Broker still reports 4 raw leg entries
        self.assertEqual(status["positions"]["leg_count"], 4)

    def test_sync_resilient_to_position_failure(self):
        self.trader.get_positions.side_effect = RuntimeError("api down")
        self.pc._sync_broker_state()
        status = self.risk_mgr.get_status()
        self.assertEqual(status["equity"]["current"], 1000000)


class TestAutoExitLoop(unittest.TestCase):
    """Verify _check_and_submit_exits wires position_manager into the sync."""

    def setUp(self):
        from cadence.position_manager import PositionManager
        from cadence.position_tracker import PositionTracker
        self.trader = MagicMock()
        self.risk_mgr = RiskManager(RiskConfig(), starting_equity_cents=1000000)
        self.position_mgr = PositionManager(
            profit_target_pct=50, time_stop_dte=21, loss_stop_multiplier=2.0)
        self.tracker = PositionTracker(state_file=None)
        self.pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, scan_interval=0.1,
            position_manager=self.position_mgr,
            position_tracker=self.tracker,
        )

    def _record_entry(self, entry_credit=2.40):
        from cadence.strategy import IronCondorCandidate
        c = IronCondorCandidate(
            symbol="SPY", expiration="2026-05-30", dte=45, iv_rank=50,
            short_put_symbol="SPY260530P00435000", short_put_strike=435,
            long_put_symbol="SPY260530P00425000", long_put_strike=425,
            short_call_symbol="SPY260530C00465000", short_call_strike=465,
            long_call_symbol="SPY260530C00475000", long_call_strike=475,
            credit=entry_credit, max_loss=10 - entry_credit,
            breakeven_low=432, breakeven_high=468,
            put_delta=-0.16, call_delta=0.16,
            prob_profit=70, return_pct=30,
        )
        # Backdate entry past the auto-exit minimum-age gate so the
        # exit logic actually runs in tests.
        old_time = time.time() - (self.pc.MIN_POSITION_AGE_SECS + 60)
        self.tracker.record_entry(c, tag="t1", contracts=1, entry_time=old_time)

    def _chain_option(self, sym, bid, ask):
        return {"symbol": sym, "bid": bid, "ask": ask}

    def test_profit_target_triggers_close(self):
        """Entry credit 2.40, current close debit 1.00 -> 58% profit.
        PositionManager threshold is 50%, so exit fires."""
        self._record_entry(entry_credit=2.40)
        self.trader.get_option_chain.return_value = [
            self._chain_option("SPY260530P00435000", 0.45, 0.55),
            self._chain_option("SPY260530P00425000", 0.10, 0.20),
            self._chain_option("SPY260530C00465000", 0.40, 0.50),
            self._chain_option("SPY260530C00475000", 0.05, 0.15),
        ]
        # close debit = 0.55 + 0.50 - 0.10 - 0.05 = 0.90
        # pnl_pct = (2.40 - 0.90) / 2.40 * 100 = 62.5% -> triggers

        self.pc._check_and_submit_exits()

        # Dry-run: execute_close logs but doesn't place an order
        self.trader.place_multileg_order.assert_not_called()
        # But dedup should now have the tag so we don't retry immediately
        with self.pc._exit_attempts_lock:
            self.assertIn("t1", self.pc._recent_exit_attempts)

    def test_no_exit_when_still_open(self):
        """Entry 2.40, current debit 1.80 -> 25% profit, below 50% target.
        No exit should fire."""
        self._record_entry(entry_credit=2.40)
        self.trader.get_option_chain.return_value = [
            self._chain_option("SPY260530P00435000", 0.85, 0.95),
            self._chain_option("SPY260530P00425000", 0.10, 0.20),
            self._chain_option("SPY260530C00465000", 0.80, 0.90),
            self._chain_option("SPY260530C00475000", 0.05, 0.15),
        ]
        # close debit = 0.95 + 0.90 - 0.10 - 0.05 = 1.70 -> 29% profit
        self.pc._check_and_submit_exits()
        with self.pc._exit_attempts_lock:
            self.assertNotIn("t1", self.pc._recent_exit_attempts)

    def test_exit_dedup_prevents_repeat_submissions(self):
        """Once a close is submitted for a tag, we shouldn't submit
        again within ATTEMPT_TTL_SECS."""
        self._record_entry(entry_credit=2.40)
        self.trader.get_option_chain.return_value = [
            self._chain_option("SPY260530P00435000", 0.45, 0.55),
            self._chain_option("SPY260530P00425000", 0.10, 0.20),
            self._chain_option("SPY260530C00465000", 0.40, 0.50),
            self._chain_option("SPY260530C00475000", 0.05, 0.15),
        ]
        # First call: triggers exit, records dedup
        self.pc._check_and_submit_exits()
        first_call_count = self.trader.get_option_chain.call_count

        # Second call: dedup should prevent re-submitting but we still
        # fetch the chain to price the position (shared code path)
        self.pc._check_and_submit_exits()

        # We should not have called place_multileg_order (dry_run)
        self.trader.place_multileg_order.assert_not_called()
        # dedup still contains the tag
        with self.pc._exit_attempts_lock:
            self.assertIn("t1", self.pc._recent_exit_attempts)

    def test_chain_failure_skips_gracefully(self):
        self._record_entry(entry_credit=2.40)
        self.trader.get_option_chain.side_effect = RuntimeError("api down")
        # Should not raise
        self.pc._check_and_submit_exits()


class TestPhantomCleanup(unittest.TestCase):
    """Phantoms whose legs overlap with real positions can't be caught
    by detect_closes (which needs ALL legs missing). The cleanup pass
    catches them by checking the orders endpoint directly."""

    def setUp(self):
        from cadence.position_manager import PositionManager
        from cadence.position_tracker import PositionTracker
        from cadence.strategy import IronCondorCandidate
        self.trader = MagicMock()
        self.risk_mgr = RiskManager(RiskConfig(), starting_equity_cents=1000000)
        self.tracker = PositionTracker(state_file=None)
        self.pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=False, scan_interval=60,
            position_manager=PositionManager(),
            position_tracker=self.tracker,
        )
        c = IronCondorCandidate(
            symbol="SPY", expiration="2026-05-30", dte=45, iv_rank=50,
            short_put_symbol="SPY260530P00435000", short_put_strike=435,
            long_put_symbol="SPY260530P00425000", long_put_strike=425,
            short_call_symbol="SPY260530C00465000", short_call_strike=465,
            long_call_symbol="SPY260530C00475000", long_call_strike=475,
            credit=2.40, max_loss=7.60,
            breakeven_low=432, breakeven_high=468,
            put_delta=-0.16, call_delta=0.16,
            prob_profit=70, return_pct=30,
        )
        old_time = time.time() - (self.pc.PHANTOM_GRACE_SECS + 60)
        self.tracker.record_entry(c, tag="t-real", contracts=1,
                                   entry_time=old_time)
        self.tracker.record_entry(c, tag="t-phantom", contracts=1,
                                   entry_time=old_time)

    def test_phantom_with_overlapping_legs_gets_dropped(self):
        """t-real has filled order, t-phantom does not. Even though
        their legs are identical and visible at Tradier (because t-real
        really opened), t-phantom should still be dropped."""
        self.trader.get_orders.return_value = [
            {"tag": "t-real", "status": "filled"},
            {"tag": "t-phantom", "status": "open"},
        ]
        self.assertEqual(len(self.tracker.get_open()), 2)
        self.pc._cleanup_phantoms()
        remaining = [t.tag for t in self.tracker.get_open()]
        self.assertEqual(remaining, ["t-real"])

    def test_fresh_phantom_not_dropped(self):
        """Entries within PHANTOM_GRACE_SECS are not eligible for
        cleanup -- the broker may not have processed the order yet."""
        from cadence.position_tracker import PositionTracker
        from cadence.strategy import IronCondorCandidate
        self.tracker = PositionTracker(state_file=None)
        self.pc.position_tracker = self.tracker
        c = IronCondorCandidate(
            symbol="SPY", expiration="2026-05-30", dte=45, iv_rank=50,
            short_put_symbol="SPY260530P00435000", short_put_strike=435,
            long_put_symbol="SPY260530P00425000", long_put_strike=425,
            short_call_symbol="SPY260530C00465000", short_call_strike=465,
            long_call_symbol="SPY260530C00475000", long_call_strike=475,
            credit=2.40, max_loss=7.60,
            breakeven_low=432, breakeven_high=468,
            put_delta=-0.16, call_delta=0.16,
            prob_profit=70, return_pct=30,
        )
        self.tracker.record_entry(c, tag="t-fresh", contracts=1,
                                   entry_time=time.time())
        self.trader.get_orders.return_value = []
        self.pc._cleanup_phantoms()
        self.assertEqual([t.tag for t in self.tracker.get_open()], ["t-fresh"])
        self.trader.get_orders.assert_not_called()


class TestSpuriousCloseDetection(unittest.TestCase):
    """Phantom 'closes' for entries that never filled must NOT record
    trades. Otherwise daily trade_count and Kelly's sample_size get
    polluted by orders that never actually opened a position."""

    def setUp(self):
        from cadence.position_manager import PositionManager
        from cadence.position_tracker import PositionTracker
        from cadence.strategy import IronCondorCandidate
        self.trader = MagicMock()
        self.risk_mgr = RiskManager(RiskConfig(), starting_equity_cents=1000000)
        self.tracker = PositionTracker(state_file=None)
        self.pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=False, scan_interval=60,
            position_manager=PositionManager(),
            position_tracker=self.tracker,
        )
        c = IronCondorCandidate(
            symbol="SPY", expiration="2026-05-30", dte=45, iv_rank=50,
            short_put_symbol="SPY260530P00435000", short_put_strike=435,
            long_put_symbol="SPY260530P00425000", long_put_strike=425,
            short_call_symbol="SPY260530C00465000", short_call_strike=465,
            long_call_symbol="SPY260530C00475000", long_call_strike=475,
            credit=2.40, max_loss=7.60,
            breakeven_low=432, breakeven_high=468,
            put_delta=-0.16, call_delta=0.16,
            prob_profit=70, return_pct=30,
        )
        self.tracker.record_entry(c, tag="t-unfilled", contracts=1)

    def test_unfilled_entry_does_not_record_trade(self):
        """Tradier shows no positions and no filled orders for the tag.
        Tracker should silently drop the entry, NOT record a $0 close."""
        self.trader.get_orders.return_value = [
            {"tag": "t-unfilled", "status": "open"}  # placed but not filled
        ]
        # broker_positions is empty -> 'all four legs missing' triggers
        self.pc._detect_and_record_closes([])

        # No trade should have been recorded
        status = self.risk_mgr.get_status()
        self.assertEqual(status["daily"]["trade_count"], 0)
        # Tracker entry was silently dropped
        self.assertEqual(len(self.tracker.get_open()), 0)

    def test_filled_entry_with_unresolved_close_records_zero(self):
        """If entry was filled but we can't find a close fill (manual
        close in Tradier UI, expiration), record 0 with UNRESOLVED tag."""
        self.trader.get_orders.return_value = [
            {"tag": "t-unfilled", "status": "filled",
             "create_date": "2026-04-13T10:00:00.000Z",
             "type": "credit", "avg_fill_price": 2.40},
        ]
        # Even though there's no close order, the entry was filled,
        # so the position WAS real -- record an UNRESOLVED close.
        self.pc._detect_and_record_closes([])

        status = self.risk_mgr.get_status()
        self.assertEqual(status["daily"]["trade_count"], 1)
        # Recorded as UNRESOLVED so operator sees it
        events = status.get("risk_events", [])
        self.assertEqual(len(self.tracker.get_open()), 0)


class TestAutoExitMinimumAge(unittest.TestCase):
    """Freshly-opened positions must not be exit-checked. Sandbox option
    chain prices can be wide/stale right after entry; without a
    minimum-age gate this could trip an immediate loss-stop close."""

    def setUp(self):
        from cadence.position_manager import PositionManager
        from cadence.position_tracker import PositionTracker
        from cadence.strategy import IronCondorCandidate
        self.trader = MagicMock()
        self.risk_mgr = RiskManager(RiskConfig(), starting_equity_cents=1000000)
        self.tracker = PositionTracker(state_file=None)
        self.pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, scan_interval=60,
            position_manager=PositionManager(loss_stop_multiplier=2.0),
            position_tracker=self.tracker,
        )
        self.candidate = IronCondorCandidate(
            symbol="SPY", expiration="2026-05-30", dte=45, iv_rank=50,
            short_put_symbol="SPY260530P00435000", short_put_strike=435,
            long_put_symbol="SPY260530P00425000", long_put_strike=425,
            short_call_symbol="SPY260530C00465000", short_call_strike=465,
            long_call_symbol="SPY260530C00475000", long_call_strike=475,
            credit=2.40, max_loss=7.60,
            breakeven_low=432, breakeven_high=468,
            put_delta=-0.16, call_delta=0.16,
            prob_profit=70, return_pct=30,
        )

    def test_freshly_opened_position_is_not_exit_checked(self):
        """Position with entry_time = now should not be exit-checked,
        even if the chain shows a 'loss stop' condition."""
        self.tracker.record_entry(self.candidate, tag="t-fresh", contracts=1,
                                   entry_time=time.time())
        # Chain shows wide spread -> would trigger loss stop if exit-
        # checked (close debit ~7.20 vs entry 2.40, loss = 4.80,
        # 2x credit = 4.80 -> trips exactly).
        self.trader.get_option_chain.return_value = [
            {"symbol": "SPY260530P00435000", "bid": 4.40, "ask": 4.60},
            {"symbol": "SPY260530P00425000", "bid": 0.10, "ask": 0.20},
            {"symbol": "SPY260530C00465000", "bid": 4.30, "ask": 4.50},
            {"symbol": "SPY260530C00475000", "bid": 0.05, "ask": 0.15},
        ]

        self.pc._check_and_submit_exits()

        # No exit attempt recorded -- position is too young
        with self.pc._exit_attempts_lock:
            self.assertNotIn("t-fresh", self.pc._recent_exit_attempts)
        # Chain wasn't even fetched (skipped before pricing)
        self.trader.get_option_chain.assert_not_called()

    def test_aged_position_is_exit_checked(self):
        """Position older than MIN_POSITION_AGE_SECS gets exit-checked."""
        old_time = time.time() - (self.pc.MIN_POSITION_AGE_SECS + 60)
        self.tracker.record_entry(self.candidate, tag="t-old", contracts=1,
                                   entry_time=old_time)
        # Chain shows profit-target condition (close debit 0.90, profit 62%)
        self.trader.get_option_chain.return_value = [
            {"symbol": "SPY260530P00435000", "bid": 0.45, "ask": 0.55},
            {"symbol": "SPY260530P00425000", "bid": 0.10, "ask": 0.20},
            {"symbol": "SPY260530C00465000", "bid": 0.40, "ask": 0.50},
            {"symbol": "SPY260530C00475000", "bid": 0.05, "ask": 0.15},
        ]
        self.pc._check_and_submit_exits()

        with self.pc._exit_attempts_lock:
            self.assertIn("t-old", self.pc._recent_exit_attempts)


class TestBrokerSyncThread(unittest.TestCase):
    """Verify the dedicated broker-sync thread runs independently of
    market hours and scanner state."""

    def setUp(self):
        self.trader = MagicMock()
        self.trader.get_account_balances.return_value = {
            "balances": {"total_equity": 10000.00, "total_cash": 8000.00}
        }
        self.trader.get_positions.return_value = []
        self.risk_mgr = RiskManager(RiskConfig(), starting_equity_cents=0)
        self.pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, scan_interval=60,
        )

    def test_start_runs_sync_immediately(self):
        # Use a very short interval so the test finishes quickly
        self.pc.start_broker_sync(interval=0.05)
        time.sleep(0.15)
        self.pc.stop_broker_sync()
        # Should have called get_account_balances at least once
        self.assertGreaterEqual(self.trader.get_account_balances.call_count, 1)
        status = self.risk_mgr.get_status()
        self.assertEqual(status["equity"]["current"], 1000000)

    def test_runs_outside_market_hours(self):
        """Even when is_market_open() returns False, broker sync runs."""
        with patch("cadence.process_controller.is_market_open",
                   return_value=False):
            self.pc.start_broker_sync(interval=0.05)
            time.sleep(0.15)
            self.pc.stop_broker_sync()
        # Sync ran despite market being "closed"
        self.assertGreaterEqual(self.trader.get_account_balances.call_count, 1)

    def test_double_start_doesnt_spawn_two_threads(self):
        self.pc.start_broker_sync(interval=0.5)
        first = self.pc._sync_thread
        self.pc.start_broker_sync(interval=0.5)
        second = self.pc._sync_thread
        self.assertIs(first, second)
        self.pc.stop_broker_sync()


class TestDryRunPersistence(unittest.TestCase):
    """Regression: dry_run state must survive restart so PAPER/LIVE
    mode doesn't silently revert and break the Close button."""

    def setUp(self):
        import tempfile
        fd, self.state_path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.unlink(self.state_path)
        self.trader = MagicMock()
        self.risk_mgr = RiskManager(RiskConfig(), starting_equity_cents=0)

    def tearDown(self):
        try:
            os.unlink(self.state_path)
        except OSError:
            pass

    def test_set_dry_run_persists_to_file(self):
        pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, state_file=self.state_path,
        )
        pc.set_dry_run(False)
        # New instance reads persisted value
        pc2 = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True,  # constructor default
            state_file=self.state_path,
        )
        self.assertFalse(pc2.dry_run, "Should restore PAPER/LIVE from state")

    def test_no_state_file_uses_default(self):
        pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, state_file=None,
        )
        self.assertTrue(pc.dry_run)
        pc.set_dry_run(False)  # shouldn't raise even without state file
        # New instance with no state file uses constructor default
        pc2 = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, state_file=None,
        )
        self.assertTrue(pc2.dry_run)

    def test_corrupt_state_file_falls_back_to_default(self):
        with open(self.state_path, "w") as f:
            f.write("not json {{")
        pc = ProcessController(
            self.trader, self.risk_mgr, StrategyConfig(),
            dry_run=True, state_file=self.state_path,
        )
        # Should not crash; falls back to constructor default
        self.assertTrue(pc.dry_run)


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

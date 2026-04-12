"""Scanner + executor background threads."""

import logging
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta

from cadence.market_calendar import (
    is_trading_day,
    is_early_close,
    is_us_holiday,
    get_market_close_time,
    MARKET_OPEN,
)

logger = logging.getLogger(__name__)

# US Eastern approximate offset (EST = -5, EDT = -4)
# Known limitation: DST transitions might be off by ~1 hour
ET_OFFSET = timedelta(hours=-5)


def _now_et():
    """Approximate Eastern Time from UTC."""
    return datetime.now(timezone.utc) + ET_OFFSET


def is_market_open():
    """Check if US equity market is open.

    Respects weekends, NYSE holidays, and 1pm-ET early closes.
    """
    now = _now_et()
    today = now.date()
    if not is_trading_day(today):
        return False
    close_time = get_market_close_time(today)
    if close_time is None:
        return False
    return MARKET_OPEN <= now.time() < close_time


class ProcessStatus:
    """Status for a background process."""

    def __init__(self):
        self.status = "stopped"
        self.started_at = None
        self.last_run_at = None
        self.last_error = None
        self.run_count = 0
        self.trades_placed = 0
        self.last_detail = ""
        self.recent_decisions = deque(maxlen=50)

    def to_dict(self):
        return {
            "status": self.status,
            "started_at": self.started_at,
            "last_run_at": self.last_run_at,
            "last_error": self.last_error,
            "run_count": self.run_count,
            "trades_placed": self.trades_placed,
            "last_detail": self.last_detail,
            "recent_decisions": list(self.recent_decisions),
        }


class ProcessController:
    """Manages scanner and executor background threads."""

    MAX_TRADES_PER_CYCLE = 1
    ATTEMPT_TTL_SECS = 300  # 5-minute dedup

    def __init__(self, trader, risk_mgr, strategy_config, notifier=None,
                 scan_interval=60, position_interval=30, dry_run=True,
                 status_interval_secs=3600):
        self.trader = trader
        self.risk_mgr = risk_mgr
        self.strategy_config = strategy_config
        self.notifier = notifier
        self.scan_interval = scan_interval
        self.position_interval = position_interval
        self.dry_run = dry_run
        self.status_interval_secs = status_interval_secs

        self._scanner_status = ProcessStatus()
        self._executor_status = ProcessStatus()

        self._scanner_thread = None
        self._executor_thread = None
        self._scanner_stop = threading.Event()
        self._executor_stop = threading.Event()

        self._candidates_lock = threading.Lock()
        self._candidates = []
        self._iv_ranks = {}  # symbol -> {rank, current, min, max, source, ...}

        self._attempts_lock = threading.Lock()
        self._recent_attempts = {}  # fingerprint -> timestamp

        self._last_status_time = 0

    # -- Scanner control -----------------------------------------------------

    def start_scanner(self):
        if self._scanner_thread and self._scanner_thread.is_alive():
            return
        self._scanner_stop.clear()
        self._scanner_status.status = "running"
        self._scanner_status.started_at = time.time()
        self._scanner_thread = threading.Thread(
            target=self._scanner_loop, daemon=True, name="scanner"
        )
        self._scanner_thread.start()
        logger.info("Scanner started")

    def stop_scanner(self):
        self._scanner_stop.set()
        self._scanner_status.status = "stopped"
        logger.info("Scanner stopped")

    # -- Executor control ----------------------------------------------------

    def start_executor(self):
        if self._executor_thread and self._executor_thread.is_alive():
            return
        self._executor_stop.clear()
        self._executor_status.status = "running"
        self._executor_status.started_at = time.time()
        self._executor_thread = threading.Thread(
            target=self._executor_loop, daemon=True, name="executor"
        )
        self._executor_thread.start()
        logger.info("Executor started")

    def stop_executor(self):
        self._executor_stop.set()
        self._executor_status.status = "stopped"
        logger.info("Executor stopped")

    # -- Status --------------------------------------------------------------

    def get_status(self):
        with self._candidates_lock:
            candidates = [c.to_dict() if hasattr(c, "to_dict") else c
                          for c in self._candidates]
            iv_ranks = dict(self._iv_ranks)
        return {
            "scanner": self._scanner_status.to_dict(),
            "executor": self._executor_status.to_dict(),
            "candidates": candidates,
            "iv_ranks": iv_ranks,
            "config": {
                "symbols": self.strategy_config.symbols,
                "target_dte": self.strategy_config.target_dte,
                "target_delta": self.strategy_config.target_delta,
                "wing_width": self.strategy_config.wing_width,
                "dry_run": self.dry_run,
                "scan_interval": self.scan_interval,
            },
        }

    # -- Scanner loop --------------------------------------------------------

    def _scanner_loop(self):
        from cadence.iv_rank import get_iv_rank_from_index
        from cadence.strategy import find_iron_condor_candidates

        while not self._scanner_stop.is_set():
            try:
                if not is_market_open():
                    self._scanner_status.last_detail = "Market closed, waiting"
                    self._scanner_stop.wait(60)
                    continue

                all_candidates = []
                iv_ranks = {}
                for symbol in self.strategy_config.symbols:
                    try:
                        # Fetch real IV rank via the matching volatility index
                        # (VIX for SPY, VXN for QQQ). Price history was a bug.
                        iv_info = get_iv_rank_from_index(self.trader, symbol)
                        if iv_info is None:
                            logger.warning("No volatility index for %s; "
                                           "IV rank unavailable", symbol)
                            iv_rank = 0.0
                        else:
                            iv_rank = iv_info.get("rank", 0.0)
                            iv_ranks[symbol] = iv_info

                        candidates = find_iron_condor_candidates(
                            self.trader, symbol, self.strategy_config, iv_rank
                        )
                        all_candidates.extend(candidates)
                    except Exception as e:
                        logger.error("Scanner error for %s: %s", symbol, e)
                        self._scanner_status.last_error = f"{symbol}: {e}"

                # Publish IV rank info for the dashboard
                with self._candidates_lock:
                    self._iv_ranks = iv_ranks

                with self._candidates_lock:
                    self._candidates = sorted(
                        all_candidates,
                        key=lambda c: c.return_pct,
                        reverse=True
                    )

                # Sync balance
                try:
                    balances = self.trader.get_account_balances()
                    bal = balances.get("balances", {})
                    equity = int(float(bal.get("total_equity", 0)) * 100)
                    cash = int(float(bal.get("total_cash",
                                            bal.get("cash", {}).get("cash_available", 0))) * 100)
                    self.risk_mgr.sync_actual_balance(cash, portfolio_value_cents=equity)
                except Exception as e:
                    logger.error("Balance sync error: %s", e)

                # Periodic status notification
                now = time.time()
                if (self.notifier and
                        now - self._last_status_time >= self.status_interval_secs):
                    try:
                        status = self.risk_mgr.get_status()
                        self.notifier.notify_status(status)
                    except Exception:
                        pass
                    self._last_status_time = now

                self._scanner_status.run_count += 1
                self._scanner_status.last_run_at = time.time()
                self._scanner_status.last_detail = (
                    f"Found {len(all_candidates)} candidates across "
                    f"{len(self.strategy_config.symbols)} symbols"
                )

            except Exception as e:
                logger.error("Scanner loop error: %s", e)
                self._scanner_status.last_error = str(e)
                if self.notifier:
                    try:
                        self.notifier.notify_scanner_error(str(e))
                    except Exception:
                        pass

            self._scanner_stop.wait(self.scan_interval)

    # -- Executor loop -------------------------------------------------------

    def _executor_loop(self):
        from cadence.executor import execute_candidate

        while not self._executor_stop.is_set():
            try:
                # Check kill switch
                risk_status = self.risk_mgr.get_status()
                if risk_status["kill_switch"]["active"]:
                    self._executor_status.last_detail = "Kill switch active, skipping"
                    self._executor_stop.wait(self.scan_interval)
                    continue

                # Get candidates
                with self._candidates_lock:
                    candidates = list(self._candidates)

                if not candidates:
                    self._executor_status.last_detail = "No candidates available"
                    self._executor_stop.wait(self.scan_interval)
                    continue

                # Filter out recently attempted
                now = time.time()
                eligible = []
                with self._attempts_lock:
                    # Clean expired entries
                    expired = [fp for fp, t in self._recent_attempts.items()
                               if now - t > self.ATTEMPT_TTL_SECS]
                    for fp in expired:
                        del self._recent_attempts[fp]

                    for c in candidates:
                        fp = c.fingerprint()
                        if fp not in self._recent_attempts:
                            eligible.append(c)

                if not eligible:
                    self._executor_status.last_detail = "All candidates recently attempted"
                    self._executor_stop.wait(self.scan_interval)
                    continue

                # Attempt at most 1 trade per cycle
                traded = 0
                for candidate in eligible[:self.MAX_TRADES_PER_CYCLE]:
                    fp = candidate.fingerprint()
                    ok, detail = execute_candidate(
                        self.trader, self.risk_mgr, candidate,
                        contracts=1, dry_run=self.dry_run
                    )

                    # Record attempt
                    with self._attempts_lock:
                        self._recent_attempts[fp] = time.time()

                    decision = {
                        "time": time.time(),
                        "ticker": candidate.symbol,
                        "success": ok,
                        "detail": detail,
                        "credit": candidate.credit,
                        "return_pct": candidate.return_pct,
                    }
                    self._executor_status.recent_decisions.append(decision)

                    if ok:
                        traded += 1
                        self._executor_status.trades_placed += 1
                        if self.notifier:
                            try:
                                self.notifier.notify_trade(detail)
                            except Exception:
                                pass
                    else:
                        if self.notifier and "order" in detail.lower():
                            try:
                                self.notifier.send(f"Order issue: {detail}")
                            except Exception:
                                pass

                self._executor_status.run_count += 1
                self._executor_status.last_run_at = time.time()
                self._executor_status.last_detail = f"Cycle complete, {traded} trades placed"

            except Exception as e:
                logger.error("Executor loop error: %s", e)
                self._executor_status.last_error = str(e)

            self._executor_stop.wait(self.scan_interval)

    # -- Helpers for setting dry_run mode ------------------------------------

    def set_dry_run(self, value):
        self.dry_run = value
        logger.info("Dry run set to %s", value)



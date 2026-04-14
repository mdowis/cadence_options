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
    et_offset_hours,
    MARKET_OPEN,
)

logger = logging.getLogger(__name__)


def _now_et():
    """Current time in US Eastern, DST-aware.

    Uses the -5h first-pass offset to determine the US calendar date,
    then looks up whether that date is in DST and applies the correct
    offset (-4 during EDT, -5 during EST). Correct to the day for all
    market-hours use cases; only ambiguous during the 1-2am local
    transition itself, when markets are closed anyway.
    """
    utc_now = datetime.now(timezone.utc)
    approx_date = (utc_now + timedelta(hours=-5)).date()
    offset = et_offset_hours(approx_date)
    return (utc_now + timedelta(hours=offset)).replace(tzinfo=None)


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
    # Minimum age before auto-exit will trigger on a new position.
    # Protects against immediate exits when sandbox chain prices are
    # stale or wide right after entry, and gives the entry order time
    # to actually fill before we start computing P&L against it.
    MIN_POSITION_AGE_SECS = 600  # 10 minutes

    def __init__(self, trader, risk_mgr, strategy_config, notifier=None,
                 scan_interval=60, position_interval=30, dry_run=True,
                 status_interval_secs=3600, position_manager=None,
                 position_tracker=None, state_file=None,
                 trade_ledger=None):
        self.trader = trader
        self.risk_mgr = risk_mgr
        self.strategy_config = strategy_config
        self.notifier = notifier
        self.scan_interval = scan_interval
        self.position_interval = position_interval
        self.status_interval_secs = status_interval_secs
        self.position_manager = position_manager
        self.position_tracker = position_tracker
        self.trade_ledger = trade_ledger
        self._state_file = state_file
        # Restore persisted dry_run if a state file is configured.
        # Otherwise the bot would silently revert to dry_run=True on
        # every restart, which is dangerous if the operator was in
        # PAPER mode and expects close orders to actually execute.
        self.dry_run = self._load_dry_run(default=dry_run)

        self._scanner_status = ProcessStatus()
        self._executor_status = ProcessStatus()

        self._scanner_thread = None
        self._executor_thread = None
        self._scanner_stop = threading.Event()
        self._executor_stop = threading.Event()

        # Broker-sync runs independently of scanner/executor so the
        # dashboard stays fresh outside market hours and when the
        # scanner is stopped.
        self._sync_thread = None
        self._sync_stop = threading.Event()
        self._sync_interval = 30  # seconds; updated by start_broker_sync

        self._candidates_lock = threading.Lock()
        self._candidates = []
        self._iv_ranks = {}  # symbol -> {rank, current, min, max, source, ...}

        self._attempts_lock = threading.Lock()
        self._recent_attempts = {}  # fingerprint -> timestamp
        # Separate dedup for auto-exit close submissions so we don't
        # spam close orders every sync cycle.
        self._exit_attempts_lock = threading.Lock()
        self._recent_exit_attempts = {}  # tag -> timestamp

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

    # -- Broker sync control -------------------------------------------------

    def start_broker_sync(self, interval=30):
        """Start the periodic broker-sync thread. Pulls equity, positions,
        Greeks from the broker every `interval` seconds regardless of
        market hours or scanner state. Keeps the dashboard fresh so
        operators see real numbers even when the scanner is idle."""
        if self._sync_thread and self._sync_thread.is_alive():
            return
        self._sync_interval = interval
        self._sync_stop.clear()
        self._sync_thread = threading.Thread(
            target=self._broker_sync_loop, daemon=True, name="broker-sync"
        )
        self._sync_thread.start()
        logger.info("Broker sync started (interval=%ds)", interval)

    def stop_broker_sync(self):
        self._sync_stop.set()
        logger.info("Broker sync stopped")

    def _broker_sync_loop(self):
        """Periodically refresh broker state. Runs forever (until stopped)
        regardless of market hours -- balance and positions can change
        from external activity (manual orders, expirations, dividends,
        wires) outside of market hours."""
        while not self._sync_stop.is_set():
            try:
                self._sync_broker_state()
            except Exception as e:
                logger.warning("Broker sync loop error: %s", e)
            self._sync_stop.wait(self._sync_interval)

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
        from cadence.iv_rank import get_iv_rank
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
                        # Fetch IV rank with automatic fallback: try the
                        # matching volatility index (VIX/VXN) first, fall
                        # back to realized volatility of the underlying
                        # when the index isn't available (e.g., Tradier
                        # sandbox doesn't serve VIX history).
                        iv_info = get_iv_rank(self.trader, symbol)
                        iv_rank = iv_info.get("rank", 0.0) if iv_info else 0.0
                        if iv_info:
                            iv_ranks[symbol] = iv_info
                            if iv_info.get("fallback_reason"):
                                logger.info("IV rank for %s: fell back to %s "
                                            "(primary %s: %s)",
                                            symbol, iv_info.get("source"),
                                            iv_info.get("fallback_from"),
                                            iv_info["fallback_reason"])
                            elif iv_info.get("error"):
                                logger.warning("IV rank for %s via %s: %s",
                                               symbol, iv_info.get("source"),
                                               iv_info["error"])

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

                # Sync balance and open positions from the broker
                self._sync_broker_state()

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
                # Don't place orders outside market hours. Tradier would
                # reject them, but failing fast here avoids noise and
                # avoids accidentally queuing orders against stale
                # candidates from an earlier cycle.
                if not is_market_open():
                    self._executor_status.last_detail = "Market closed, skipping"
                    self._executor_stop.wait(60)
                    continue

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
                        contracts=1, dry_run=self.dry_run,
                        tracker=self.position_tracker,
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
                        # Immediate broker sync so the dashboard reflects
                        # the new position + equity without waiting for
                        # the next scan cycle.
                        if not self.dry_run:
                            self._sync_broker_state()
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

    # -- Broker state sync ---------------------------------------------------

    def _sync_broker_state(self):
        """Pull equity, cash, and open-position count from the broker
        and push into the risk manager so dashboard stats stay fresh.

        Called periodically from the scanner loop and immediately after
        a successful trade so operators see numbers update right away.
        """
        try:
            balances = self.trader.get_account_balances()
            bal = balances.get("balances", {})
            equity = int(float(bal.get("total_equity", 0)) * 100)
            cash = int(float(bal.get("total_cash",
                                     bal.get("cash", {})
                                        .get("cash_available", 0))) * 100)
            self.risk_mgr.sync_actual_balance(
                cash, portfolio_value_cents=equity)
        except Exception as e:
            logger.warning("Balance sync error: %s", e)

        try:
            positions = self.trader.get_positions()
        except Exception as e:
            logger.warning("Position count sync error: %s", e)
            positions = None

        # Position count semantics:
        #   ic_count = number of bot-managed iron condors (from tracker).
        #              This is what max_position_count is meant to limit.
        #   leg_count = raw per-leg count from Tradier (informational).
        # Previously we were passing leg_count as position_count, which
        # caused the risk check to trip at leg_count >= 5 (i.e., ~1 IC)
        # and made the dashboard show '8 of 5 max' when the user had
        # 2 ICs (8 legs).
        leg_count = len(positions) if positions is not None else 0
        if self.position_tracker is not None:
            ic_count = len(self.position_tracker.get_open())
        else:
            # No tracker: estimate IC count as legs/4 (assumes all
            # broker positions are part of 4-leg iron condors).
            ic_count = leg_count // 4
        self.risk_mgr.update_position_count(ic_count, leg_count=leg_count)

        # Aggregate portfolio Greeks from open positions
        if positions:
            try:
                from cadence.greeks import aggregate_portfolio_greeks
                greeks = aggregate_portfolio_greeks(self.trader, positions)
                self.risk_mgr.update_greeks(
                    delta_cents=greeks["delta_cents"],
                    gamma_cents=greeks["gamma_cents"],
                    vega_cents=greeks["vega_cents"],
                    theta_cents=greeks["theta_cents"],
                )
            except Exception as e:
                logger.warning("Portfolio Greek sync error: %s", e)
        else:
            # No open positions -- zero the Greeks so stale values don't
            # linger after all positions close.
            self.risk_mgr.update_greeks(0, 0, 0, 0)

        # Drop phantom tracker entries (orders that were placed but
        # never filled). This catches phantoms that share legs with
        # real positions -- detect_closes can't see those because the
        # leg symbols still appear at the broker from the real fill.
        if self.position_tracker is not None:
            self._cleanup_phantoms()

        # Detect closed positions (tracked locally but missing from broker),
        # compute realized P&L, record them for daily P&L / Kelly stats.
        if self.position_tracker is not None and positions is not None:
            self._detect_and_record_closes(positions)

        # For still-open tracked positions, check exit conditions and
        # submit close orders for any that hit profit target / time
        # stop / loss stop.
        if self.position_manager is not None and self.position_tracker is not None:
            self._check_and_submit_exits()

    # Grace period: don't drop a fresh entry as a phantom -- give the
    # broker time to actually fill it. After this many seconds we ask
    # Tradier whether the entry order ever filled.
    PHANTOM_GRACE_SECS = 300

    def _cleanup_phantoms(self):
        """For each tracked entry older than PHANTOM_GRACE_SECS, ask
        Tradier whether the entry order was ever filled. Drop entries
        with no filled order on record.

        Catches the case where N phantoms share legs with a real IC,
        which detect_closes can't handle (it only fires when ALL legs
        are missing from broker positions, but the real IC's legs are
        present)."""
        # Fetch the order list ONCE for the whole cleanup pass so we
        # don't issue N get_orders calls and burn Tradier rate limit.
        # If the API is unreachable, abort the cleanup pass entirely
        # rather than treating every position as a phantom (which
        # would silently delete real positions on rate-limit errors).
        try:
            cached_orders = self.trader.get_orders()
        except Exception as e:
            logger.warning("Phantom cleanup: get_orders failed (%s); "
                           "deferring cleanup so we don't drop real "
                           "positions when the API is unreachable", e)
            return
        now = time.time()
        for tracked in self.position_tracker.get_open():
            age = now - (tracked.entry_time or now)
            if age < self.PHANTOM_GRACE_SECS:
                continue
            try:
                filled = self.position_tracker.position_was_filled(
                    tracked, self.trader, orders=cached_orders)
            except Exception as e:
                logger.warning("Phantom check for %s raised: %s",
                               tracked.tag, e)
                continue
            # Tri-state: only drop on EXPLICIT False. None means we
            # couldn't determine -- leave the entry, retry next sync.
            if filled is False:
                logger.info(
                    "Tracker: dropping phantom tag=%s symbol=%s "
                    "(age=%.0fs, no filled order at broker)",
                    tracked.tag, tracked.symbol, age)
                self.position_tracker.remove(tracked.tag)

    def _detect_and_record_closes(self, broker_positions):
        """For any tracked position whose legs no longer appear at the
        broker, fetch the closing order fills and record realized P&L."""
        try:
            closed = self.position_tracker.detect_closes(broker_positions)
        except Exception as e:
            logger.warning("Close detection failed: %s", e)
            return

        for tracked in closed:
            try:
                pnl_cents, detail = self.position_tracker.compute_realized_pnl_cents(
                    tracked, self.trader)
            except Exception as e:
                logger.warning("P&L computation failed for tag %s: %s",
                               tracked.tag, e)
                pnl_cents, detail = None, None

            if pnl_cents is None:
                # Position is missing from broker positions but we
                # couldn't find a filled close order. Two cases:
                #   1. Entry was filled, then closed by something we
                #      can't identify (manual close in Tradier UI,
                #      assignment, expiration). Record P&L=0 so the
                #      tracker advances, log loudly so operator sees it.
                #   2. Entry was NEVER filled (limit order didn't hit).
                #      The legs never appeared, so 'all legs missing'
                #      doesn't mean 'closed' -- it means 'never opened'.
                #      Silently drop without recording a trade.
                filled = self.position_tracker.position_was_filled(
                    tracked, self.trader)
                if filled is False:
                    logger.info(
                        "Tracker: dropping unfilled tag=%s "
                        "(entry order never filled, no trade recorded)",
                        tracked.tag)
                    self.position_tracker.remove(tracked.tag)
                    continue
                if filled is None:
                    # API unreachable; defer so we don't drop a real
                    # position. detect_closes will retry next sync.
                    logger.warning(
                        "Tracker: cannot determine fill status for "
                        "tag=%s (API unreachable). Deferring; will "
                        "retry next sync.", tracked.tag)
                    continue
                # Filled but we can't resolve close P&L
                detail = (detail if isinstance(detail, str)
                          else f"{tracked.symbol} IC closed (P&L unknown)")
                self.risk_mgr.record_trade(0, f"UNRESOLVED: {detail}")
                self._write_ledger_record(
                    tracked, 0, None, "UNRESOLVED",
                    detail=f"UNRESOLVED: {detail}")
                logger.warning("Recorded close with unknown P&L: %s", detail)
            else:
                self.risk_mgr.record_trade(pnl_cents, detail)
                self._write_ledger_record(tracked, pnl_cents,
                                          close_debit=None,  # extracted below
                                          exit_reason=None,
                                          detail=detail)
                logger.info("Recorded close: %s", detail)
                if self.notifier:
                    try:
                        self.notifier.send(f"Closed: {detail}")
                    except Exception:
                        pass

            # Stop tracking locally regardless of whether we got P&L
            self.position_tracker.remove(tracked.tag)

    def _write_ledger_record(self, tracked, pnl_cents, close_debit,
                              exit_reason, detail=None):
        """Fan out the close event to the trade ledger with full context."""
        if self.trade_ledger is None:
            return
        # Prefer tracker-set reason (auto-exit or manual) over caller-
        # supplied one, since the tracker stores what execute_close
        # annotated when the close order was submitted.
        reason = getattr(tracked, "close_attempted_reason", None) or exit_reason
        if not reason:
            reason = "external"  # close wasn't submitted by the bot
        # Try to grab the close underlying price; best-effort, non-fatal
        close_spot = None
        try:
            q = self.trader.get_quote(tracked.symbol)
            close_spot = float(q.get("last") or q.get("close") or 0) or None
        except Exception:
            close_spot = None
        try:
            self.trade_ledger.record_close(
                tracked, pnl_cents=pnl_cents, close_debit=close_debit,
                close_underlying_price=close_spot,
                exit_reason=reason, detail=detail,
            )
        except Exception as e:
            logger.warning("Trade ledger write failed: %s", e)

    def _check_and_submit_exits(self):
        """For each still-open tracked position, compute the cost to close,
        feed into position_manager.check_for_exits, and submit close
        orders for positions that hit profit target / time stop / loss stop.

        Dedup: once we submit a close for a tag, we don't submit
        another for ATTEMPT_TTL_SECS -- gives the open close order
        time to fill. Once the fill removes the position from Tradier
        the tracker removes it on the next sync, so no further exits
        will ever fire for that tag.
        """
        from cadence.executor import compute_close_debit, execute_close

        tracked_list = self.position_tracker.get_open()
        if not tracked_list:
            return

        now = time.time()
        # Clean expired exit-attempt dedup entries
        with self._exit_attempts_lock:
            expired = [tag for tag, t in self._recent_exit_attempts.items()
                       if now - t > self.ATTEMPT_TTL_SECS]
            for tag in expired:
                del self._recent_exit_attempts[tag]

        # Build position dicts (shape that PositionManager expects) and
        # keep a debit lookup for close-order pricing. Skip positions
        # younger than MIN_POSITION_AGE_SECS so a freshly-opened
        # position can't be exit-checked against stale chain prices.
        position_dicts = []
        debits_by_tag = {}
        for t in tracked_list:
            age = now - (t.entry_time or now)
            if age < self.MIN_POSITION_AGE_SECS:
                continue
            debit, _chain = compute_close_debit(self.trader, t)
            if debit is None:
                continue
            debits_by_tag[t.tag] = debit
            position_dicts.append({
                "id": t.tag,
                "entry_credit": t.entry_credit,
                "current_debit": debit,
                "dte": t.current_dte(),
            })

        if not position_dicts:
            return

        try:
            exits = self.position_manager.check_for_exits(position_dicts)
        except Exception as e:
            logger.warning("Exit check failed: %s", e)
            return

        for action in exits:
            tag = action.position_id
            with self._exit_attempts_lock:
                if tag in self._recent_exit_attempts:
                    continue  # already submitted within TTL
                self._recent_exit_attempts[tag] = now

            tracked = self.position_tracker.get_by_tag(tag)
            if tracked is None:
                continue
            debit = debits_by_tag.get(tag, tracked.entry_credit)
            try:
                ok, detail = execute_close(
                    self.trader, tracked, limit_debit=debit,
                    dry_run=self.dry_run, reason=action.reason.value,
                    tracker=self.position_tracker,
                )
            except Exception as e:
                logger.warning("Auto-exit execute_close for %s raised: %s",
                               tag, e)
                continue
            logger.info("Auto-exit %s -> %s: %s",
                        tag, "OK" if ok else "FAIL", detail)
            if ok and self.notifier:
                try:
                    self.notifier.send(
                        f"Auto-exit ({action.reason.value}): {detail}")
                except Exception:
                    pass

    # -- Helpers for setting dry_run mode ------------------------------------

    def set_dry_run(self, value):
        self.dry_run = bool(value)
        logger.info("Dry run set to %s", self.dry_run)
        self._save_dry_run()

    def _load_dry_run(self, default=True):
        """Read persisted dry_run flag from state file, or default."""
        if not self._state_file:
            return default
        try:
            import json
            import os
            if not os.path.isfile(self._state_file):
                return default
            with open(self._state_file, "r") as f:
                data = json.load(f)
            v = data.get("dry_run", default)
            logger.info("Loaded dry_run=%s from %s", v, self._state_file)
            return bool(v)
        except Exception as e:
            logger.warning("Failed to load dry_run state: %s", e)
            return default

    def _save_dry_run(self):
        """Persist the current dry_run flag so it survives restarts."""
        if not self._state_file:
            return
        try:
            import json
            with open(self._state_file, "w") as f:
                json.dump({"dry_run": self.dry_run}, f)
        except Exception as e:
            logger.warning("Failed to save dry_run state: %s", e)



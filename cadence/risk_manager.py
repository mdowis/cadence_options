"""Risk manager: trade gating, kill switch, drawdown, Greek limits."""

import json
import logging
import os
import threading
import time
from enum import Enum

logger = logging.getLogger(__name__)


class RiskAction(Enum):
    ALLOW = "allow"
    BLOCK_KILL_SWITCH = "block_kill_switch"
    BLOCK_COOLDOWN = "block_cooldown"
    BLOCK_IV_RANK = "block_iv_rank"
    BLOCK_CREDIT = "block_credit"
    BLOCK_POSITION_COUNT = "block_position_count"
    BLOCK_POSITION_SIZE = "block_position_size"
    BLOCK_DAILY_LOSS = "block_daily_loss"
    BLOCK_DRAWDOWN = "block_drawdown"


class RiskDecision:
    """Result of a risk check."""

    def __init__(self, action, reason=""):
        self.action = action
        self.reason = reason

    @property
    def allowed(self):
        return self.action == RiskAction.ALLOW

    def to_dict(self):
        return {"action": self.action.value, "reason": self.reason, "allowed": self.allowed}


class RiskConfig:
    """Risk management configuration."""

    def __init__(self, max_drawdown_pct=10.0, max_drawdown_cents=None,
                 drawdown_reference="session_start",
                 max_daily_loss_cents=None, max_daily_loss_pct=None,
                 max_position_count=5, max_risk_per_position_pct=2.0,
                 min_credit_pct_of_width=20.0, min_iv_rank=30.0,
                 max_portfolio_delta_cents=500000, max_portfolio_vega_cents=50000,
                 max_consecutive_losses=5, cooldown_after_consecutive_secs=300,
                 use_kelly=False, kelly_fraction_of_full=0.25):
        self.max_drawdown_pct = max_drawdown_pct
        self.max_drawdown_cents = max_drawdown_cents
        self.drawdown_reference = drawdown_reference
        self.max_daily_loss_cents = max_daily_loss_cents
        self.max_daily_loss_pct = max_daily_loss_pct
        self.max_position_count = max_position_count
        self.max_risk_per_position_pct = max_risk_per_position_pct
        self.min_credit_pct_of_width = min_credit_pct_of_width
        self.min_iv_rank = min_iv_rank
        self.max_portfolio_delta_cents = max_portfolio_delta_cents
        self.max_portfolio_vega_cents = max_portfolio_vega_cents
        self.max_consecutive_losses = max_consecutive_losses
        self.cooldown_after_consecutive_secs = cooldown_after_consecutive_secs
        # Kelly position sizing: when True, the per-position cap is the
        # min of max_risk_per_position_pct and fractional-Kelly derived
        # from trade history. Defaults off so behavior is unchanged.
        self.use_kelly = use_kelly
        self.kelly_fraction_of_full = kelly_fraction_of_full


class RiskState:
    """Mutable risk tracking state."""

    def __init__(self, starting_equity_cents=0):
        # Equity
        self.starting_equity_cents = starting_equity_cents
        self.peak_equity_cents = starting_equity_cents
        self.current_equity_cents = starting_equity_cents
        self.cash_cents = 0

        # Daily
        self.daily_date = _today_str()
        self.daily_starting_balance_cents = starting_equity_cents
        self.daily_pnl_cents = 0
        self.daily_trade_count = 0
        self.daily_win_count = 0
        self.daily_loss_count = 0

        # Portfolio Greeks (dollar terms, in cents)
        self.portfolio_delta_cents = 0
        self.portfolio_gamma_cents = 0
        self.portfolio_vega_cents = 0
        self.portfolio_theta_cents = 0

        # Position tracking
        # position_count = number of bot-managed iron condors (from
        # position_tracker). Used for the max_position_count risk
        # check. leg_count = raw per-leg count from Tradier, for
        # display only (informational).
        self.position_count = 0
        self.position_leg_count = 0
        self.consecutive_losses = 0

        # Kill switch
        self.kill_switch_active = False
        self.kill_switch_reason = ""
        self.kill_switch_time = None

        # History (capped)
        self.risk_events = []
        self.trade_history = []

        # Balance sync
        self.balance_sync_time = None
        self.balance_sync_count = 0

    def to_dict(self):
        return {
            "starting_equity_cents": self.starting_equity_cents,
            "peak_equity_cents": self.peak_equity_cents,
            "current_equity_cents": self.current_equity_cents,
            "cash_cents": self.cash_cents,
            "daily_date": self.daily_date,
            "daily_starting_balance_cents": self.daily_starting_balance_cents,
            "daily_pnl_cents": self.daily_pnl_cents,
            "daily_trade_count": self.daily_trade_count,
            "daily_win_count": self.daily_win_count,
            "daily_loss_count": self.daily_loss_count,
            "portfolio_delta_cents": self.portfolio_delta_cents,
            "portfolio_gamma_cents": self.portfolio_gamma_cents,
            "portfolio_vega_cents": self.portfolio_vega_cents,
            "portfolio_theta_cents": self.portfolio_theta_cents,
            "position_count": self.position_count,
            "consecutive_losses": self.consecutive_losses,
            "kill_switch_active": self.kill_switch_active,
            "kill_switch_reason": self.kill_switch_reason,
            "kill_switch_time": self.kill_switch_time,
            "risk_events": self.risk_events[-200:],
            "trade_history": self.trade_history[-500:],
            "balance_sync_time": self.balance_sync_time,
            "balance_sync_count": self.balance_sync_count,
        }

    @classmethod
    def from_dict(cls, d):
        state = cls(d.get("starting_equity_cents", 0))
        for key in [
            "peak_equity_cents", "current_equity_cents", "cash_cents",
            "daily_date", "daily_starting_balance_cents", "daily_pnl_cents",
            "daily_trade_count", "daily_win_count", "daily_loss_count",
            "portfolio_delta_cents", "portfolio_gamma_cents",
            "portfolio_vega_cents", "portfolio_theta_cents",
            "position_count", "consecutive_losses",
            "kill_switch_active", "kill_switch_reason", "kill_switch_time",
            "risk_events", "trade_history",
            "balance_sync_time", "balance_sync_count",
        ]:
            if key in d:
                setattr(state, key, d[key])
        return state


def _today_str():
    return time.strftime("%Y-%m-%d")


class RiskManager:
    """Gate trades through risk checks. Thread-safe."""

    MAX_TRADE_HISTORY = 500
    MAX_RISK_EVENTS = 200

    def __init__(self, config, starting_equity_cents=0, state_file=None, notifier=None):
        self.config = config
        self._notifier = notifier
        self._state_file = state_file
        self._lock = threading.Lock()
        self._state = self._load_state(starting_equity_cents)

    def _load_state(self, starting_equity_cents):
        if self._state_file and os.path.exists(self._state_file):
            try:
                with open(self._state_file, "r") as f:
                    data = json.load(f)
                logger.info("Loaded risk state from %s", self._state_file)
                return RiskState.from_dict(data)
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Failed to load risk state: %s", e)
        return RiskState(starting_equity_cents)

    def _save_state(self):
        if not self._state_file:
            return
        try:
            data = self._state.to_dict()
            with open(self._state_file, "w") as f:
                json.dump(data, f)
        except OSError as e:
            logger.warning("Failed to save risk state: %s", e)

    def _cap_history(self):
        if len(self._state.trade_history) > self.MAX_TRADE_HISTORY:
            self._state.trade_history = self._state.trade_history[-self.MAX_TRADE_HISTORY:]
        if len(self._state.risk_events) > self.MAX_RISK_EVENTS:
            self._state.risk_events = self._state.risk_events[-self.MAX_RISK_EVENTS:]

    def _add_risk_event(self, event_type, detail):
        self._state.risk_events.append({
            "time": time.time(),
            "type": event_type,
            "detail": detail,
        })
        self._cap_history()

    def _get_drawdown_reference_cents(self):
        """Get the reference equity for drawdown calculation."""
        if self.config.drawdown_reference == "peak":
            return self._state.peak_equity_cents
        # Default: session_start
        return self._state.daily_starting_balance_cents

    def _check_drawdown(self):
        """Check if drawdown exceeds limits. Returns (tripped, pct, detail)."""
        ref = self._get_drawdown_reference_cents()
        if ref <= 0:
            return False, 0.0, ""
        current = self._state.current_equity_cents
        drawdown_cents = ref - current
        drawdown_pct = (drawdown_cents / ref) * 100 if ref > 0 else 0

        # Check percentage limit
        if drawdown_pct >= self.config.max_drawdown_pct:
            detail = (f"Drawdown {drawdown_pct:.1f}% >= {self.config.max_drawdown_pct}% "
                      f"(ref={ref/100:.2f}, current={current/100:.2f}, "
                      f"mode={self.config.drawdown_reference})")
            return True, drawdown_pct, detail

        # Check absolute limit
        if self.config.max_drawdown_cents is not None:
            if drawdown_cents >= self.config.max_drawdown_cents:
                detail = (f"Drawdown ${drawdown_cents/100:.2f} >= "
                          f"${self.config.max_drawdown_cents/100:.2f}")
                return True, drawdown_pct, detail

        return False, drawdown_pct, ""

    def check_trade(self, candidate, contracts=1):
        """Run ALL risk checks. Returns first failing check or ALLOW."""
        with self._lock:
            return self._check_trade_unlocked(candidate, contracts)

    def _check_trade_unlocked(self, candidate, contracts):
        # 1. Kill switch
        if self._state.kill_switch_active:
            return RiskDecision(RiskAction.BLOCK_KILL_SWITCH,
                                f"Kill switch active: {self._state.kill_switch_reason}")

        # 2. Consecutive loss cooldown
        if self._state.consecutive_losses >= self.config.max_consecutive_losses:
            return RiskDecision(RiskAction.BLOCK_COOLDOWN,
                                f"{self._state.consecutive_losses} consecutive losses, "
                                f"cooldown required")

        # 3. IV rank minimum
        if hasattr(candidate, "iv_rank") and candidate.iv_rank < self.config.min_iv_rank:
            return RiskDecision(RiskAction.BLOCK_IV_RANK,
                                f"IV rank {candidate.iv_rank:.1f} < {self.config.min_iv_rank}")

        # 4. Credit minimum
        if hasattr(candidate, "credit") and hasattr(candidate, "max_loss"):
            wing_width = candidate.credit + candidate.max_loss
            min_credit = self.config.min_credit_pct_of_width / 100.0 * wing_width
            if candidate.credit < min_credit:
                return RiskDecision(RiskAction.BLOCK_CREDIT,
                                    f"Credit {candidate.credit:.2f} < "
                                    f"min {min_credit:.2f}")

        # 5. Position count
        if self._state.position_count >= self.config.max_position_count:
            return RiskDecision(RiskAction.BLOCK_POSITION_COUNT,
                                f"{self._state.position_count} positions >= "
                                f"max {self.config.max_position_count}")

        # 6. Per-position size: cap is the min of the manual limit and
        # the fractional-Kelly derived cap (when Kelly is enabled).
        if (hasattr(candidate, "max_loss") and
                self._state.current_equity_cents > 0):
            max_loss_cents = int(candidate.max_loss * 100 * contracts)  # per-contract * 100
            risk_pct = (max_loss_cents / self._state.current_equity_cents) * 100
            effective_cap = self.config.max_risk_per_position_pct
            cap_source = "manual"
            if self.config.use_kelly:
                from cadence.kelly import recommended_position_risk_pct
                rec = recommended_position_risk_pct(
                    self._state.trade_history,
                    fraction_of_full=self.config.kelly_fraction_of_full,
                    absolute_cap_pct=self.config.max_risk_per_position_pct,
                )
                effective_cap = rec["recommended_pct"]
                if rec["fractional_kelly_pct"] < self.config.max_risk_per_position_pct:
                    cap_source = "kelly"
            if risk_pct > effective_cap:
                return RiskDecision(RiskAction.BLOCK_POSITION_SIZE,
                                    f"Position risk {risk_pct:.1f}% > "
                                    f"{cap_source} cap {effective_cap:.2f}%")

        # 7. Daily loss limit
        daily_loss_limit_cents = self._get_daily_loss_limit_cents()
        if daily_loss_limit_cents is not None:
            if abs(self._state.daily_pnl_cents) >= daily_loss_limit_cents and self._state.daily_pnl_cents < 0:
                return RiskDecision(RiskAction.BLOCK_DAILY_LOSS,
                                    f"Daily loss ${abs(self._state.daily_pnl_cents)/100:.2f} "
                                    f">= limit ${daily_loss_limit_cents/100:.2f}")

        # 8. Drawdown
        tripped, pct, detail = self._check_drawdown()
        if tripped:
            self._activate_kill_switch_internal(detail)
            return RiskDecision(RiskAction.BLOCK_DRAWDOWN, detail)

        return RiskDecision(RiskAction.ALLOW, "All checks passed")

    def _get_daily_loss_limit_cents(self):
        limits = []
        if self.config.max_daily_loss_cents is not None:
            limits.append(self.config.max_daily_loss_cents)
        if self.config.max_daily_loss_pct is not None and self._state.daily_starting_balance_cents > 0:
            pct_limit = int(self.config.max_daily_loss_pct / 100 * self._state.daily_starting_balance_cents)
            limits.append(pct_limit)
        return min(limits) if limits else None

    def sync_actual_balance(self, balance_cents, portfolio_value_cents=None):
        """Sync equity from broker. portfolio_value_cents is authoritative
        when available (Tradier's total_equity). Never compute equity as
        balance + internally tracked exposure."""
        with self._lock:
            if portfolio_value_cents is not None:
                self._state.current_equity_cents = portfolio_value_cents
            else:
                self._state.current_equity_cents = balance_cents

            self._state.cash_cents = balance_cents

            # Update peak
            if self._state.current_equity_cents > self._state.peak_equity_cents:
                self._state.peak_equity_cents = self._state.current_equity_cents

            # Check for day rollover
            today = _today_str()
            if self._state.daily_date != today:
                self._reset_daily_unlocked()

            self._state.balance_sync_time = time.time()
            self._state.balance_sync_count += 1

            # Check drawdown after sync
            tripped, pct, detail = self._check_drawdown()
            if tripped and not self._state.kill_switch_active:
                self._activate_kill_switch_internal(detail)

            self._save_state()

    def activate_kill_switch(self, reason):
        """Manually activate the kill switch."""
        with self._lock:
            self._activate_kill_switch_internal(reason)
            self._save_state()

    def _activate_kill_switch_internal(self, reason):
        """Internal kill switch activation. Notifies only on transition."""
        was_active = self._state.kill_switch_active
        self._state.kill_switch_active = True
        self._state.kill_switch_reason = reason
        self._state.kill_switch_time = time.time()
        self._add_risk_event("kill_switch_activated", reason)
        if not was_active and self._notifier:
            try:
                self._notifier.notify_kill_switch(reason)
            except Exception as e:
                logger.warning("Failed to notify kill switch: %s", e)

    def deactivate_kill_switch(self):
        """Manually deactivate the kill switch."""
        with self._lock:
            self._state.kill_switch_active = False
            self._state.kill_switch_reason = ""
            self._state.kill_switch_time = None
            self._add_risk_event("kill_switch_deactivated", "")
            self._save_state()

    def reset_daily(self):
        """Reset daily metrics and rebaseline peak to current."""
        with self._lock:
            self._reset_daily_unlocked()
            self._save_state()

    def _reset_daily_unlocked(self):
        current = self._state.current_equity_cents
        self._state.daily_date = _today_str()
        self._state.daily_starting_balance_cents = current
        self._state.daily_pnl_cents = 0
        self._state.daily_trade_count = 0
        self._state.daily_win_count = 0
        self._state.daily_loss_count = 0
        # Critical: rebaseline peak to current to prevent stale peak trips
        self._state.peak_equity_cents = current
        self._add_risk_event("daily_reset", f"Rebaselined to {current}")

    def record_trade(self, pnl_cents, detail=""):
        """Record a completed trade."""
        with self._lock:
            self._state.daily_pnl_cents += pnl_cents
            self._state.daily_trade_count += 1
            if pnl_cents >= 0:
                self._state.daily_win_count += 1
                self._state.consecutive_losses = 0
            else:
                self._state.daily_loss_count += 1
                self._state.consecutive_losses += 1
            self._state.trade_history.append({
                "time": time.time(),
                "pnl_cents": pnl_cents,
                "detail": detail,
            })
            self._cap_history()
            self._save_state()

    def update_position_count(self, count, leg_count=None):
        """Update counts.

        count: number of bot-managed iron condors (from tracker). Used
               for the max_position_count risk check.
        leg_count: raw per-leg count from Tradier (informational only).
        """
        with self._lock:
            self._state.position_count = count
            if leg_count is not None:
                self._state.position_leg_count = leg_count
            self._save_state()

    def update_greeks(self, delta_cents=0, gamma_cents=0, vega_cents=0, theta_cents=0):
        """Update portfolio-level Greeks in dollar terms (cents)."""
        with self._lock:
            self._state.portfolio_delta_cents = delta_cents
            self._state.portfolio_gamma_cents = gamma_cents
            self._state.portfolio_vega_cents = vega_cents
            self._state.portfolio_theta_cents = theta_cents
            self._save_state()

    def get_status(self):
        """Full snapshot for the dashboard."""
        with self._lock:
            ref = self._get_drawdown_reference_cents()
            current = self._state.current_equity_cents
            drawdown_cents = ref - current if ref > 0 else 0
            drawdown_pct = (drawdown_cents / ref * 100) if ref > 0 else 0

            return {
                "equity": {
                    "starting": self._state.starting_equity_cents,
                    "peak": self._state.peak_equity_cents,
                    "current": self._state.current_equity_cents,
                    "cash": self._state.cash_cents,
                },
                "drawdown": {
                    "reference_mode": self.config.drawdown_reference,
                    "reference_cents": ref,
                    "current_pct": drawdown_pct,
                    "current_cents": drawdown_cents,
                    "max_pct": self.config.max_drawdown_pct,
                },
                "daily": {
                    "date": self._state.daily_date,
                    "starting_balance_cents": self._state.daily_starting_balance_cents,
                    "pnl_cents": self._state.daily_pnl_cents,
                    "trade_count": self._state.daily_trade_count,
                    "win_count": self._state.daily_win_count,
                    "loss_count": self._state.daily_loss_count,
                },
                "greeks": {
                    "delta_cents": self._state.portfolio_delta_cents,
                    "gamma_cents": self._state.portfolio_gamma_cents,
                    "vega_cents": self._state.portfolio_vega_cents,
                    "theta_cents": self._state.portfolio_theta_cents,
                },
                "positions": {
                    "count": self._state.position_count,      # IC count
                    "leg_count": self._state.position_leg_count,  # broker legs
                    "max": self.config.max_position_count,
                },
                "kill_switch": {
                    "active": self._state.kill_switch_active,
                    "reason": self._state.kill_switch_reason,
                    "time": self._state.kill_switch_time,
                },
                "consecutive_losses": self._state.consecutive_losses,
                "balance_sync": {
                    "time": self._state.balance_sync_time,
                    "count": self._state.balance_sync_count,
                },
                "drawdown_reference_mode": self.config.drawdown_reference,
                "risk_events": list(self._state.risk_events[-10:]),
                "trade_history": list(self._state.trade_history[-50:]),
                "kelly": self._kelly_snapshot(),
            }

    def _kelly_snapshot(self):
        """Build the Kelly diagnostic block for the dashboard.

        Always computed so operators can see the recommendation even
        when Kelly capping is disabled. The effective_cap_pct is the
        cap actually used in risk checks: if use_kelly is False, it
        equals max_risk_per_position_pct; otherwise min(manual, Kelly).
        """
        from cadence.kelly import recommended_position_risk_pct
        rec = recommended_position_risk_pct(
            self._state.trade_history,
            fraction_of_full=self.config.kelly_fraction_of_full,
            absolute_cap_pct=self.config.max_risk_per_position_pct,
        )
        enabled = self.config.use_kelly
        effective = (rec["recommended_pct"] if enabled
                     else self.config.max_risk_per_position_pct)
        return {
            "enabled": enabled,
            "fraction_of_full": self.config.kelly_fraction_of_full,
            "full_kelly_fraction": rec["kelly_fraction"],
            "fractional_kelly_pct": rec["fractional_kelly_pct"],
            "manual_cap_pct": self.config.max_risk_per_position_pct,
            "effective_cap_pct": effective,
            "sample_size": rec["sample_size"],
            "using_defaults": rec["using_defaults"],
            "win_rate": rec["win_rate"],
        }

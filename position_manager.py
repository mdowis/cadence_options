"""Open-position tracking and exit detection."""

import logging
from datetime import date, datetime
from enum import Enum

logger = logging.getLogger(__name__)


class ExitReason(Enum):
    PROFIT_TARGET = "profit_target"
    TIME_STOP = "time_stop"
    LOSS_STOP = "loss_stop"
    MANUAL = "manual"


class ExitAction:
    """Describes a position that should be closed."""

    def __init__(self, position_id, reason, detail=""):
        self.position_id = position_id
        self.reason = reason
        self.detail = detail

    def to_dict(self):
        return {
            "position_id": self.position_id,
            "reason": self.reason.value,
            "detail": self.detail,
        }


class PositionManager:
    """Evaluates open positions for exit conditions."""

    def __init__(self, profit_target_pct=50, time_stop_dte=21,
                 loss_stop_multiplier=2.0):
        self.profit_target_pct = profit_target_pct
        self.time_stop_dte = time_stop_dte
        self.loss_stop_multiplier = loss_stop_multiplier

    def check_for_exits(self, positions, today=None):
        """Check all positions for exit conditions.

        Each position dict should have:
        - id: unique identifier
        - entry_credit: credit received per share when opened
        - current_debit: cost to close per share right now
        - expiration: "YYYY-MM-DD" string
        - dte: days to expiration (optional, computed if missing)

        Returns list of ExitAction for positions that should be closed.
        """
        if today is None:
            today = date.today()

        exits = []
        for pos in positions:
            action = self._check_position(pos, today)
            if action:
                exits.append(action)
        return exits

    def _check_position(self, pos, today):
        pos_id = pos.get("id", "unknown")
        entry_credit = pos.get("entry_credit", 0)
        current_debit = pos.get("current_debit", 0)

        if entry_credit <= 0:
            return None

        # P&L = credit received - cost to close
        pnl = entry_credit - current_debit
        pnl_pct = (pnl / entry_credit) * 100

        # 1. Profit target: P&L >= 50% of credit collected
        if pnl_pct >= self.profit_target_pct:
            return ExitAction(pos_id, ExitReason.PROFIT_TARGET,
                              f"P&L {pnl_pct:.1f}% >= {self.profit_target_pct}% target")

        # 2. Time stop: DTE <= threshold
        dte = pos.get("dte")
        if dte is None:
            exp_str = pos.get("expiration", "")
            if exp_str:
                try:
                    exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                    dte = (exp_date - today).days
                except ValueError:
                    dte = None

        if dte is not None and dte <= self.time_stop_dte:
            return ExitAction(pos_id, ExitReason.TIME_STOP,
                              f"DTE {dte} <= {self.time_stop_dte}")

        # 3. Loss stop: loss >= 2x the credit collected
        if pnl < 0 and abs(pnl) >= self.loss_stop_multiplier * entry_credit:
            return ExitAction(pos_id, ExitReason.LOSS_STOP,
                              f"Loss {abs(pnl):.2f} >= {self.loss_stop_multiplier}x "
                              f"credit {entry_credit:.2f}")

        return None

"""Append-only ledger of closed trades.

One JSON record per line (JSONL format) so:
  - appends are atomic (the OS flushes line-by-line when we call
    fh.write + fh.flush)
  - history never shrinks from a rewrite failure
  - records can be grep'd, cut, or fed into a pandas DataFrame for
    strategy analysis

Each record captures the FULL entry + exit context needed to answer:
  - Win rate by DTE at entry
  - Win rate by IV rank band at entry
  - Avg win vs avg loss
  - Distribution of exit reasons
  - Duration distribution
  - Correlation of underlying move vs outcome

Use cases:
  - Tune the min_iv_rank filter based on which trades actually won
  - Decide whether profit_target=50% or 25% has better net outcomes
  - See if loss_stop=2x is triggering too often / too rarely
  - Spot regime changes (recent trades suddenly worse than historical)
"""

import json
import logging
import os
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)


class TradeLedger:
    """Append-only JSONL ledger of closed trade records."""

    def __init__(self, path=None):
        self.path = path
        self._lock = threading.Lock()

    def record_close(self, tracked, pnl_cents, close_debit=None,
                     close_underlying_price=None, exit_reason=None,
                     exit_time=None, detail=None):
        """Append a closed-trade record.

        tracked: the TrackedPosition that just closed
        pnl_cents: realized P&L in cents (signed)
        close_debit: per-share debit we paid to close, if known
        close_underlying_price: underlying spot at close time
        exit_reason: 'profit_target'|'time_stop'|'loss_stop'|'manual'|
                     'external'|'expiration'|'UNRESOLVED'|None
        exit_time: unix timestamp, defaults to now
        detail: free-form description (e.g., from risk_mgr.record_trade)
        """
        if exit_time is None:
            exit_time = time.time()

        entry_time = tracked.entry_time or exit_time
        entry_credit = tracked.entry_credit
        entry_credit_mid = getattr(tracked, "entry_credit_mid", entry_credit)
        entry_underlying = getattr(tracked, "entry_underlying_price", None)
        iv_rank_at_entry = getattr(tracked, "iv_rank_at_entry", None)

        # Wing widths (can be asymmetric)
        put_wing = tracked.short_put_strike - tracked.long_put_strike
        call_wing = tracked.long_call_strike - tracked.short_call_strike
        effective_wing = max(put_wing, call_wing)
        max_loss_per_share = effective_wing - entry_credit

        duration_secs = exit_time - entry_time
        duration_days = duration_secs / 86400.0

        # Percent return on the max loss (capital at risk)
        pnl_dollars = pnl_cents / 100.0
        risk_dollars = max_loss_per_share * 100 * tracked.contracts
        return_on_risk_pct = (pnl_dollars / risk_dollars * 100) if risk_dollars > 0 else None

        underlying_move = None
        underlying_move_pct = None
        if entry_underlying and close_underlying_price:
            underlying_move = close_underlying_price - entry_underlying
            if entry_underlying > 0:
                underlying_move_pct = underlying_move / entry_underlying * 100

        record = {
            # Identification
            "tag": tracked.tag,
            "symbol": tracked.symbol,
            "expiration": tracked.expiration,

            # Structure
            "contracts": tracked.contracts,
            "short_put_strike": tracked.short_put_strike,
            "long_put_strike": tracked.long_put_strike,
            "short_call_strike": tracked.short_call_strike,
            "long_call_strike": tracked.long_call_strike,
            "put_wing": put_wing,
            "call_wing": call_wing,

            # Entry
            "entry_time": entry_time,
            "entry_time_iso": _iso(entry_time),
            "entry_credit": entry_credit,
            "entry_credit_mid": entry_credit_mid,
            "entry_underlying_price": entry_underlying,
            "iv_rank_at_entry": iv_rank_at_entry,
            "dte_at_entry": getattr(tracked, "dte_at_entry", None),

            # Exit
            "exit_time": exit_time,
            "exit_time_iso": _iso(exit_time),
            "exit_debit": close_debit,
            "exit_underlying_price": close_underlying_price,
            "exit_reason": exit_reason or "unknown",
            "detail": detail,

            # Outcomes
            "pnl_cents": pnl_cents,
            "pnl_dollars": pnl_dollars,
            "max_loss_dollars": risk_dollars,
            "return_on_risk_pct": return_on_risk_pct,
            "duration_days": round(duration_days, 3),
            "underlying_move": underlying_move,
            "underlying_move_pct": underlying_move_pct,
            "win": pnl_cents > 0,
        }

        if self.path:
            try:
                with self._lock:
                    # Append + flush for crash-resilience
                    with open(self.path, "a") as f:
                        f.write(json.dumps(record) + "\n")
            except OSError as e:
                logger.warning("Trade ledger write failed: %s", e)
        else:
            logger.info("Trade ledger (no path): %s", record)
        return record

    def read_all(self, limit=None):
        """Read records from the ledger. Newest last. Returns list of dicts."""
        if not self.path or not os.path.isfile(self.path):
            return []
        records = []
        try:
            with self._lock:
                with open(self.path, "r") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            logger.warning("Skipping malformed ledger line: %s",
                                           line[:80])
        except OSError as e:
            logger.warning("Trade ledger read failed: %s", e)
            return []
        if limit is not None and len(records) > limit:
            return records[-limit:]
        return records

    def summary_stats(self):
        """Aggregate stats for strategy analysis."""
        records = self.read_all()
        if not records:
            return {
                "n": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
                "avg_win": 0.0, "avg_loss": 0.0,
                "total_pnl": 0.0, "avg_duration_days": 0.0,
                "by_exit_reason": {},
            }
        wins = [r for r in records if r.get("win")]
        losses = [r for r in records if not r.get("win")]
        total_pnl = sum(r.get("pnl_dollars", 0) or 0 for r in records)
        avg_win = (sum(r.get("pnl_dollars", 0) or 0 for r in wins)
                   / len(wins)) if wins else 0
        avg_loss = (sum(r.get("pnl_dollars", 0) or 0 for r in losses)
                    / len(losses)) if losses else 0
        avg_duration = (sum(r.get("duration_days", 0) or 0 for r in records)
                        / len(records)) if records else 0

        by_reason = {}
        for r in records:
            reason = r.get("exit_reason", "unknown")
            entry = by_reason.setdefault(
                reason, {"count": 0, "wins": 0, "total_pnl": 0.0})
            entry["count"] += 1
            if r.get("win"):
                entry["wins"] += 1
            entry["total_pnl"] += r.get("pnl_dollars", 0) or 0

        return {
            "n": len(records),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": len(wins) / len(records) * 100 if records else 0,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "total_pnl": total_pnl,
            "avg_duration_days": avg_duration,
            "by_exit_reason": by_reason,
        }


def _iso(ts):
    """Format unix timestamp as ISO 8601 (local time)."""
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(ts).isoformat(timespec="seconds")
    except (ValueError, OSError):
        return None

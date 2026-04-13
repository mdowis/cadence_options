"""Tracks open iron condor positions locally and detects closes.

Tradier's /accounts/{id}/positions endpoint returns individual option
legs, not the logical iron condor grouping we opened. To track P&L
properly we need to remember:

  - which tag / position_id we opened
  - the four OCC leg symbols
  - the entry credit (what we received)
  - contracts and timestamps

When any of those legs disappears from the broker's position list,
the position has been closed (either by a close order we submitted,
a partial assignment, or expiration). We then look up the matching
closing orders by `tag`, compute realized P&L, and feed it into the
risk manager.

State is persisted to a JSON file so the bot survives restarts
without losing track of open positions.
"""

import json
import logging
import os
import threading
import time
from datetime import date, datetime

logger = logging.getLogger(__name__)


class TrackedPosition:
    """A locally-tracked iron condor position."""

    __slots__ = (
        "tag", "symbol", "expiration", "dte_at_entry", "contracts",
        "entry_credit", "entry_time",
        "short_put_symbol", "long_put_symbol",
        "short_call_symbol", "long_call_symbol",
        "short_put_strike", "long_put_strike",
        "short_call_strike", "long_call_strike",
    )

    def __init__(self, tag, symbol, expiration, dte_at_entry, contracts,
                 entry_credit, entry_time,
                 short_put_symbol, long_put_symbol,
                 short_call_symbol, long_call_symbol,
                 short_put_strike, long_put_strike,
                 short_call_strike, long_call_strike):
        self.tag = tag
        self.symbol = symbol
        self.expiration = expiration
        self.dte_at_entry = dte_at_entry
        self.contracts = contracts
        self.entry_credit = entry_credit          # per-share (e.g., 2.40)
        self.entry_time = entry_time              # unix ts
        self.short_put_symbol = short_put_symbol
        self.long_put_symbol = long_put_symbol
        self.short_call_symbol = short_call_symbol
        self.long_call_symbol = long_call_symbol
        self.short_put_strike = short_put_strike
        self.long_put_strike = long_put_strike
        self.short_call_strike = short_call_strike
        self.long_call_strike = long_call_strike

    def leg_symbols(self):
        return (self.short_put_symbol, self.long_put_symbol,
                self.short_call_symbol, self.long_call_symbol)

    def current_dte(self, today=None):
        """Days remaining until expiration."""
        today = today or date.today()
        try:
            exp = datetime.strptime(self.expiration, "%Y-%m-%d").date()
            return (exp - today).days
        except (ValueError, TypeError):
            return None

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in self.__slots__}

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: d.get(k) for k in cls.__slots__})


class PositionTracker:
    """In-memory store with optional JSON persistence."""

    def __init__(self, state_file=None):
        self._state_file = state_file
        self._lock = threading.Lock()
        self._positions = {}   # tag -> TrackedPosition
        self._load()

    # -- Persistence -------------------------------------------------------

    def _load(self):
        if not self._state_file or not os.path.isfile(self._state_file):
            return
        try:
            with open(self._state_file, "r") as f:
                data = json.load(f)
            for d in data.get("positions", []):
                try:
                    pos = TrackedPosition.from_dict(d)
                    self._positions[pos.tag] = pos
                except (TypeError, KeyError) as e:
                    logger.warning("Skipping malformed tracked position: %s", e)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Position tracker load failed: %s", e)

    def _save_unlocked(self):
        if not self._state_file:
            return
        try:
            data = {"positions": [p.to_dict() for p in self._positions.values()]}
            with open(self._state_file, "w") as f:
                json.dump(data, f)
        except OSError as e:
            logger.warning("Position tracker save failed: %s", e)

    # -- Public API --------------------------------------------------------

    def record_entry(self, candidate, tag, contracts=1, entry_time=None):
        """Record a new iron condor position we just opened."""
        if entry_time is None:
            entry_time = time.time()
        pos = TrackedPosition(
            tag=tag,
            symbol=candidate.symbol,
            expiration=candidate.expiration,
            dte_at_entry=candidate.dte,
            contracts=contracts,
            entry_credit=candidate.credit,
            entry_time=entry_time,
            short_put_symbol=candidate.short_put_symbol,
            long_put_symbol=candidate.long_put_symbol,
            short_call_symbol=candidate.short_call_symbol,
            long_call_symbol=candidate.long_call_symbol,
            short_put_strike=candidate.short_put_strike,
            long_put_strike=candidate.long_put_strike,
            short_call_strike=candidate.short_call_strike,
            long_call_strike=candidate.long_call_strike,
        )
        with self._lock:
            self._positions[tag] = pos
            self._save_unlocked()
        logger.info("Tracker: recorded entry tag=%s %s @ %.2f credit",
                    tag, candidate.symbol, candidate.credit)

    def get_open(self):
        """Return list of currently-tracked open positions."""
        with self._lock:
            return list(self._positions.values())

    def get_by_tag(self, tag):
        with self._lock:
            return self._positions.get(tag)

    def detect_closes(self, broker_positions):
        """Return list of tracked positions that no longer exist at the broker.

        A tracked position is considered closed when none of its four
        leg symbols appear in the broker's positions list. Detection
        is symmetric: all four legs missing = closed.
        """
        broker_symbols = set()
        for p in broker_positions:
            sym = p.get("symbol")
            if sym:
                broker_symbols.add(sym)

        closed = []
        with self._lock:
            for tag, pos in list(self._positions.items()):
                legs = pos.leg_symbols()
                # All four legs missing -> position is closed
                if not any(leg in broker_symbols for leg in legs):
                    closed.append(pos)
        return closed

    def remove(self, tag):
        """Remove a tracked position (after close has been recorded)."""
        with self._lock:
            self._positions.pop(tag, None)
            self._save_unlocked()

    # -- P&L computation ---------------------------------------------------

    def compute_realized_pnl_cents(self, tracked, trader):
        """Look up the closing orders for a tracked position by tag and
        compute realized P&L.

        Returns (pnl_cents, detail_str) or (None, reason_str) if we
        couldn't find the closing fills.
        """
        try:
            orders = trader.get_orders()
        except Exception as e:
            return None, f"get_orders failed: {e}"

        # Find orders matching this tag that are close-side
        matching = []
        for o in orders:
            if o.get("tag") != tracked.tag:
                continue
            status = (o.get("status") or "").lower()
            if status != "filled":
                continue
            matching.append(o)

        if not matching:
            return None, f"no filled close orders found for tag {tracked.tag}"

        # Sum the close debit/credit across any closing orders.
        # For an iron condor we opened for credit X, closing costs
        # some debit Y. Realized P&L = (X - Y) * contracts * 100.
        # The close order's fill price is the net price we paid.
        close_debit = 0.0
        for o in matching:
            side_class = (o.get("class") or "").lower()
            # We only care about the close orders we placed ourselves
            # (not the entry). Entry is typically a credit order;
            # close is a debit order. Skip if this order's status/side
            # doesn't look like a close.
            order_type = (o.get("type") or "").lower()
            if order_type not in ("debit", "credit", "market"):
                continue
            avg_fill = o.get("avg_fill_price") or o.get("price") or 0
            try:
                close_debit += float(avg_fill)
            except (TypeError, ValueError):
                continue

        # Heuristic: we only want to subtract the CLOSING leg, not the
        # entry credit that's also tagged. Find the later order(s).
        # The entry has class=multileg and type=credit. Close is also
        # typically class=multileg type=debit. Fall back to: count all
        # tagged orders after the entry_time.
        close_orders = [
            o for o in matching
            if _order_created_after(o, tracked.entry_time + 1)
        ]
        if close_orders:
            close_debit = 0.0
            for o in close_orders:
                avg_fill = o.get("avg_fill_price") or o.get("price") or 0
                try:
                    close_debit += float(avg_fill)
                except (TypeError, ValueError):
                    continue

        pnl_per_share = tracked.entry_credit - close_debit
        pnl_cents = int(round(pnl_per_share * 100 * tracked.contracts * 100))
        detail = (f"{tracked.symbol} IC closed: entry_credit=${tracked.entry_credit:.2f}, "
                  f"close_debit=${close_debit:.2f}, P&L=${pnl_cents/100:.2f}, "
                  f"tag={tracked.tag}")
        return pnl_cents, detail


def _order_created_after(order, ts):
    """True if an order's create_date is later than the given unix ts."""
    d = order.get("create_date") or order.get("transaction_date")
    if not d:
        return False
    try:
        # Tradier returns ISO 8601 timestamps like '2026-04-13T14:30:00.000Z'
        dt = datetime.strptime(d[:19], "%Y-%m-%dT%H:%M:%S")
        return dt.timestamp() > ts
    except (ValueError, TypeError):
        return False

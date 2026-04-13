"""IV rank and IV percentile computation.

For SPY and QQQ we use real implied-volatility indices (VIX and VXN)
rather than price history as a proxy. These indices ARE the 30-day
implied volatility of the underlying's options -- the exact quantity
IV rank is meant to describe.

For other symbols where no matching volatility index exists, fall back
to a local IV-history file that snapshots ATM IV from the option chain
daily and builds up history over time.
"""

import json
import logging
import math
import os
import threading
import time

logger = logging.getLogger(__name__)

# Maps underlying symbol to the volatility index that tracks its implied vol.
# These are cash indices available via Tradier's /markets/history endpoint.
VOLATILITY_INDEX_SYMBOLS = {
    "SPY": "VIX",   # S&P 500 Volatility Index
    "QQQ": "VXN",   # Nasdaq-100 Volatility Index
    "IWM": "RVX",   # Russell 2000 Volatility Index
    "DIA": "VXD",   # Dow Jones Volatility Index
}

# How many days of history to request (52 weeks + a bit of slack)
_HISTORY_DAYS = 380

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600  # 1 hour


# ============================================================================
# Pure computation
# ============================================================================

def compute_iv_rank(current_iv, iv_history_list):
    """Compute IV rank (0-100).

    Formula: (current - min) / (max - min) * 100
    Returns 0 if history is empty or max == min.
    """
    if not iv_history_list:
        return 0.0
    iv_min = min(iv_history_list)
    iv_max = max(iv_history_list)
    if iv_max == iv_min:
        return 0.0
    return (current_iv - iv_min) / (iv_max - iv_min) * 100.0


def compute_iv_percentile(current_iv, iv_history_list):
    """What percentage of history was below current IV (0-100).

    Returns 0 if history is empty.
    """
    if not iv_history_list:
        return 0.0
    below = sum(1 for iv in iv_history_list if iv < current_iv)
    return below / len(iv_history_list) * 100.0


# ============================================================================
# Primary: fetch IV rank via the matching volatility index
# ============================================================================

def _history_start_str():
    """Return a YYYY-MM-DD string for _HISTORY_DAYS ago."""
    t = time.time() - (_HISTORY_DAYS * 24 * 3600)
    return time.strftime("%Y-%m-%d", time.localtime(t))


def _extract_price(quote):
    """Pull a usable current price from a Tradier quote dict.

    Tradier sometimes omits `last` on cash indices or during closed
    sessions. Fall through a list of likely alternative fields.
    """
    if not quote:
        return None
    for key in ("last", "last_price", "close", "prevclose", "mark"):
        val = quote.get(key)
        if val is None:
            continue
        try:
            f = float(val)
            if f > 0:
                return f
        except (TypeError, ValueError):
            continue
    # Bid/ask midpoint as last resort
    bid = quote.get("bid")
    ask = quote.get("ask")
    if bid is not None and ask is not None:
        try:
            b, a = float(bid), float(ask)
            if b > 0 and a > 0:
                return (b + a) / 2.0
        except (TypeError, ValueError):
            pass
    return None


def get_iv_rank_from_index(trader, underlying, today_fn=None):
    """Fetch IV rank for an underlying using its volatility index.

    Returns a dict:
      {"rank": float, "current": float, "min": float, "max": float,
       "source": "VIX"|"VXN"|..., "history_points": int}
    or None if no matching volatility index is available.

    Raises nothing -- on API error, returns a dict with rank=0 and
    an `error` key describing what went wrong. If the live quote
    endpoint doesn't produce a usable price, falls back to the most
    recent close from the history series so we still get a value.
    """
    index_symbol = VOLATILITY_INDEX_SYMBOLS.get(underlying)
    if not index_symbol:
        return None

    try:
        # Fetch history first -- we need it regardless, and we can use
        # the latest close as a fallback for "current" if the live
        # quote is missing or malformed.
        start = _history_start_str()
        end = time.strftime("%Y-%m-%d") if today_fn is None else today_fn()
        history = trader.get_history(
            index_symbol, interval="daily", start=start, end=end
        )
        iv_values = []
        for d in history:
            c = d.get("close")
            if c is None:
                continue
            try:
                iv_values.append(float(c))
            except (TypeError, ValueError):
                continue

        if not iv_values:
            return {"rank": 0.0, "current": 0, "min": 0, "max": 0,
                    "source": index_symbol, "history_points": 0,
                    "error": f"no history available for {index_symbol}"}

        # Try the live quote; fall back to latest history close.
        current_iv = None
        current_source = index_symbol
        try:
            quote = trader.get_quote(index_symbol)
            current_iv = _extract_price(quote)
        except Exception as e:
            logger.debug("Live quote for %s failed: %s", index_symbol, e)

        if current_iv is None:
            current_iv = iv_values[-1]
            current_source = f"{index_symbol} (latest close)"

        rank = compute_iv_rank(current_iv, iv_values)
        return {
            "rank": rank,
            "current": current_iv,
            "min": min(iv_values),
            "max": max(iv_values),
            "source": current_source,
            "history_points": len(iv_values),
        }
    except Exception as e:
        logger.warning("IV rank fetch for %s via %s failed: %s",
                       underlying, index_symbol, e)
        return {"rank": 0.0, "current": 0, "min": 0, "max": 0,
                "source": index_symbol, "history_points": 0,
                "error": str(e)}


# ============================================================================
# Fallback: realized volatility from underlying price history
# ============================================================================
#
# When the volatility index isn't available (e.g., Tradier sandbox
# doesn't serve VIX/VXN history, or a symbol doesn't have a matching
# vol index at all), we can compute rolling realized volatility from
# the underlying's own daily closes and rank the current RV against
# its 52w history. RV-rank is not the same thing as IV-rank, but the
# two track each other closely enough across vol regimes to be a
# useful stand-in while true IV data is unavailable.

REALIZED_VOL_WINDOW = 20  # trading days for rolling RV
TRADING_DAYS_PER_YEAR = 252


def compute_realized_vol_series(prices, window=REALIZED_VOL_WINDOW,
                                annualize_days=TRADING_DAYS_PER_YEAR):
    """Rolling N-day annualized realized volatility from a list of daily closes.

    Returns the RV series in percentage points (e.g., 18.5 for 18.5%).
    Empty list if input is too short.
    """
    if len(prices) < window + 1:
        return []
    log_returns = []
    for i in range(1, len(prices)):
        prev, cur = prices[i - 1], prices[i]
        if prev <= 0 or cur <= 0:
            continue
        log_returns.append(math.log(cur / prev))

    if len(log_returns) < window:
        return []

    vols = []
    for end_idx in range(window, len(log_returns) + 1):
        window_rets = log_returns[end_idx - window:end_idx]
        mean = sum(window_rets) / window
        variance = sum((r - mean) ** 2 for r in window_rets) / (window - 1)
        annualized = math.sqrt(variance) * math.sqrt(annualize_days)
        vols.append(annualized * 100.0)
    return vols


def get_iv_rank_from_realized_vol(trader, underlying,
                                  window=REALIZED_VOL_WINDOW, today_fn=None):
    """Compute IV rank from the underlying's realized volatility history.

    Fallback for when no volatility index is available or serving data.
    Returns the same shape as get_iv_rank_from_index. The source field
    is set to "<SYMBOL> RV{window}" so operators can tell at a glance
    which method provided the number.
    """
    try:
        start = _history_start_str()
        end = time.strftime("%Y-%m-%d") if today_fn is None else today_fn()
        history = trader.get_history(
            underlying, interval="daily", start=start, end=end
        )
        prices = []
        for d in history:
            c = d.get("close")
            if c is None:
                continue
            try:
                prices.append(float(c))
            except (TypeError, ValueError):
                continue

        if len(prices) < window + 1:
            return {"rank": 0.0, "current": 0, "min": 0, "max": 0,
                    "source": f"{underlying} RV{window}",
                    "history_points": len(prices),
                    "error": f"insufficient price history for {underlying} "
                             f"({len(prices)} closes, need {window + 1})"}

        rv_series = compute_realized_vol_series(prices, window=window)
        if not rv_series:
            return {"rank": 0.0, "current": 0, "min": 0, "max": 0,
                    "source": f"{underlying} RV{window}",
                    "history_points": 0,
                    "error": "could not compute realized vol series"}

        current_rv = rv_series[-1]
        rank = compute_iv_rank(current_rv, rv_series)
        return {
            "rank": rank,
            "current": current_rv,
            "min": min(rv_series),
            "max": max(rv_series),
            "source": f"{underlying} RV{window}",
            "history_points": len(rv_series),
        }
    except Exception as e:
        logger.warning("Realized-vol IV rank for %s failed: %s", underlying, e)
        return {"rank": 0.0, "current": 0, "min": 0, "max": 0,
                "source": f"{underlying} RV{window}",
                "history_points": 0,
                "error": str(e)}


def get_iv_rank(trader, underlying, today_fn=None):
    """Get IV rank from the best-available source with automatic fallback.

    Order of preference:
      1. Matching volatility index (VIX/VXN/RVX/VXD) via the broker
      2. Realized volatility of the underlying itself

    Returns the same shape as the individual functions with an
    additional `fallback_reason` field when the primary source was
    tried but unusable.
    """
    primary = get_iv_rank_from_index(trader, underlying, today_fn=today_fn)
    if (primary is not None
            and not primary.get("error")
            and primary.get("history_points", 0) > 0):
        return primary

    fallback = get_iv_rank_from_realized_vol(trader, underlying, today_fn=today_fn)
    if primary is not None and primary.get("error"):
        fallback["fallback_from"] = primary.get("source")
        fallback["fallback_reason"] = primary["error"]
    return fallback


# ============================================================================
# Fallback: local IV snapshot tracking for symbols without a vol index
# ============================================================================

class IVHistoryStore:
    """Append-only daily IV snapshots per symbol, persisted to JSON.

    Use this for symbols without a matching volatility index. Call
    record_daily_iv(symbol, iv) once per day with the ATM IV pulled
    from the current option chain. Over time, build-up enough history
    (a few months minimum) to produce a meaningful IV rank.
    """

    def __init__(self, state_file):
        self.state_file = state_file
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self):
        if self.state_file and os.path.isfile(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("IV history load failed: %s", e)
        return {}

    def _save(self):
        if not self.state_file:
            return
        try:
            with open(self.state_file, "w") as f:
                json.dump(self._data, f)
        except OSError as e:
            logger.warning("IV history save failed: %s", e)

    def record_daily_iv(self, symbol, iv):
        """Record today's IV for a symbol (idempotent per day)."""
        today = time.strftime("%Y-%m-%d")
        with self._lock:
            series = self._data.setdefault(symbol, [])
            if series and series[-1][0] == today:
                series[-1] = [today, iv]
            else:
                series.append([today, iv])
            # Trim to 2 years
            if len(series) > 520:
                self._data[symbol] = series[-520:]
            self._save()

    def get_iv_rank(self, symbol, current_iv, min_points=20):
        """Compute IV rank from stored history.

        Returns dict like get_iv_rank_from_index, or None if insufficient data.
        """
        with self._lock:
            series = list(self._data.get(symbol, []))
        # Take the last 252 trading days (~52 weeks)
        values = [v for _, v in series[-252:]]
        if len(values) < min_points:
            return {"rank": 0.0, "current": current_iv,
                    "min": min(values) if values else 0,
                    "max": max(values) if values else 0,
                    "source": "local",
                    "history_points": len(values),
                    "error": "insufficient history (need {} more)".format(
                        min_points - len(values))}
        rank = compute_iv_rank(current_iv, values)
        return {
            "rank": rank,
            "current": current_iv,
            "min": min(values),
            "max": max(values),
            "source": "local",
            "history_points": len(values),
        }


def get_atm_iv_from_chain(chain, spot_price):
    """Extract at-the-money implied volatility from a Tradier option chain.

    Averages the IVs of the closest call and put to spot price. Returns
    None if the chain lacks Greeks.
    """
    if not chain or not spot_price:
        return None

    puts = [o for o in chain if o.get("option_type") == "put"]
    calls = [o for o in chain if o.get("option_type") == "call"]

    def closest_iv(options):
        best, best_dist = None, float("inf")
        for o in options:
            strike = o.get("strike")
            if strike is None:
                continue
            dist = abs(strike - spot_price)
            if dist < best_dist:
                greeks = o.get("greeks") or {}
                iv = greeks.get("mid_iv") or greeks.get("smv_vol")
                if iv is not None:
                    best = iv
                    best_dist = dist
        return best

    put_iv = closest_iv(puts)
    call_iv = closest_iv(calls)

    if put_iv is None and call_iv is None:
        return None
    if put_iv is None:
        return call_iv
    if call_iv is None:
        return put_iv
    return (put_iv + call_iv) / 2.0


# ============================================================================
# Cache
# ============================================================================

def get_cached_iv_rank(symbol, current_iv, iv_history_list):
    """Return cached IV rank if fresh, otherwise compute and cache."""
    now = time.time()
    with _cache_lock:
        entry = _cache.get(symbol)
        if entry and (now - entry["time"]) < _CACHE_TTL:
            return entry["rank"]
    rank = compute_iv_rank(current_iv, iv_history_list)
    with _cache_lock:
        _cache[symbol] = {"rank": rank, "time": now}
    return rank


def clear_cache():
    """Clear the IV rank cache."""
    with _cache_lock:
        _cache.clear()

"""IV rank and IV percentile computation with caching."""

import time
import threading

_cache = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 3600  # 1 hour


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

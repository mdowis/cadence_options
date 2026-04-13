"""Iron condor candidate scanner."""

import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)


class StrategyConfig:
    """Configuration for the iron condor strategy."""

    def __init__(self, target_dte=45, dte_tolerance_low=40, dte_tolerance_high=50,
                 target_delta=16, wing_width=10, min_iv_rank=30,
                 min_credit_pct_of_width=20, symbols=None):
        self.target_dte = target_dte
        self.dte_tolerance_low = dte_tolerance_low
        self.dte_tolerance_high = dte_tolerance_high
        self.target_delta = target_delta
        self.wing_width = wing_width
        self.min_iv_rank = min_iv_rank
        self.min_credit_pct_of_width = min_credit_pct_of_width
        self.symbols = symbols or ["SPY", "QQQ"]


class IronCondorCandidate:
    """A potential iron condor trade."""

    def __init__(self, symbol, expiration, dte, iv_rank,
                 short_put_symbol, short_put_strike,
                 long_put_symbol, long_put_strike,
                 short_call_symbol, short_call_strike,
                 long_call_symbol, long_call_strike,
                 credit, max_loss,
                 breakeven_low, breakeven_high,
                 put_delta, call_delta,
                 prob_profit, return_pct):
        self.symbol = symbol
        self.expiration = expiration
        self.dte = dte
        self.iv_rank = iv_rank
        self.short_put_symbol = short_put_symbol
        self.short_put_strike = short_put_strike
        self.long_put_symbol = long_put_symbol
        self.long_put_strike = long_put_strike
        self.short_call_symbol = short_call_symbol
        self.short_call_strike = short_call_strike
        self.long_call_symbol = long_call_symbol
        self.long_call_strike = long_call_strike
        self.credit = credit  # per-share credit
        self.max_loss = max_loss  # per-share max loss
        self.breakeven_low = breakeven_low
        self.breakeven_high = breakeven_high
        self.put_delta = put_delta
        self.call_delta = call_delta
        self.prob_profit = prob_profit
        self.return_pct = return_pct

    def to_dict(self):
        return self.__dict__.copy()

    def fingerprint(self):
        """Unique identifier for dedup."""
        return (f"{self.symbol}:{self.expiration}:"
                f"{self.short_put_strike}/{self.long_put_strike}/"
                f"{self.short_call_strike}/{self.long_call_strike}")


def _pick_expiration(expirations, target_dte, today=None):
    """Pick the expiration closest to target_dte. Returns (date_str, dte) or None."""
    if not expirations:
        return None
    if today is None:
        today = date.today()
    best = None
    best_diff = float("inf")
    for exp_str in expirations:
        try:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (exp_date - today).days
        if dte <= 0:
            continue
        diff = abs(dte - target_dte)
        if diff < best_diff:
            best = (exp_str, dte)
            best_diff = diff
    return best


def _find_strike_by_delta(options, target_delta, option_type):
    """Find the option closest to target_delta.

    For puts: delta is negative, we target -target_delta/100.
    For calls: delta is positive, we target +target_delta/100.
    """
    target = target_delta / 100.0
    if option_type == "put":
        target = -target

    best = None
    best_diff = float("inf")
    for opt in options:
        greeks = opt.get("greeks")
        if not greeks:
            continue
        delta = greeks.get("delta")
        if delta is None:
            continue
        diff = abs(delta - target)
        if diff < best_diff:
            best = opt
            best_diff = diff
    return best


def _find_option_at_strike(options, strike):
    """Find an option at the exact strike price."""
    for opt in options:
        if opt.get("strike") == strike:
            return opt
    return None


def _find_option_nearest_strike(options, target_strike, max_distance):
    """Find the option with strike closest to target, within max_distance.

    QQQ and some other chains have uneven strike grids -- $1 increments
    near ATM, $5 further out -- so the exact wing target (short - 10)
    may not exist as a listed strike. We pick the nearest available
    within a reasonable tolerance (typically half the wing width).
    Returns None if no strike is within range.
    """
    best = None
    best_dist = float("inf")
    for opt in options:
        strike = opt.get("strike")
        if strike is None:
            continue
        dist = abs(strike - target_strike)
        if dist < best_dist:
            best = opt
            best_dist = dist
    if best is None or best_dist > max_distance:
        return None
    return best


def find_iron_condor_candidates(trader, symbol, config, iv_rank, today=None):
    """Scan for iron condor candidates.

    Returns list of IronCondorCandidate sorted by return_pct descending.
    """
    if today is None:
        today = date.today()

    # 1. Get expirations and pick closest to target DTE
    expirations = trader.get_expirations(symbol)
    result = _pick_expiration(expirations, config.target_dte, today)
    if not result:
        logger.info("%s: no valid expirations found", symbol)
        return []
    exp_str, dte = result

    # Check DTE tolerance
    if dte < config.dte_tolerance_low or dte > config.dte_tolerance_high:
        logger.info("%s: closest expiration %s has DTE %d, outside tolerance [%d, %d]",
                    symbol, exp_str, dte, config.dte_tolerance_low, config.dte_tolerance_high)
        return []

    # 2. Fetch full option chain
    chain = trader.get_option_chain(symbol, exp_str, greeks=True)
    if not chain:
        logger.info("%s: empty option chain for %s", symbol, exp_str)
        return []

    # 3. Separate puts and calls
    puts = [o for o in chain if o.get("option_type") == "put"]
    calls = [o for o in chain if o.get("option_type") == "call"]

    if not puts or not calls:
        logger.info("%s: missing puts or calls in chain", symbol)
        return []

    # 4-5. Find short strikes by delta
    short_put = _find_strike_by_delta(puts, config.target_delta, "put")
    short_call = _find_strike_by_delta(calls, config.target_delta, "call")

    if not short_put or not short_call:
        logger.info("%s: could not find short strikes at %d delta", symbol, config.target_delta)
        return []

    short_put_strike = short_put["strike"]
    short_call_strike = short_call["strike"]

    # 6-7. Long strikes -- target is short strike +/- wing_width, but
    # chains (especially QQQ) have uneven strike grids, so pick the
    # nearest available strike within a tolerance of half the wing width.
    long_put_target = short_put_strike - config.wing_width
    long_call_target = short_call_strike + config.wing_width
    wing_tolerance = max(config.wing_width * 0.5, 2.5)

    long_put = _find_option_nearest_strike(
        puts, long_put_target, max_distance=wing_tolerance)
    long_call = _find_option_nearest_strike(
        calls, long_call_target, max_distance=wing_tolerance)

    if not long_put or not long_call:
        logger.info("%s: could not find wing strikes near %s/%s "
                    "(tolerance +/-%.1f)",
                    symbol, long_put_target, long_call_target, wing_tolerance)
        return []

    long_put_strike = long_put["strike"]
    long_call_strike = long_call["strike"]
    put_wing_width = short_put_strike - long_put_strike
    call_wing_width = long_call_strike - short_call_strike
    # Iron condor max loss is set by the wider wing
    effective_wing_width = max(put_wing_width, call_wing_width)

    # 8. Credit calculation
    short_put_bid = short_put.get("bid", 0) or 0
    short_call_bid = short_call.get("bid", 0) or 0
    long_put_ask = long_put.get("ask", 0) or 0
    long_call_ask = long_call.get("ask", 0) or 0

    credit = short_put_bid + short_call_bid - long_put_ask - long_call_ask

    if credit <= 0:
        logger.info("%s: negative credit %.2f", symbol, credit)
        return []

    # 9. Max loss = effective wing width - credit (per share)
    max_loss = effective_wing_width - credit

    if max_loss <= 0:
        logger.info("%s: non-positive max loss (credit exceeds wing width)", symbol)
        return []

    # Breakevens
    breakeven_low = short_put_strike - credit
    breakeven_high = short_call_strike + credit

    # Deltas
    put_delta = short_put.get("greeks", {}).get("delta", 0)
    call_delta = short_call.get("greeks", {}).get("delta", 0)

    # Probability of profit (approximate: 1 - delta on each side)
    prob_profit = (1 - abs(put_delta)) * (1 - abs(call_delta)) * 100

    # Return percentage
    return_pct = (credit / max_loss) * 100 if max_loss > 0 else 0

    # 10. Filters
    min_credit = config.min_credit_pct_of_width / 100.0 * effective_wing_width
    if credit < min_credit:
        logger.info("%s: credit %.2f below minimum %.2f (%.0f%% of width %.1f)",
                    symbol, credit, min_credit,
                    config.min_credit_pct_of_width, effective_wing_width)
        return []

    if iv_rank < config.min_iv_rank:
        logger.info("%s: IV rank %.1f below minimum %.1f", symbol, iv_rank, config.min_iv_rank)
        return []

    candidate = IronCondorCandidate(
        symbol=symbol,
        expiration=exp_str,
        dte=dte,
        iv_rank=iv_rank,
        short_put_symbol=short_put.get("symbol", ""),
        short_put_strike=short_put_strike,
        long_put_symbol=long_put.get("symbol", ""),
        long_put_strike=long_put_strike,
        short_call_symbol=short_call.get("symbol", ""),
        short_call_strike=short_call_strike,
        long_call_symbol=long_call.get("symbol", ""),
        long_call_strike=long_call_strike,
        credit=credit,
        max_loss=max_loss,
        breakeven_low=breakeven_low,
        breakeven_high=breakeven_high,
        put_delta=put_delta,
        call_delta=call_delta,
        prob_profit=prob_profit,
        return_pct=return_pct,
    )

    logger.info("%s: candidate %s %dDTE %s/%s-%s/%s credit=%.2f maxloss=%.2f "
                "return=%.1f%% IVR=%.0f",
                symbol, exp_str, dte,
                long_put_strike, short_put_strike,
                short_call_strike, long_call_strike,
                credit, max_loss, return_pct, iv_rank)

    return [candidate]

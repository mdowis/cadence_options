"""Portfolio-level Greek aggregation across open option positions.

Tradier's /accounts/{id}/positions endpoint returns OCC-format option
symbols and quantities but no Greeks. To compute portfolio Greek
exposure we:

  1. Parse each position's OCC symbol to get (underlying, expiration,
     option_type, strike).
  2. Group by (underlying, expiration) so we can fetch each option
     chain once -- chains are per-expiration.
  3. Look up each position's specific option in its chain and pull
     the per-share Greeks Tradier provides when greeks=true.
  4. Aggregate with the 100x contract multiplier and the quantity
     sign (negative quantity = short, flips Greek signs appropriately).

All returned values are in CENTS to match risk_manager's unit
conventions. 'delta_cents' is dollar-delta (the $ P&L per 1-point
move in the underlying, times 100 to yield cents). 'vega_cents' is
$ per 1% IV change times 100. 'theta_cents' is $ per day times 100.
"""

import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def _parse_occ_symbol(symbol):
    """Parse a Tradier OCC-format option symbol.

    Format: {ROOT}{YY}{MM}{DD}{C|P}{STRIKE*1000 zero-padded to 8 digits}
    Example: 'SPY260530P00435000' -> ('SPY', '2026-05-30', 'put', 435.0)

    Returns None on any parse error.
    """
    if not symbol or len(symbol) < 15:
        return None
    try:
        strike = int(symbol[-8:]) / 1000.0
        opt_type_char = symbol[-9]
        if opt_type_char == "C":
            opt_type = "call"
        elif opt_type_char == "P":
            opt_type = "put"
        else:
            return None
        date_str = symbol[-15:-9]
        yy = int(date_str[0:2])
        mm = int(date_str[2:4])
        dd = int(date_str[4:6])
        if not (1 <= mm <= 12 and 1 <= dd <= 31):
            return None
        year = 2000 + yy
        exp = "{:04d}-{:02d}-{:02d}".format(year, mm, dd)
        root = symbol[:-15]
        if not root:
            return None
        return (root, exp, opt_type, strike)
    except (ValueError, IndexError):
        return None


def _position_quantity(pos):
    """Signed quantity for a position dict. Negative = short."""
    q = pos.get("quantity")
    if q is None:
        return 0
    try:
        return float(q)
    except (TypeError, ValueError):
        return 0


def _safe_float(v, default=0.0):
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def aggregate_portfolio_greeks(trader, positions):
    """Fetch per-option Greeks and sum into portfolio-level dollar values.

    Returns {delta_cents, gamma_cents, vega_cents, theta_cents}. Units
    match risk_manager's expectations: all values in cents where the
    underlying quantity is a dollar amount.

    - delta_cents = sum(option_delta * qty * 100 * spot) * 100
                  (dollar-delta: $ P&L per 1-point underlying move)
    - vega_cents  = sum(option_vega * qty * 100) * 100
                  ($ change per 1% IV change)
    - theta_cents = sum(option_theta * qty * 100) * 100
                  ($ change per day)
    - gamma_cents = sum(option_gamma * qty * 100) * 100
                  (raw contract gamma * 100; informational only)

    Positions that can't be parsed or looked up are skipped rather
    than crashing the aggregation.
    """
    result = {
        "delta_cents": 0,
        "gamma_cents": 0,
        "vega_cents": 0,
        "theta_cents": 0,
    }
    if not positions:
        return result

    # Group positions by (underlying, expiration) so we fetch each
    # chain once. Chains are per-expiration.
    groups = defaultdict(list)
    for pos in positions:
        symbol = pos.get("symbol", "")
        parsed = _parse_occ_symbol(symbol)
        if parsed is None:
            continue
        root, expiration, _, _ = parsed
        groups[(root, expiration)].append((pos, parsed))

    spot_cache = {}
    delta_total = 0.0
    gamma_total = 0.0
    vega_total = 0.0
    theta_total = 0.0

    for (root, expiration), items in groups.items():
        # Fetch the chain with Greeks once for this underlying+expiration
        try:
            chain = trader.get_option_chain(root, expiration, greeks=True)
        except Exception as e:
            logger.warning("Greeks: chain fetch for %s %s failed: %s",
                           root, expiration, e)
            continue

        # Index chain by option symbol for O(1) lookup
        chain_by_symbol = {}
        for opt in chain:
            sym = opt.get("symbol")
            if sym:
                chain_by_symbol[sym] = opt

        # Look up spot price for dollar-delta calc
        if root not in spot_cache:
            try:
                q = trader.get_quote(root)
                spot = _safe_float(q.get("last"))
                if spot <= 0:
                    spot = _safe_float(q.get("close"))
                spot_cache[root] = spot if spot > 0 else 0
            except Exception as e:
                logger.warning("Greeks: quote for %s failed: %s", root, e)
                spot_cache[root] = 0

        spot = spot_cache[root]

        for pos, (_, _, _, _) in items:
            symbol = pos.get("symbol")
            qty = _position_quantity(pos)
            if qty == 0:
                continue
            opt = chain_by_symbol.get(symbol)
            if opt is None:
                # Option not found in chain (stale symbol?)
                continue
            greeks = opt.get("greeks") or {}
            delta = _safe_float(greeks.get("delta"))
            gamma = _safe_float(greeks.get("gamma"))
            vega = _safe_float(greeks.get("vega"))
            theta = _safe_float(greeks.get("theta"))

            # Per-contract multiplier is 100 shares
            # Signed by quantity (negative qty = short position)
            delta_total += delta * qty * 100 * spot  # dollar-delta
            gamma_total += gamma * qty * 100
            vega_total += vega * qty * 100           # $ per 1% IV
            theta_total += theta * qty * 100         # $ per day

    # Convert to cents (values above are in dollars)
    result["delta_cents"] = int(round(delta_total * 100))
    result["gamma_cents"] = int(round(gamma_total * 100))
    result["vega_cents"] = int(round(vega_total * 100))
    result["theta_cents"] = int(round(theta_total * 100))
    return result

"""Order placement and safety rails."""

import logging
import uuid

logger = logging.getLogger(__name__)


def build_iron_condor_legs(candidate, contracts=1):
    """Build 4-leg order for an iron condor.

    Returns list of (option_symbol, side, quantity) tuples.
    """
    return [
        (candidate.short_put_symbol, "sell_to_open", contracts),
        (candidate.long_put_symbol, "buy_to_open", contracts),
        (candidate.short_call_symbol, "sell_to_open", contracts),
        (candidate.long_call_symbol, "buy_to_open", contracts),
    ]


def build_close_legs(position, contracts=1):
    """Build legs to close an iron condor position.

    position should have short_put_symbol, long_put_symbol,
    short_call_symbol, long_call_symbol.
    Returns list of (option_symbol, side, quantity) tuples.
    """
    return [
        (position["short_put_symbol"], "buy_to_close", contracts),
        (position["long_put_symbol"], "sell_to_close", contracts),
        (position["short_call_symbol"], "buy_to_close", contracts),
        (position["long_call_symbol"], "sell_to_close", contracts),
    ]


def build_close_legs_from_tracked(tracked):
    """Build close legs from a TrackedPosition."""
    return [
        (tracked.short_put_symbol, "buy_to_close", tracked.contracts),
        (tracked.long_put_symbol, "sell_to_close", tracked.contracts),
        (tracked.short_call_symbol, "buy_to_close", tracked.contracts),
        (tracked.long_call_symbol, "sell_to_close", tracked.contracts),
    ]


def _safe_float(v):
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def compute_close_debit(trader, tracked):
    """Fetch the option chain for a tracked position's expiration and
    compute the per-share debit required to close.

    To close an iron condor we:
      - buy back the two shorts (pay ask)
      - sell the two longs (receive bid)

    Returns (debit, chain_by_symbol) or (None, None) on failure.
    """
    try:
        chain = trader.get_option_chain(
            tracked.symbol, tracked.expiration, greeks=False)
    except Exception as e:
        logger.warning("Close-debit chain fetch for %s %s failed: %s",
                       tracked.symbol, tracked.expiration, e)
        return None, None

    by_sym = {}
    for o in chain:
        sym = o.get("symbol")
        if sym:
            by_sym[sym] = o

    sp = by_sym.get(tracked.short_put_symbol, {})
    lp = by_sym.get(tracked.long_put_symbol, {})
    sc = by_sym.get(tracked.short_call_symbol, {})
    lc = by_sym.get(tracked.long_call_symbol, {})

    debit = (_safe_float(sp.get("ask"))
             + _safe_float(sc.get("ask"))
             - _safe_float(lp.get("bid"))
             - _safe_float(lc.get("bid")))
    # Guard against malformed chain: all zeros means we couldn't price
    # this position -- better to return None than submit a $0 debit.
    if debit <= 0 and all(o == {} for o in (sp, lp, sc, lc)):
        return None, by_sym
    return debit, by_sym


def execute_close(trader, tracked, limit_debit, dry_run=True, reason=""):
    """Submit a closing debit order for a tracked iron condor position.

    limit_debit is per-share. The order is submitted at that price as
    a day limit; if the market moves away it won't fill and we'll
    retry on the next cycle.
    """
    legs = build_close_legs_from_tracked(tracked)
    _validate_leg_count(legs)

    summary = (f"CLOSE {tracked.symbol} IC tag={tracked.tag} "
               f"@ ${limit_debit:.2f}debit ({reason})")

    if dry_run:
        detail = f"[DRY RUN] {summary}"
        logger.info(detail)
        return True, detail

    try:
        result = trader.place_multileg_order(
            symbol=tracked.symbol,
            legs=legs,
            order_type="debit",
            duration="day",
            price=limit_debit,
            tag=tracked.tag,  # same tag so close detection + P&L calc can match
        )
        order = result.get("order", {})
        order_id = order.get("id", "unknown")
        status = order.get("status", "unknown")
        detail = f"{summary} order_id={order_id} status={status}"
        logger.info(detail)
        return True, detail
    except Exception as e:
        detail = f"Close order failed: {e}"
        logger.error(detail)
        return False, detail


def _format_order_summary(candidate, contracts=1):
    """Concise human-readable summary of an iron condor order.

    Example: "QQQ IC 45DTE 557/567P 654/665C $2.32cr x1"
    """
    def _s(x):
        # Render strikes as int when exact, else one decimal
        try:
            return f"{int(x)}" if float(x) == int(x) else f"{x:.1f}"
        except (TypeError, ValueError):
            return str(x)

    parts = [
        f"{candidate.symbol} IC {candidate.dte}DTE",
        f"{_s(candidate.long_put_strike)}/{_s(candidate.short_put_strike)}P",
        f"{_s(candidate.short_call_strike)}/{_s(candidate.long_call_strike)}C",
        f"${candidate.credit:.2f}cr",
    ]
    if contracts != 1:
        parts.append(f"x{contracts}")
    return " ".join(parts)


def _validate_leg_count(legs):
    """Safety: only allow 4-leg (iron condor) or 2-leg (credit spread) orders."""
    if len(legs) not in (2, 4):
        raise ValueError(
            f"Invalid leg count {len(legs)}: only 4-leg iron condors or "
            f"2-leg credit spreads are allowed. No naked options."
        )


def execute_candidate(trader, risk_mgr, candidate, contracts=1,
                      dry_run=True, tracker=None):
    """Execute an iron condor trade through the full safety pipeline.

    Returns (success: bool, detail: str). When `tracker` is provided
    and the live order places successfully, records the entry into
    the tracker so closes can later be detected and P&L attributed.
    """
    tag = f"cadence-{uuid.uuid4().hex[:8]}"

    # 1. Pre-trade balance reconciliation (NOT cached)
    # Run in both dry-run and live modes so the risk check sees the
    # same equity it would in production. In dry-run, a network blip
    # or unexpected response shape doesn't abort -- we just log --
    # since no order is placed anyway. In live, a failed sync IS fatal
    # (we won't risk-check against stale numbers when real money is
    # on the line).
    try:
        balances = trader.get_account_balances()
        if not isinstance(balances, dict):
            raise ValueError("get_account_balances did not return a dict")
        bal = balances.get("balances", {})
        if not isinstance(bal, dict):
            raise ValueError("'balances' key is not a dict")
        total_equity = bal.get("total_equity")
        cash_val = bal.get("total_cash")
        if cash_val is None:
            cash_obj = bal.get("cash", {}) if isinstance(bal.get("cash"), dict) else {}
            cash_val = cash_obj.get("cash_available")
        if total_equity is None:
            raise ValueError("broker response missing total_equity")
        equity_cents = int(float(total_equity) * 100)
        cash_cents = int(float(cash_val or 0) * 100)
        if equity_cents <= 0:
            raise ValueError(f"broker equity is non-positive: {equity_cents}")
        risk_mgr.sync_actual_balance(cash_cents, portfolio_value_cents=equity_cents)
    except Exception as e:
        if not dry_run:
            return False, f"Pre-trade balance sync failed: {e}"
        logger.warning("Pre-trade balance sync failed (dry-run, "
                       "continuing with existing equity): %s", e)

    # 2. Risk check
    decision = risk_mgr.check_trade(candidate, contracts)
    if not decision.allowed:
        return False, f"Blocked by risk manager: {decision.reason}"

    # 3. Build order legs
    legs = build_iron_condor_legs(candidate, contracts)
    _validate_leg_count(legs)

    # Concise human-readable summary used in both dry-run and live detail
    summary = _format_order_summary(candidate, contracts)

    # 4. Dry run: log and return
    if dry_run:
        detail = f"[DRY RUN] {summary}"
        logger.info(detail)
        return True, detail

    # 5. Live: place the order
    try:
        result = trader.place_multileg_order(
            symbol=candidate.symbol,
            legs=legs,
            order_type="credit",
            duration="day",
            price=candidate.credit,
            tag=tag,
        )
        order = result.get("order", {})
        order_id = order.get("id", "unknown")
        status = order.get("status", "unknown")
        detail = f"{summary} order_id={order_id} status={status}"
        logger.info(detail)
        # Record locally so we can detect the close later and compute P&L
        if tracker is not None:
            try:
                tracker.record_entry(candidate, tag=tag, contracts=contracts)
            except Exception as e:
                logger.warning("Tracker record_entry failed: %s", e)
        return True, detail
    except Exception as e:
        detail = f"Order placement failed: {e}"
        logger.error(detail)
        return False, detail

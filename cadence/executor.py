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


def _validate_leg_count(legs):
    """Safety: only allow 4-leg (iron condor) or 2-leg (credit spread) orders."""
    if len(legs) not in (2, 4):
        raise ValueError(
            f"Invalid leg count {len(legs)}: only 4-leg iron condors or "
            f"2-leg credit spreads are allowed. No naked options."
        )


def execute_candidate(trader, risk_mgr, candidate, contracts=1, dry_run=True):
    """Execute an iron condor trade through the full safety pipeline.

    Returns (success: bool, detail: str).
    """
    tag = f"cadence-{uuid.uuid4().hex[:8]}"

    # 1. Pre-trade balance reconciliation (NOT cached)
    if not dry_run:
        try:
            balances = trader.get_account_balances()
            bal = balances.get("balances", {})
            total_equity = bal.get("total_equity", 0)
            cash = bal.get("total_cash", bal.get("cash", {}).get("cash_available", 0))
            equity_cents = int(float(total_equity) * 100)
            cash_cents = int(float(cash) * 100)
            risk_mgr.sync_actual_balance(cash_cents, portfolio_value_cents=equity_cents)
        except Exception as e:
            return False, f"Pre-trade balance sync failed: {e}"

    # 2. Risk check
    decision = risk_mgr.check_trade(candidate, contracts)
    if not decision.allowed:
        return False, f"Blocked by risk manager: {decision.reason}"

    # 3. Build order legs
    legs = build_iron_condor_legs(candidate, contracts)
    _validate_leg_count(legs)

    # 4. Dry run: log and return
    if dry_run:
        leg_desc = "; ".join(f"{sym} {side} x{qty}" for sym, side, qty in legs)
        detail = (f"[DRY RUN] Would place {candidate.symbol} iron condor: "
                  f"{leg_desc} @ {candidate.credit:.2f} credit, tag={tag}")
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
        detail = (f"Placed {candidate.symbol} iron condor: order_id={order_id}, "
                  f"status={status}, credit={candidate.credit:.2f}, tag={tag}")
        logger.info(detail)
        return True, detail
    except Exception as e:
        detail = f"Order placement failed: {e}"
        logger.error(detail)
        return False, detail

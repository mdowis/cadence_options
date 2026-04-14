"""Main entry point: .env loader, HTTP server, and glue between all modules."""

import json
import logging
import os
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from cadence.tradier_client import TradierClient
from cadence.risk_manager import RiskManager, RiskConfig
from cadence.strategy import StrategyConfig
from cadence.process_controller import ProcessController
from cadence.position_manager import PositionManager
from cadence.position_tracker import PositionTracker
from cadence.trade_ledger import TradeLedger
from cadence.notifier import build_from_env
from cadence.iv_rank import compute_iv_rank, get_cached_iv_rank

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("cadence")

# Module-level globals for handler access
_trader = None
_risk_mgr = None
_process_ctrl = None
_notifier = None
_position_mgr = None
_position_tracker = None
_trade_ledger = None
_env_path = None
_script_dir = None


# -- .env loader (regression test 7) ----------------------------------------

def load_dotenv(search_dirs=None):
    """Load .env file. Checks cwd first, then the script's directory.

    Returns the path loaded, or None if not found.
    """
    if search_dirs is None:
        search_dirs = [
            os.getcwd(),
            os.path.dirname(os.path.abspath(__file__)),
        ]
    for d in search_dirs:
        path = os.path.join(d, ".env")
        if os.path.isfile(path):
            _load_env_file(path)
            return path
    return None


def _load_env_file(path):
    """Parse a .env file and set environment variables."""
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Remove surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)


def _env(key, default=""):
    return os.environ.get(key, default)


def _env_int(key, default=0):
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default


def _resolve_tradier_creds(tradier_env):
    """Pick the right Tradier token/account pair for the given env.

    Preference:
      1. TRADIER_{ENV_UPPER}_ACCESS_TOKEN / TRADIER_{ENV_UPPER}_ACCOUNT_ID
      2. TRADIER_ACCESS_TOKEN / TRADIER_ACCOUNT_ID (legacy fallback)

    Returns (token, account_id, source) where source is one of
    "env-specific", "legacy", or "none" for diagnostics.
    """
    env_upper = tradier_env.upper()
    specific_token = _env(f"TRADIER_{env_upper}_ACCESS_TOKEN")
    specific_acct = _env(f"TRADIER_{env_upper}_ACCOUNT_ID")

    if specific_token and specific_acct:
        return specific_token, specific_acct, "env-specific"

    legacy_token = _env("TRADIER_ACCESS_TOKEN")
    legacy_acct = _env("TRADIER_ACCOUNT_ID")
    if legacy_token and legacy_acct:
        return legacy_token, legacy_acct, "legacy"

    # Prefer env-specific if only one is set, otherwise legacy
    token = specific_token or legacy_token
    acct = specific_acct or legacy_acct
    return token, acct, "none"


def _env_float(key, default=0.0):
    try:
        return float(_env(key, str(default)))
    except ValueError:
        return default


def _env_bool(key, default=False):
    return _env(key, str(default)).lower() in ("true", "1", "yes")


def _mask(s, show=4):
    if not s or len(s) <= show:
        return "***"
    return s[:show] + "*" * (len(s) - show)


# -- HTTP Handler ------------------------------------------------------------

class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the dashboard API."""

    def log_message(self, format, *args):
        logger.debug(format, *args)

    def _send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-cache, no-store")
        # No Access-Control-Allow-Origin: we serve the dashboard
        # same-origin; cross-origin scripts should NOT read our responses.
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, path):
        try:
            with open(path, "r") as f:
                body = f.read()
            # Inject a token meta tag so the dashboard JS can include
            # the bearer token on API calls. Only rendered when the
            # request arrived with a matching ?token= query param so
            # the token itself isn't handed out to anyone who fetches
            # the root page.
            token_meta = ""
            configured = _env("CADENCE_DASHBOARD_TOKEN")
            if configured:
                parsed = urlparse(self.path)
                qs = parse_qs(parsed.query)
                supplied = qs.get("token", [""])[0]
                if supplied == configured:
                    token_meta = (
                        '<meta name="cadence-auth-token" content="{}">'
                        .format(configured.replace('"', '&quot;'))
                    )
                else:
                    # Token required but not supplied -- render a
                    # minimal login page instead of the real dashboard.
                    body = (
                        "<!DOCTYPE html><meta charset='utf-8'>"
                        "<title>Cadence Options - Login</title>"
                        "<body style='font-family:sans-serif;"
                        "background:#0a0e17;color:#e2e8f0;padding:40px;"
                        "max-width:400px;margin:auto'>"
                        "<h1 style='font-size:16px'>Cadence Options</h1>"
                        "<p>Paste your CADENCE_DASHBOARD_TOKEN:</p>"
                        "<form>"
                        "<input type='password' name='token' style='"
                        "width:100%;padding:8px;background:#111827;"
                        "border:1px solid #1e2d4a;color:#e2e8f0;"
                        "font-family:monospace'>"
                        "<button type='submit' style='margin-top:12px;"
                        "padding:8px 16px;background:#06b6d4;border:0;"
                        "color:#000;cursor:pointer'>Continue</button>"
                        "</form></body>"
                    )
                    body_bytes = body.encode("utf-8")
                    self.send_response(401)
                    self.send_header("Content-Type", "text/html")
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    self.wfile.write(body_bytes)
                    return
            body = body.replace("</head>", token_meta + "</head>", 1)
            body_bytes = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body_bytes)
        except FileNotFoundError:
            self._send_json({"error": "dashboard.html not found"}, 404)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            return self.rfile.read(length)
        return b""

    # NOTE: do_OPTIONS is intentionally absent. We return 501 to CORS
    # preflights, which blocks cross-origin fetches that use non-simple
    # request headers (like X-Cadence-Client below). Combined with the
    # absence of Access-Control-Allow-Origin, this is our CSRF defense:
    # external sites cannot read our responses or issue state-changing
    # requests that trip our header check.

    def _check_csrf(self):
        """Require a custom header on mutating endpoints.

        Browsers cannot add this header to a cross-origin 'simple'
        request (POSTs with text/plain, form-urlencoded, or multipart)
        without a CORS preflight. Since we don't respond to preflight,
        external sites cannot issue POSTs to our mutating endpoints.
        Same-origin dashboard JS can set it freely.
        """
        expected = "dashboard"
        actual = self.headers.get("X-Cadence-Client", "")
        return actual == expected

    def _check_auth(self):
        """If CADENCE_DASHBOARD_TOKEN is configured, require it as a
        bearer token in X-Cadence-Auth. Empty config = no auth (relies
        on 127.0.0.1 binding and CSRF header)."""
        expected = _env("CADENCE_DASHBOARD_TOKEN")
        if not expected:
            return True
        supplied = self.headers.get("X-Cadence-Auth", "")
        return supplied == expected

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        # Serve the dashboard HTML unauthenticated (it will supply
        # credentials for subsequent /api calls). Everything else
        # requires auth when a token is configured.
        if path == "/":
            html_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "dashboard.html"
            )
            self._send_html(html_path)
            return

        if not self._check_auth():
            self._send_json({"error": "unauthorized"}, 401)
            return

        if path == "/api/scan":
            status = _process_ctrl.get_status() if _process_ctrl else {}
            self._send_json({
                "candidates": status.get("candidates", []),
                "scanner": status.get("scanner", {}),
            })

        elif path == "/api/positions":
            positions = []
            if _trader and _trader.authenticated:
                try:
                    positions = _trader.get_positions()
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)
                    return
            self._send_json({"positions": positions})

        elif path == "/api/risk":
            status = _risk_mgr.get_status() if _risk_mgr else {}
            self._send_json(status)

        elif path == "/api/processes":
            status = _process_ctrl.get_status() if _process_ctrl else {}
            # Augment config with auth and env info
            if "config" not in status:
                status["config"] = {}
            status["config"]["tradier_env"] = _env("TRADIER_ENV", "sandbox")
            status["config"]["authenticated"] = _trader.authenticated if _trader else False
            self._send_json(status)

        elif path == "/api/iv_rank":
            data = {}
            if _process_ctrl:
                status = _process_ctrl.get_status()
                iv_ranks = status.get("iv_ranks", {})
                for symbol in _process_ctrl.strategy_config.symbols:
                    info = iv_ranks.get(symbol, {})
                    data[symbol] = {
                        "rank": info.get("rank", 0),
                        "current": info.get("current", 0),
                        "min": info.get("min", 0),
                        "max": info.get("max", 0),
                        "source": info.get("source", ""),
                        "history_points": info.get("history_points", 0),
                    }
            self._send_json(data)

        elif path == "/api/state-summary":
            # Unified diagnostic: broker vs tracker vs risk manager
            # state. Shows counts AND a legs-diff so the operator
            # can see exactly which broker legs are untracked and
            # which tracker entries have no matching broker position.
            self._send_json(_build_state_summary())

        elif path == "/api/orders":
            # Broker orders (pending + filled) so operator can see
            # close orders that haven't filled yet and are blocking
            # tracker cleanup.
            orders = []
            if _trader and _trader.authenticated:
                try:
                    orders = _trader.get_orders()
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)
                    return
            self._send_json({"orders": orders})

        elif path == "/api/tracked-positions":
            # Our own view of open iron condors with unrealized P&L.
            # P&L uses MIDPOINT close debit (fair mark) so the
            # dashboard isn't chronically pessimistic on wide spreads.
            # Real close orders still price at the conservative bid/ask
            # boundary -- see compute_close_debit.
            from cadence.executor import (
                compute_close_debit_mid, _legs_from_chain, _safe_float)
            tracked_info = []
            # Cache underlying quotes per request -- multiple ICs
            # often share the same underlying.
            _spot_cache = {}
            def _get_spot(sym):
                if sym in _spot_cache:
                    return _spot_cache[sym]
                try:
                    q = _trader.get_quote(sym)
                    spot = _safe_float(q.get("last")) or _safe_float(q.get("close"))
                except Exception:
                    spot = 0
                _spot_cache[sym] = spot
                return spot
            if _position_tracker and _trader and _trader.authenticated:
                # Prefer actual fill price from Tradier's order history
                # over our pre-fill midpoint estimate.
                for t in _position_tracker.get_open():
                    debit = None
                    pnl_dollars = None
                    pnl_pct = None

                    # 1. Try the actual fill price from Tradier orders
                    actual_fill = None
                    try:
                        actual_fill = _position_tracker.get_entry_fill_price(
                            t, _trader)
                    except Exception:
                        actual_fill = None

                    # 2. Fall back to our stored midpoint, else the
                    # conservative entry credit.
                    if actual_fill is not None and actual_fill > 0:
                        entry_for_pnl = actual_fill
                        entry_source = "actual"
                    else:
                        entry_for_pnl = getattr(t, "entry_credit_mid",
                                                 t.entry_credit)
                        entry_source = ("mid" if entry_for_pnl != t.entry_credit
                                        else "legacy")
                    is_legacy = (entry_source == "legacy")
                    try:
                        sp, lp, sc, lc, _by_sym = _legs_from_chain(_trader, t)
                    except Exception:
                        sp = lp = sc = lc = None
                    if sp is not None:
                        # Compute midpoint debit
                        from cadence.executor import _opt_mid as _mid_fn
                        debit = (_mid_fn(sp) + _mid_fn(sc)
                                 - _mid_fn(lp) - _mid_fn(lc))
                        if debit == 0 and all(o == {} for o in (sp, lp, sc, lc)):
                            debit = None
                        # For legacy entries, adjust entry upward by
                        # the sum of half-spreads on all four legs.
                        if is_legacy and debit is not None:
                            half_spread_sum = 0.0
                            for leg in (sp, sc, lp, lc):
                                bid = _safe_float(leg.get("bid"))
                                ask = _safe_float(leg.get("ask"))
                                if bid > 0 and ask > 0:
                                    half_spread_sum += (ask - bid) / 2
                            entry_for_pnl = t.entry_credit + half_spread_sum
                    if debit is not None and entry_for_pnl > 0:
                        pnl_dollars = (entry_for_pnl - debit) * 100 * t.contracts
                        pnl_pct = ((entry_for_pnl - debit)
                                   / entry_for_pnl) * 100
                    tracked_info.append({
                        "tag": t.tag,
                        "symbol": t.symbol,
                        "expiration": t.expiration,
                        "dte": t.current_dte(),
                        "contracts": t.contracts,
                        "entry_credit": t.entry_credit,
                        "entry_credit_mid": entry_for_pnl,
                        "entry_is_estimated": is_legacy,
                        "entry_source": entry_source,
                        "current_debit": debit,  # midpoint
                        "pnl_dollars": pnl_dollars,
                        "pnl_pct": pnl_pct,
                        "short_put_strike": t.short_put_strike,
                        "long_put_strike": t.long_put_strike,
                        "short_call_strike": t.short_call_strike,
                        "long_call_strike": t.long_call_strike,
                        "entry_time": t.entry_time,
                        "entry_underlying_price": getattr(
                            t, "entry_underlying_price", None),
                        # Visualization data: underlying price right now,
                        # plus distance-to-short-strike buffers in dollars
                        # and percent. Lets the frontend render an inline
                        # strike ladder + danger indicator without needing
                        # extra round trips.
                        "current_price": _get_spot(t.symbol),
                        "put_buffer_dollars":
                            _get_spot(t.symbol) - t.short_put_strike
                            if _get_spot(t.symbol) else None,
                        "call_buffer_dollars":
                            t.short_call_strike - _get_spot(t.symbol)
                            if _get_spot(t.symbol) else None,
                        "put_buffer_pct":
                            ((_get_spot(t.symbol) - t.short_put_strike)
                             / _get_spot(t.symbol) * 100)
                            if _get_spot(t.symbol) else None,
                        "call_buffer_pct":
                            ((t.short_call_strike - _get_spot(t.symbol))
                             / _get_spot(t.symbol) * 100)
                            if _get_spot(t.symbol) else None,
                    })
            self._send_json({"tracked": tracked_info})

        elif path == "/api/trade-ledger":
            # Full closed-trade records with entry+exit context, plus
            # aggregate stats for strategy analysis.
            if not _trade_ledger:
                self._send_json({"records": [], "stats": {}})
                return
            records = _trade_ledger.read_all(limit=200)
            stats = _trade_ledger.summary_stats()
            self._send_json({"records": records, "stats": stats})

        elif path == "/api/diagnostics":
            tradier_env = _env("TRADIER_ENV", "sandbox")
            token, acct, cred_source = _resolve_tradier_creds(tradier_env)
            self._send_json({
                "env_path": _env_path or "NOT FOUND",
                "tradier_env": tradier_env,
                "credentials_source": cred_source,
                "token": _mask(token),
                "account_id": _mask(acct),
                "sandbox_configured": bool(
                    _env("TRADIER_SANDBOX_ACCESS_TOKEN") and
                    _env("TRADIER_SANDBOX_ACCOUNT_ID")),
                "production_configured": bool(
                    _env("TRADIER_PRODUCTION_ACCESS_TOKEN") and
                    _env("TRADIER_PRODUCTION_ACCOUNT_ID")),
                "authenticated": _trader.authenticated if _trader else False,
                "scanner": _process_ctrl.get_status()["scanner"] if _process_ctrl else {},
                "executor": _process_ctrl.get_status()["executor"] if _process_ctrl else {},
                "telegram": _notifier.get_stats() if _notifier else {},
            })

        else:
            self._send_json({"error": "Not found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        # Every mutating endpoint requires the CSRF header. This blocks
        # cross-origin state changes -- a malicious page cannot set a
        # custom header on a simple POST without CORS preflight, and
        # we don't respond to preflight, so the POST never happens.
        if not self._check_csrf():
            self._send_json({"error": "CSRF check failed: missing "
                                      "X-Cadence-Client header"}, 403)
            return
        if not self._check_auth():
            self._send_json({"error": "unauthorized"}, 401)
            return

        if path == "/api/scanner/start":
            if _process_ctrl:
                _process_ctrl.start_scanner()
            self._send_json({"status": "started"})

        elif path == "/api/scanner/stop":
            if _process_ctrl:
                _process_ctrl.stop_scanner()
            self._send_json({"status": "stopped"})

        elif path == "/api/executor/start":
            if _process_ctrl:
                _process_ctrl.start_executor()
            self._send_json({"status": "started"})

        elif path == "/api/executor/stop":
            if _process_ctrl:
                _process_ctrl.stop_executor()
            self._send_json({"status": "stopped"})

        elif path == "/api/executor/dry-run":
            # Toggle dry_run. Body is form-encoded "value=true|false".
            # Production-live requires explicit confirmation via a second
            # header so accidental fat-finger requests can't trigger real
            # orders; sandbox-live (paper) is single-confirm client-side.
            body = self._read_body().decode("utf-8")
            params = {}
            for pair in body.split("&"):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    params[k] = v
            new_val = params.get("value", "").lower()
            if new_val not in ("true", "false"):
                self._send_json({"error": "value must be 'true' or 'false'"}, 400)
                return
            dry_run_target = new_val == "true"
            # Safety: never allow enabling production live via this endpoint
            # unless an explicit confirm header is present. Sandbox paper is fine.
            if (not dry_run_target) and _env("TRADIER_ENV", "sandbox") == "production":
                confirm = self.headers.get("X-Cadence-Confirm-Live", "")
                if confirm != "CONFIRM-PRODUCTION-LIVE":
                    self._send_json({
                        "error": "production live requires X-Cadence-Confirm-Live: "
                                 "CONFIRM-PRODUCTION-LIVE header"
                    }, 403)
                    return
            if _process_ctrl:
                _process_ctrl.set_dry_run(dry_run_target)
            self._send_json({"status": "ok", "dry_run": dry_run_target})

        elif path == "/api/risk/kill-switch/activate":
            if _risk_mgr:
                _risk_mgr.activate_kill_switch("Manual activation via dashboard")
            self._send_json({"status": "activated"})

        elif path == "/api/risk/kill-switch/deactivate":
            # Require explicit confirmation header so an attacker or
            # fat-finger cannot silently re-enable trading after a
            # kill. The dashboard's Resume button sends this header.
            confirm = self.headers.get("X-Cadence-Confirm-Resume", "")
            if confirm != "CONFIRM-RESUME":
                self._send_json({
                    "error": "resume requires X-Cadence-Confirm-Resume: "
                             "CONFIRM-RESUME header"
                }, 403)
                return
            if _risk_mgr:
                _risk_mgr.deactivate_kill_switch()
            self._send_json({"status": "deactivated"})

        elif path == "/api/risk/reset-daily":
            if _risk_mgr:
                _risk_mgr.reset_daily()
            self._send_json({"status": "reset"})

        elif path == "/api/reconcile":
            # Bring the tracker back into parity with the broker.
            # Walks Tradier's filled credit multileg orders, and for
            # each one whose four legs are currently open at the
            # broker AND aren't already in the tracker, adds a
            # tracker entry using the order's avg_fill_price as the
            # entry credit.
            result = _reconcile_tracker_with_broker()
            self._send_json(result)

        elif path.startswith("/api/orders/") and path.endswith("/cancel"):
            # Cancel a pending broker order by order_id. For stuck
            # close orders that never filled.
            order_id = path.split("/")[-2]
            if not _trader or not _trader.authenticated:
                self._send_json({"error": "broker not authenticated"}, 401)
                return
            try:
                result = _trader.cancel_order(order_id)
                self._send_json({"ok": True, "result": result})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)

        elif (path.startswith("/api/tracked-positions/")
              and path.endswith("/forget")):
            # Manually drop a tracker entry without submitting a close
            # order. Use when the position was closed outside the bot
            # (Tradier UI, expiration, assignment) or when a stuck
            # phantom needs to be evicted. Does NOT touch the broker.
            tag = path.split("/")[-2]
            if not _position_tracker:
                self._send_json({"error": "tracker not initialized"}, 500)
                return
            tracked = _position_tracker.get_by_tag(tag)
            if tracked is None:
                self._send_json({"error": f"no tracked position with tag {tag}"}, 404)
                return
            _position_tracker.remove(tag)
            logger.info("Tracker: forgot tag=%s symbol=%s (manual evict)",
                        tag, tracked.symbol)
            self._send_json({"ok": True, "detail": f"forgot {tag}"})

        elif (path.startswith("/api/tracked-positions/")
              and path.endswith("/close")):
            # Manually close a tracked iron condor by tag. Submits a
            # debit order priced at the current market close-debit;
            # if the market moves away the order won't fill and the
            # auto-exit loop (if conditions hold) will retry next cycle.
            tag = path.split("/")[-2]
            if not _position_tracker:
                self._send_json({"error": "tracker not initialized"}, 500)
                return
            if not _trader or not _trader.authenticated:
                self._send_json({"error": "broker not authenticated"}, 401)
                return
            tracked = _position_tracker.get_by_tag(tag)
            if tracked is None:
                self._send_json({"error": f"no tracked position with tag {tag}"}, 404)
                return
            from cadence.executor import compute_close_debit, execute_close
            debit, _ = compute_close_debit(_trader, tracked)
            if debit is None:
                self._send_json({"error": "could not price close"}, 500)
                return
            dry = _process_ctrl.dry_run if _process_ctrl else True
            ok, detail = execute_close(
                _trader, tracked, limit_debit=debit,
                dry_run=dry, reason="manual",
                tracker=_position_tracker,
            )
            # Trigger an immediate broker sync so the dashboard reflects
            # the close within ~1 second (instead of waiting for the
            # next periodic sync). Skip in dry-run since nothing changed
            # at the broker.
            if ok and not dry and _process_ctrl:
                try:
                    _process_ctrl._sync_broker_state()
                except Exception as e:
                    logger.warning("Post-close sync failed: %s", e)
            self._send_json({"ok": ok, "detail": detail,
                             "close_debit": debit, "dry_run": dry})

        elif path == "/api/test-fetch":
            if _trader and _trader.authenticated:
                try:
                    quote = _trader.get_quote("SPY")
                    self._send_json({"quote": quote})
                except Exception as e:
                    self._send_json({"error": str(e)}, 500)
            else:
                self._send_json({"error": "Not authenticated"}, 401)

        elif path == "/api/telegram/test":
            if _notifier and _notifier.enabled:
                _notifier.send("[Cadence] Test message from dashboard")
                self._send_json({"status": "sent"})
            else:
                self._send_json({"error": "Telegram not configured"}, 400)

        else:
            self._send_json({"error": "Not found"}, 404)


# -- Telegram command handlers -----------------------------------------------

def _cmd_help(*args):
    commands = [
        "/help - Show this help",
        "/status - Current risk status",
        "/positions - Open positions",
        "/decisions - Recent executor decisions",
        "/config - Current configuration",
        "/kill - Activate kill switch",
        "/resume - Deactivate kill switch",
        "/reset - Reset daily metrics",
        "/scanner_start - Start scanner",
        "/scanner_stop - Stop scanner",
        "/exec_start - Start executor",
        "/exec_stop - Stop executor",
        "/exec_live - Switch to live trading (requires CONFIRM)",
    ]
    return "\n".join(commands)


def _cmd_status(*args):
    if not _risk_mgr:
        return "Risk manager not initialized"
    s = _risk_mgr.get_status()
    eq = s["equity"]
    dd = s["drawdown"]
    return (
        f"Equity: ${eq['current']/100:.2f}\n"
        f"Drawdown: {dd['current_pct']:.1f}% (ref: {dd['reference_mode']})\n"
        f"Kill switch: {'ACTIVE' if s['kill_switch']['active'] else 'off'}\n"
        f"Positions: {s['positions']['count']}/{s['positions']['max']}"
    )


def _cmd_positions(*args):
    if not _trader or not _trader.authenticated:
        return "Not authenticated"
    try:
        positions = _trader.get_positions()
        if not positions:
            return "No open positions"
        lines = []
        for p in positions:
            lines.append(f"{p.get('symbol', '?')}: qty={p.get('quantity', '?')}")
        return "\n".join(lines)
    except Exception as e:
        return f"Error: {e}"


def _cmd_decisions(*args):
    if not _process_ctrl:
        return "Process controller not initialized"
    status = _process_ctrl.get_status()
    decisions = status["executor"].get("recent_decisions", [])
    if not decisions:
        return "No recent decisions"
    lines = []
    for d in decisions[-10:]:
        ok = "OK" if d.get("success") else "BLOCKED"
        lines.append(f"[{ok}] {d.get('ticker', '?')}: {d.get('detail', '')[:80]}")
    return "\n".join(lines)


def _cmd_config(*args):
    if not _process_ctrl:
        return "Not initialized"
    s = _process_ctrl.get_status()
    c = s.get("config", {})
    return "\n".join(f"{k}: {v}" for k, v in c.items())


def _cmd_kill(*args):
    if _risk_mgr:
        _risk_mgr.activate_kill_switch("Telegram /kill command")
    return "Kill switch activated"


def _cmd_resume(*args):
    if _risk_mgr:
        _risk_mgr.deactivate_kill_switch()
    return "Kill switch deactivated"


def _cmd_reset(*args):
    if _risk_mgr:
        _risk_mgr.reset_daily()
    return "Daily metrics reset"


def _cmd_scanner_start(*args):
    if _process_ctrl:
        _process_ctrl.start_scanner()
    return "Scanner started"


def _cmd_scanner_stop(*args):
    if _process_ctrl:
        _process_ctrl.stop_scanner()
    return "Scanner stopped"


def _cmd_exec_start(*args):
    if _process_ctrl:
        _process_ctrl.start_executor()
    return "Executor started"


def _cmd_exec_stop(*args):
    if _process_ctrl:
        _process_ctrl.stop_executor()
    return "Executor stopped"


def _cmd_exec_live(*args):
    # This command is registered with notifier.register_confirmation,
    # which re-dispatches the handler with "__confirmed__" appended to
    # args after the user replies CONFIRM. On the FIRST invocation we
    # must NOT enable live trading -- we just return the prompt string,
    # which the notifier sends back to the user. The dangerous action
    # happens only on the confirmed second call.
    if "__confirmed__" not in args:
        env = _env("TRADIER_ENV", "sandbox")
        env_warning = " (PRODUCTION -- real money)" if env == "production" else " (sandbox paper)"
        return ("Enable live trading" + env_warning + "? Reply CONFIRM "
                "within 30s to proceed.")
    if _process_ctrl:
        _process_ctrl.set_dry_run(False)
    return "LIVE TRADING ENABLED. Executor will place real orders."


# -- State reconciliation ---------------------------------------------------

def _build_state_summary():
    """Snapshot of broker / tracker / risk-manager state plus a diff
    so the operator can see where they've drifted out of sync."""
    summary = {
        "broker": {"legs": [], "leg_count": 0, "authenticated": False},
        "tracker": {"entries": [], "count": 0},
        "risk": {"daily_pnl_cents": 0, "daily_trade_count": 0,
                 "equity_cents": 0, "position_count": 0},
        "diff": {"untracked_broker_legs": [], "orphan_tracker_entries": []},
    }
    if _trader and _trader.authenticated:
        summary["broker"]["authenticated"] = True
        try:
            positions = _trader.get_positions()
            summary["broker"]["legs"] = [
                {"symbol": p.get("symbol"),
                 "quantity": p.get("quantity"),
                 "cost_basis": p.get("cost_basis")}
                for p in positions
            ]
            summary["broker"]["leg_count"] = len(positions)
        except Exception as e:
            summary["broker"]["error"] = str(e)
    if _position_tracker:
        tracked = _position_tracker.get_open()
        summary["tracker"]["count"] = len(tracked)
        summary["tracker"]["entries"] = [
            {"tag": t.tag, "symbol": t.symbol,
             "expiration": t.expiration, "contracts": t.contracts,
             "legs": list(t.leg_symbols())}
            for t in tracked
        ]
        # Compute diff
        broker_symbols = {p["symbol"] for p in summary["broker"]["legs"]
                          if p.get("symbol")}
        tracker_symbols = set()
        for t in tracked:
            tracker_symbols.update(t.leg_symbols())
        summary["diff"]["untracked_broker_legs"] = sorted(
            broker_symbols - tracker_symbols)
        summary["diff"]["orphan_tracker_legs"] = sorted(
            tracker_symbols - broker_symbols)
    if _risk_mgr:
        r = _risk_mgr.get_status()
        summary["risk"] = {
            "daily_pnl_cents": r["daily"]["pnl_cents"],
            "daily_trade_count": r["daily"]["trade_count"],
            "equity_cents": r["equity"]["current"],
            "position_count": r["positions"]["count"],
            "position_leg_count": r["positions"].get("leg_count", 0),
        }
    return summary


def _reconcile_tracker_with_broker():
    """Rebuild the tracker from Tradier's filled credit multileg orders.

    Algorithm:
      1. Fetch current broker positions (set of open leg symbols).
      2. Fetch all orders from Tradier.
      3. For each filled credit multileg order:
         - Extract its 4 leg symbols
         - If all 4 are currently open at the broker AND we don't
           already track this tag, create a tracker entry.
      4. Optionally: drop tracker entries whose legs are no longer
         in broker (orphans).

    Returns a summary dict of what was done.
    """
    result = {
        "adopted": [],
        "dropped_orphans": [],
        "skipped_no_match": 0,
        "error": None,
    }
    if not _trader or not _trader.authenticated:
        result["error"] = "broker not authenticated"
        return result
    if not _position_tracker:
        result["error"] = "tracker not initialized"
        return result

    try:
        positions = _trader.get_positions()
        orders = _trader.get_orders()
    except Exception as e:
        result["error"] = f"broker fetch failed: {e}"
        return result

    broker_symbols = {p.get("symbol") for p in positions if p.get("symbol")}
    existing_tags = {t.tag for t in _position_tracker.get_open()}

    # Group existing tracker leg-sets by leg symbols so we can skip
    # orders whose legs already match a tracker entry
    existing_leg_sets = [set(t.leg_symbols())
                         for t in _position_tracker.get_open()]

    from cadence.greeks import _parse_occ_symbol
    from cadence.strategy import IronCondorCandidate
    from datetime import datetime

    for o in orders:
        status = (o.get("status") or "").lower()
        if status != "filled":
            continue
        otype = (o.get("type") or "").lower()
        if otype and otype != "credit":
            continue  # only credit entries

        order_legs_raw = o.get("leg") or o.get("legs") or []
        if isinstance(order_legs_raw, dict):
            order_legs_raw = [order_legs_raw]
        leg_syms = []
        for leg in order_legs_raw:
            sym = leg.get("option_symbol") or leg.get("symbol")
            if sym:
                leg_syms.append(sym)

        if len(leg_syms) != 4:
            continue
        leg_set = set(leg_syms)

        # Skip if all legs aren't currently at broker
        if not leg_set.issubset(broker_symbols):
            result["skipped_no_match"] += 1
            continue

        # Skip if tracker already has these legs
        if any(leg_set == s for s in existing_leg_sets):
            continue

        # Parse the legs to figure out which is short/long put/call
        parsed_legs = []
        underlying = None
        expiration = None
        for sym in leg_syms:
            parsed = _parse_occ_symbol(sym)
            if parsed is None:
                break
            root, exp, opt_type, strike = parsed
            parsed_legs.append({"symbol": sym, "type": opt_type,
                                 "strike": strike})
            underlying = root
            expiration = exp
        if len(parsed_legs) != 4 or underlying is None:
            continue

        puts = sorted([l for l in parsed_legs if l["type"] == "put"],
                      key=lambda l: l["strike"])
        calls = sorted([l for l in parsed_legs if l["type"] == "call"],
                       key=lambda l: l["strike"])
        if len(puts) != 2 or len(calls) != 2:
            continue

        # Iron condor: lower put = long, upper put = short,
        # lower call = short, upper call = long
        long_put, short_put = puts
        short_call, long_call = calls

        # Verify by checking quantities at broker: shorts should be
        # negative, longs positive
        qty_by_sym = {}
        for p in positions:
            try:
                qty_by_sym[p.get("symbol")] = float(p.get("quantity", 0))
            except (TypeError, ValueError):
                qty_by_sym[p.get("symbol")] = 0
        if qty_by_sym.get(short_put["symbol"], 0) >= 0:
            continue  # not actually short
        if qty_by_sym.get(short_call["symbol"], 0) >= 0:
            continue
        if qty_by_sym.get(long_put["symbol"], 0) <= 0:
            continue
        if qty_by_sym.get(long_call["symbol"], 0) <= 0:
            continue

        # Contracts from the order or qty
        contracts = 1
        try:
            contracts = abs(int(qty_by_sym.get(short_put["symbol"], 1)))
            if contracts <= 0:
                contracts = 1
        except (TypeError, ValueError):
            contracts = 1

        entry_credit = _env_float_field(o, "avg_fill_price") or \
                       _env_float_field(o, "price") or 0
        if entry_credit <= 0:
            # Can't determine entry credit -- skip
            continue

        # DTE at entry is today-diff from expiration; close enough
        try:
            exp_date = datetime.strptime(expiration, "%Y-%m-%d").date()
            from datetime import date
            dte_at_entry = (exp_date - date.today()).days
        except ValueError:
            dte_at_entry = 0

        # Use order's tag if present, else synthesize one
        tag = o.get("tag") or f"adopted-{o.get('id', 'unknown')}"
        if tag in existing_tags:
            continue

        # Parse create_date for entry_time
        entry_time = None
        create = o.get("create_date") or o.get("transaction_date")
        if create:
            try:
                dt = datetime.strptime(create[:19], "%Y-%m-%dT%H:%M:%S")
                entry_time = dt.timestamp()
            except (ValueError, TypeError):
                pass

        # Synthesize a minimal IronCondorCandidate for record_entry
        cand = IronCondorCandidate(
            symbol=underlying,
            expiration=expiration,
            dte=dte_at_entry,
            iv_rank=0,
            short_put_symbol=short_put["symbol"],
            short_put_strike=short_put["strike"],
            long_put_symbol=long_put["symbol"],
            long_put_strike=long_put["strike"],
            short_call_symbol=short_call["symbol"],
            short_call_strike=short_call["strike"],
            long_call_symbol=long_call["symbol"],
            long_call_strike=long_call["strike"],
            credit=entry_credit,
            credit_mid=entry_credit,  # we have the actual fill, it IS the mid
            max_loss=max(short_put["strike"] - long_put["strike"],
                          long_call["strike"] - short_call["strike"]) - entry_credit,
            breakeven_low=short_put["strike"] - entry_credit,
            breakeven_high=short_call["strike"] + entry_credit,
            put_delta=0, call_delta=0, prob_profit=0, return_pct=0,
        )
        _position_tracker.record_entry(
            cand, tag=tag, contracts=contracts, entry_time=entry_time)
        existing_tags.add(tag)
        existing_leg_sets.append(leg_set)
        result["adopted"].append({
            "tag": tag, "symbol": underlying,
            "expiration": expiration, "entry_credit": entry_credit,
            "contracts": contracts,
        })

    # Drop orphan tracker entries (legs not in broker)
    for t in list(_position_tracker.get_open()):
        if not (set(t.leg_symbols()) & broker_symbols):
            # Only drop if old enough to not be a fresh unfilled entry
            import time
            age = time.time() - (t.entry_time or time.time())
            if age > 300:
                _position_tracker.remove(t.tag)
                result["dropped_orphans"].append(
                    {"tag": t.tag, "symbol": t.symbol})

    return result


def _env_float_field(d, key):
    """Safely extract a positive float from a dict."""
    try:
        v = float(d.get(key, 0) or 0)
        return v if v > 0 else 0
    except (TypeError, ValueError):
        return 0


# -- Main --------------------------------------------------------------------

def main():
    global _trader, _risk_mgr, _process_ctrl, _notifier, _position_mgr
    global _position_tracker, _trade_ledger, _env_path, _script_dir

    _script_dir = os.path.dirname(os.path.abspath(__file__))

    # 1. Load .env
    _env_path = load_dotenv()
    print(f"[Cadence] .env: {_env_path or 'NOT FOUND (checked ' + os.getcwd() + ' and ' + _script_dir + ')'}")

    # 2. Print diagnostics
    tradier_env = _env("TRADIER_ENV", "sandbox")
    token, acct, cred_source = _resolve_tradier_creds(tradier_env)
    print(f"[Cadence] Tradier env: {tradier_env}")
    print(f"[Cadence] Credentials source: {cred_source}")
    print(f"[Cadence] Token: {_mask(token)}")
    print(f"[Cadence] Account: {_mask(acct)}")

    # 3. Initialize components
    _trader = TradierClient(token, acct, env=tradier_env)
    print(f"[Cadence] Authenticated: {_trader.authenticated}")

    # Telegram
    _notifier = build_from_env()
    if _notifier.enabled:
        print("[Cadence] Telegram: enabled")
    else:
        print("[Cadence] Telegram: disabled (no credentials)")

    # Risk manager
    risk_config = RiskConfig(
        max_drawdown_pct=_env_float("CADENCE_MAX_DRAWDOWN_PCT", 10.0),
        drawdown_reference=_env("CADENCE_DRAWDOWN_REFERENCE", "session_start"),
        max_risk_per_position_pct=_env_float("CADENCE_MAX_PER_POSITION_PCT", 2.0),
        max_position_count=_env_int("CADENCE_MAX_CONCURRENT_POSITIONS", 5),
        max_portfolio_delta_cents=_env_int("CADENCE_MAX_PORTFOLIO_DELTA", 5000) * 100,
        max_portfolio_vega_cents=_env_int("CADENCE_MAX_PORTFOLIO_VEGA", 500) * 100,
        min_iv_rank=_env_float("CADENCE_MIN_IV_RANK", 30.0),
        min_credit_pct_of_width=_env_float("CADENCE_MIN_CREDIT_PCT", 20.0),
        use_kelly=_env_bool("CADENCE_USE_KELLY", False),
        kelly_fraction_of_full=_env_float("CADENCE_KELLY_FRACTION", 0.25),
    )
    state_file = _env("CADENCE_STATE_FILE", "")
    _risk_mgr = RiskManager(risk_config, starting_equity_cents=0,
                             state_file=state_file or None, notifier=_notifier)

    # Strategy config
    symbols = [s.strip() for s in _env("CADENCE_SYMBOLS", "SPY,QQQ").split(",") if s.strip()]
    strategy_config = StrategyConfig(
        target_dte=_env_int("CADENCE_TARGET_DTE", 45),
        target_delta=_env_int("CADENCE_TARGET_DELTA", 16),
        wing_width=_env_int("CADENCE_WING_WIDTH", 10),
        min_iv_rank=_env_float("CADENCE_MIN_IV_RANK", 30.0),
        min_credit_pct_of_width=_env_float("CADENCE_MIN_CREDIT_PCT", 20.0),
        symbols=symbols,
    )

    # Position manager (exit detection) and tracker (entry/close bookkeeping)
    _position_mgr = PositionManager(
        profit_target_pct=_env_float("CADENCE_PROFIT_TARGET_PCT", 50),
        time_stop_dte=_env_int("CADENCE_TIME_STOP_DTE", 21),
        loss_stop_multiplier=_env_float("CADENCE_LOSS_STOP_MULTIPLIER", 2.0),
    )
    tracker_state_file = _env("CADENCE_TRACKER_STATE_FILE", "position_tracker.json")
    _position_tracker = PositionTracker(state_file=tracker_state_file or None)

    # Trade ledger: append-only JSONL of all closed trades with full
    # entry+exit context for strategy analysis.
    ledger_file = _env("CADENCE_TRADE_LEDGER_FILE", "trade_ledger.jsonl")
    _trade_ledger = TradeLedger(path=ledger_file or None)

    # Process controller. Persist its state (currently just dry_run)
    # so toggling to PAPER or LIVE survives a restart instead of
    # silently reverting to dry_run -- which would cause Close button
    # clicks to log [DRY RUN] and never actually close at the broker.
    status_interval = _env_int("CADENCE_TELEGRAM_STATUS_INTERVAL", 3600)
    executor_state_file = _env("CADENCE_EXECUTOR_STATE_FILE",
                                "executor_state.json")
    _process_ctrl = ProcessController(
        trader=_trader,
        risk_mgr=_risk_mgr,
        strategy_config=strategy_config,
        notifier=_notifier if _notifier.enabled else None,
        dry_run=True,  # default; overridden by state file if present
        status_interval_secs=status_interval,
        position_manager=_position_mgr,
        position_tracker=_position_tracker,
        state_file=executor_state_file or None,
        trade_ledger=_trade_ledger,
    )
    print(f"[Cadence] Executor mode: "
          f"{'DRY RUN' if _process_ctrl.dry_run else 'LIVE/PAPER'}")

    # 4. Sync initial balance
    if _trader.authenticated:
        try:
            balances = _trader.get_account_balances()
            bal = balances.get("balances", {})
            equity = int(float(bal.get("total_equity", 0)) * 100)
            cash = int(float(bal.get("total_cash",
                                     bal.get("cash", {}).get("cash_available", 0))) * 100)
            _risk_mgr.sync_actual_balance(cash, portfolio_value_cents=equity)
            print(f"[Cadence] Initial equity: ${equity/100:.2f}")
        except Exception as e:
            print(f"[Cadence] Balance sync failed: {e}")

    # 5. Register Telegram commands
    if _notifier.enabled:
        _notifier.register_command("help", _cmd_help, "Show help")
        _notifier.register_command("status", _cmd_status, "Risk status")
        _notifier.register_command("positions", _cmd_positions, "Open positions")
        _notifier.register_command("decisions", _cmd_decisions, "Recent decisions")
        _notifier.register_command("config", _cmd_config, "Configuration")
        _notifier.register_command("kill", _cmd_kill, "Activate kill switch")
        _notifier.register_command("resume", _cmd_resume, "Deactivate kill switch")
        _notifier.register_command("reset", _cmd_reset, "Reset daily metrics")
        _notifier.register_command("scanner_start", _cmd_scanner_start, "Start scanner")
        _notifier.register_command("scanner_stop", _cmd_scanner_stop, "Stop scanner")
        _notifier.register_command("exec_start", _cmd_exec_start, "Start executor")
        _notifier.register_command("exec_stop", _cmd_exec_stop, "Stop executor")
        _notifier.register_command("exec_live", _cmd_exec_live, "Enable live trading")
        _notifier.register_confirmation("exec_live")

        if _notifier.commands_enabled:
            _notifier.start_command_listener()

        _notifier.notify_startup(detail=f"env={tradier_env}, auth={_trader.authenticated}")

    # 6. Start the broker sync loop unconditionally so dashboard stats
    # stay fresh even outside market hours and when the scanner is
    # stopped. Independent of the scanner cycle.
    if _trader.authenticated:
        sync_interval = _env_int("CADENCE_BROKER_SYNC_INTERVAL", 30)
        _process_ctrl.start_broker_sync(interval=sync_interval)
        print(f"[Cadence] Broker sync running every {sync_interval}s")

    # Auto-start scanner
    if _trader.authenticated and _env_bool("CADENCE_AUTOSTART_SCANNER", True):
        _process_ctrl.start_scanner()
        print("[Cadence] Scanner auto-started")

    # 7. Start HTTP server
    # Default to 127.0.0.1 so the dashboard is not exposed to the LAN.
    # Set CADENCE_BIND_ADDR=0.0.0.0 only if you intentionally want
    # other devices to reach it -- and only after setting a strong
    # CADENCE_DASHBOARD_TOKEN.
    port = _env_int("CADENCE_PORT", 8050)
    bind_addr = _env("CADENCE_BIND_ADDR", "127.0.0.1")
    dash_token = _env("CADENCE_DASHBOARD_TOKEN")
    if bind_addr != "127.0.0.1" and not dash_token:
        print("[Cadence] WARNING: bound to a non-loopback address "
              f"({bind_addr}) without CADENCE_DASHBOARD_TOKEN set. "
              "Anyone on the network can control the bot.")
    server = HTTPServer((bind_addr, port), DashboardHandler)
    print(f"[Cadence] Dashboard: http://{bind_addr}:{port}")
    if dash_token:
        print("[Cadence] Auth token: required (CADENCE_DASHBOARD_TOKEN)")
    print("[Cadence] Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Cadence] Shutting down...")
        _process_ctrl.stop_scanner()
        _process_ctrl.stop_executor()
        _process_ctrl.stop_broker_sync()
        if _notifier and _notifier.enabled:
            _notifier.notify_shutdown()
            _notifier.stop()
        server.shutdown()


if __name__ == "__main__":
    main()

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

        elif path == "/api/tracked-positions":
            # Our own view of open iron condors with unrealized P&L.
            # More useful than Tradier's per-leg /api/positions view.
            from cadence.executor import compute_close_debit
            tracked_info = []
            if _position_tracker and _trader and _trader.authenticated:
                for t in _position_tracker.get_open():
                    debit = None
                    pnl_dollars = None
                    pnl_pct = None
                    try:
                        debit, _ = compute_close_debit(_trader, t)
                    except Exception:
                        debit = None
                    if debit is not None and t.entry_credit > 0:
                        pnl_dollars = (t.entry_credit - debit) * 100 * t.contracts
                        pnl_pct = ((t.entry_credit - debit)
                                   / t.entry_credit) * 100
                    tracked_info.append({
                        "tag": t.tag,
                        "symbol": t.symbol,
                        "expiration": t.expiration,
                        "dte": t.current_dte(),
                        "contracts": t.contracts,
                        "entry_credit": t.entry_credit,
                        "current_debit": debit,
                        "pnl_dollars": pnl_dollars,
                        "pnl_pct": pnl_pct,
                        "short_put_strike": t.short_put_strike,
                        "long_put_strike": t.long_put_strike,
                        "short_call_strike": t.short_call_strike,
                        "long_call_strike": t.long_call_strike,
                        "entry_time": t.entry_time,
                    })
            self._send_json({"tracked": tracked_info})

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
            )
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


# -- Main --------------------------------------------------------------------

def main():
    global _trader, _risk_mgr, _process_ctrl, _notifier, _position_mgr
    global _position_tracker, _env_path, _script_dir

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

    # Process controller
    status_interval = _env_int("CADENCE_TELEGRAM_STATUS_INTERVAL", 3600)
    _process_ctrl = ProcessController(
        trader=_trader,
        risk_mgr=_risk_mgr,
        strategy_config=strategy_config,
        notifier=_notifier if _notifier.enabled else None,
        dry_run=True,
        status_interval_secs=status_interval,
        position_manager=_position_mgr,
        position_tracker=_position_tracker,
    )

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

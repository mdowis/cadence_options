"""Main entry point: .env loader, HTTP server, and glue between all modules."""

import json
import logging
import os
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from tradier_client import TradierClient
from risk_manager import RiskManager, RiskConfig
from strategy import StrategyConfig
from process_controller import ProcessController
from position_manager import PositionManager
from notifier import TelegramNotifier, build_from_env
from iv_rank import compute_iv_rank, get_cached_iv_rank

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
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, path):
        try:
            with open(path, "r") as f:
                body = f.read().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)
        except FileNotFoundError:
            self._send_json({"error": "dashboard.html not found"}, 404)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            return self.rfile.read(length)
        return b""

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"

        if path == "/":
            html_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "dashboard.html"
            )
            self._send_html(html_path)

        elif path == "/api/scan":
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
                for symbol in _process_ctrl.strategy_config.symbols:
                    data[symbol] = {"rank": 0, "percentile": 0}
            self._send_json(data)

        elif path == "/api/diagnostics":
            token = _env("TRADIER_ACCESS_TOKEN")
            acct = _env("TRADIER_ACCOUNT_ID")
            self._send_json({
                "env_path": _env_path or "NOT FOUND",
                "tradier_env": _env("TRADIER_ENV", "sandbox"),
                "token": _mask(token),
                "account_id": _mask(acct),
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

        elif path == "/api/risk/kill-switch/activate":
            if _risk_mgr:
                _risk_mgr.activate_kill_switch("Manual activation via dashboard")
            self._send_json({"status": "activated"})

        elif path == "/api/risk/kill-switch/deactivate":
            if _risk_mgr:
                _risk_mgr.deactivate_kill_switch()
            self._send_json({"status": "deactivated"})

        elif path == "/api/risk/reset-daily":
            if _risk_mgr:
                _risk_mgr.reset_daily()
            self._send_json({"status": "reset"})

        elif path.startswith("/api/positions/") and path.endswith("/close"):
            pos_id = path.split("/")[-2]
            self._send_json({"status": "close requested", "position_id": pos_id})

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
    if _process_ctrl:
        _process_ctrl.set_dry_run(False)
    return "LIVE TRADING ENABLED. Executor will place real orders."


# -- Main --------------------------------------------------------------------

def main():
    global _trader, _risk_mgr, _process_ctrl, _notifier, _position_mgr
    global _env_path, _script_dir

    _script_dir = os.path.dirname(os.path.abspath(__file__))

    # 1. Load .env
    _env_path = load_dotenv()
    print(f"[Cadence] .env: {_env_path or 'NOT FOUND (checked ' + os.getcwd() + ' and ' + _script_dir + ')'}")

    # 2. Print diagnostics
    token = _env("TRADIER_ACCESS_TOKEN")
    acct = _env("TRADIER_ACCOUNT_ID")
    tradier_env = _env("TRADIER_ENV", "sandbox")
    print(f"[Cadence] Tradier env: {tradier_env}")
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

    # Position manager
    _position_mgr = PositionManager(
        profit_target_pct=_env_float("CADENCE_PROFIT_TARGET_PCT", 50),
        time_stop_dte=_env_int("CADENCE_TIME_STOP_DTE", 21),
        loss_stop_multiplier=_env_float("CADENCE_LOSS_STOP_MULTIPLIER", 2.0),
    )

    # Process controller
    status_interval = _env_int("CADENCE_TELEGRAM_STATUS_INTERVAL", 3600)
    _process_ctrl = ProcessController(
        trader=_trader,
        risk_mgr=_risk_mgr,
        strategy_config=strategy_config,
        notifier=_notifier if _notifier.enabled else None,
        dry_run=True,
        status_interval_secs=status_interval,
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

        _notifier.notify_startup(f"env={tradier_env}, auth={_trader.authenticated}")

    # 6. Auto-start scanner
    if _trader.authenticated and _env_bool("CADENCE_AUTOSTART_SCANNER", True):
        _process_ctrl.start_scanner()
        print("[Cadence] Scanner auto-started")

    # 7. Start HTTP server
    port = _env_int("CADENCE_PORT", 8050)
    server = HTTPServer(("0.0.0.0", port), DashboardHandler)
    print(f"[Cadence] Dashboard: http://localhost:{port}")
    print("[Cadence] Press Ctrl+C to stop")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[Cadence] Shutting down...")
        _process_ctrl.stop_scanner()
        _process_ctrl.stop_executor()
        if _notifier and _notifier.enabled:
            _notifier.notify_shutdown()
            _notifier.stop()
        server.shutdown()


if __name__ == "__main__":
    main()

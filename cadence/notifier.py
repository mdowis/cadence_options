"""Telegram notification and remote command module for cadence_options.

Two background daemon threads:
1. Send worker: drains an outbound queue, POSTs to Telegram. Retries on 5xx,
   fails fast on 4xx. Never blocks callers.
2. Command listener (opt-in): long-polls getUpdates, dispatches /commands to
   registered handlers. Only runs when commands_enabled=True.

Security: chat-ID allowlist, opt-in commands, confirmation for dangerous ops,
boot-time update skip to prevent command replay.

Zero external dependencies -- stdlib only (urllib.request, json, threading, queue).
"""

import json
import logging
import os
import queue
import threading
import time
import urllib.request
import urllib.parse
import urllib.error

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}/{method}"


def _escape_md(s):
    """Strip Markdown v1 special characters from user-provided strings."""
    if not s:
        return ""
    for ch in ("*", "_", "`", "["):
        s = s.replace(ch, "")
    return s


class TelegramNotifier:
    """Thread-safe Telegram bot for notifications and remote commands."""

    DEDUP_WINDOW_SECS = 60
    REQUEST_TIMEOUT_SECS = 10
    MAX_QUEUE = 200
    MAX_RETRIES = 3
    POLL_TIMEOUT_SECS = 25
    CONFIRMATION_TTL_SECS = 30

    def __init__(self, bot_token, chat_id, enabled=True, commands_enabled=False):
        self.bot_token = bot_token or ""
        self.chat_id = str(chat_id) if chat_id else ""
        self.commands_enabled = commands_enabled

        # enabled requires both token and chat_id, plus not explicitly disabled
        self._enabled = enabled and bool(self.bot_token) and bool(self.chat_id)

        self._queue = queue.Queue(maxsize=self.MAX_QUEUE)
        self._stop = threading.Event()

        self._commands = {}            # name -> (handler, description)
        self._confirmations = {}       # cmd_name -> {chat_id_str: expiry_timestamp}
        self._confirm_lock = threading.Lock()

        self._recent_messages = {}     # text_hash -> timestamp
        self._dedup_lock = threading.Lock()

        self._stats = {
            "sent": 0,
            "failed": 0,
            "dropped": 0,
            "deduped": 0,
            "received": 0,
            "queued": 0,
        }
        self._stats_lock = threading.Lock()

        self._last_update_id = 0
        self._sent_messages = []       # for testing introspection only
        self._sent_lock = threading.Lock()

        # Start the send worker
        self._sender_thread = threading.Thread(
            target=self._sender_loop, daemon=True, name="telegram-sender"
        )
        self._sender_thread.start()

    # ---- Properties ----

    @property
    def enabled(self):
        return self._enabled

    @property
    def allowed_chat_ids(self):
        """Chat-ID allowlist for backward compatibility."""
        if self.chat_id:
            return {self.chat_id}
        return set()

    # ---- Sending ----

    def send(self, text, parse_mode="Markdown"):
        """Enqueue a message. Non-blocking, thread-safe.

        Returns False if disabled, deduped, or queue full.
        """
        if not self._enabled:
            return False

        # Dedup within window
        msg_hash = hash(text)
        now = time.time()
        with self._dedup_lock:
            last = self._recent_messages.get(msg_hash)
            if last and (now - last) < self.DEDUP_WINDOW_SECS:
                with self._stats_lock:
                    self._stats["deduped"] += 1
                return False
            self._recent_messages[msg_hash] = now
            # Garbage-collect expired entries
            expired = [h for h, t in self._recent_messages.items()
                       if now - t > self.DEDUP_WINDOW_SECS]
            for h in expired:
                del self._recent_messages[h]

        try:
            self._queue.put_nowait((text, parse_mode))
            with self._stats_lock:
                self._stats["queued"] += 1
            return True
        except queue.Full:
            logger.warning("Telegram send queue full, dropping message")
            with self._stats_lock:
                self._stats["dropped"] += 1
            return False

    def stop(self):
        """Signal workers to drain and exit."""
        self._stop.set()

    # ---- Event helpers ----

    def notify_startup(self, equity_cents=None, authenticated=None, detail=None):
        """Boot notification. Accepts flexible arguments for backward compat.

        If the first positional argument is a string (old call style like
        notify_startup("env=sandbox, auth=True")), treat it as detail.
        """
        # Back-compat: first positional arg as string means detail
        if isinstance(equity_cents, str):
            if detail is None:
                detail = equity_cents
            equity_cents = None

        parts = ["*Cadence Started*"]
        if detail:
            parts.append(_escape_md(str(detail)))
        if equity_cents is not None:
            parts.append("Equity: `${:.2f}`".format(equity_cents / 100))
        if authenticated is not None:
            parts.append("Auth: `{}`".format("OK" if authenticated else "FAILED"))
        self.send("\n".join(parts))

    def notify_trade(self, opp_or_detail, success=None, detail=None, contracts=None):
        """Trade notification. Supports both old (string) and new (structured) forms."""
        if isinstance(opp_or_detail, str):
            # Old call style from dashboard.py / process_controller.py
            self.send("*Trade*\n" + _escape_md(opp_or_detail))
            return
        # New structured form
        opp = opp_or_detail
        lines = ["*Trade {}*".format("Executed" if success else "Blocked")]
        if hasattr(opp, "symbol"):
            lines.append("Event: `{} IC {}DTE`".format(
                _escape_md(opp.symbol),
                getattr(opp, "dte", "?"),
            ))
        if hasattr(opp, "short_put_strike") and hasattr(opp, "short_call_strike"):
            lines.append("Strikes: `{}P/{}P - {}C/{}C`".format(
                getattr(opp, "short_put_strike", "?"),
                getattr(opp, "long_put_strike", "?"),
                getattr(opp, "short_call_strike", "?"),
                getattr(opp, "long_call_strike", "?"),
            ))
        if hasattr(opp, "credit"):
            lines.append("Credit: `${:.0f}`".format(opp.credit * 100))
        if contracts:
            lines.append("Contracts: `{}`".format(contracts))
        if detail:
            lines.append("Detail: {}".format(_escape_md(str(detail))))
        self.send("\n".join(lines))

    def notify_kill_switch(self, reason, equity_cents=None):
        """Kill switch transition notification."""
        lines = ["*KILL SWITCH ACTIVATED*", "Reason: {}".format(_escape_md(str(reason)))]
        if equity_cents is not None:
            lines.append("Equity: `${:.2f}`".format(equity_cents / 100))
        self.send("\n".join(lines))

    def notify_partial_fill(self, opp, filled, total, unwound=False):
        """Partial fill warning."""
        lines = ["*Partial Fill Warning*"]
        if hasattr(opp, "symbol"):
            lines.append("Symbol: `{}`".format(_escape_md(opp.symbol)))
        lines.append("Filled: `{}/{}` legs".format(filled, total))
        if unwound:
            lines.append("Status: Unwound remaining legs")
        else:
            lines.append("Status: Manual intervention may be needed")
        self.send("\n".join(lines))

    def notify_status(self, status_dict_or_equity=None, daily_pnl_cents=None,
                      drawdown_pct=None, exposure_cents=None, trades_today=None,
                      open_positions=None):
        """Periodic status. Supports both dict and positional args."""
        if isinstance(status_dict_or_equity, dict):
            # Old call style from dashboard.py / process_controller.py
            d = status_dict_or_equity
            equity = d.get("equity", {})
            current = equity.get("current", 0)
            dd = d.get("drawdown", {})
            dd_pct = dd.get("current_pct", 0)
            positions = d.get("positions", {})
            pos_count = positions.get("count", 0)
            daily = d.get("daily", {})
            pnl = daily.get("pnl_cents", 0)
            lines = [
                "*Hourly Status*",
                "Equity: `${:.2f}`".format(current / 100),
                "Daily P&L: `${:.2f}`".format(pnl / 100),
                "Drawdown: `{:.1f}%`".format(dd_pct),
                "Positions: `{}`".format(pos_count),
            ]
            self.send("\n".join(lines))
            return
        # New positional args form
        equity_cents = status_dict_or_equity or 0
        lines = [
            "*Hourly Status*",
            "Equity: `${:.2f}`".format(equity_cents / 100),
        ]
        if daily_pnl_cents is not None:
            lines.append("Daily P&L: `${:.2f}`".format(daily_pnl_cents / 100))
        if drawdown_pct is not None:
            lines.append("Drawdown: `{:.1f}%`".format(drawdown_pct))
        if exposure_cents is not None:
            lines.append("Exposure: `${:.2f}`".format(exposure_cents / 100))
        if trades_today is not None:
            lines.append("Trades today: `{}`".format(trades_today))
        if open_positions is not None:
            lines.append("Open positions: `{}`".format(open_positions))
        self.send("\n".join(lines))

    def notify_scanner_error(self, error_message):
        """Scanner exception (deduped via normal send dedup)."""
        self.send("*Scanner Error*\n{}".format(_escape_md(str(error_message))))

    def notify_shutdown(self):
        """Clean exit notification."""
        self.send("*Cadence Stopped*\nBot shutting down.")

    # ---- Command handling ----

    def register_command(self, name, handler, description=""):
        """Register a command handler. name without leading /.

        handler signature: fn(*args) -> str. Return value sent as reply.
        """
        self._commands[name] = (handler, description)

    def registered_commands(self):
        """Return {name: description} dict of registered commands."""
        return {name: desc for name, (_, desc) in self._commands.items()}

    def register_confirmation(self, name, args=None):
        """Mark a command as requiring two-step confirmation.

        On first invocation the handler gets called WITHOUT __confirmed__.
        After user replies CONFIRM, handler gets re-called WITH __confirmed__
        appended to args.

        Returns prompt text for the user.
        """
        with self._confirm_lock:
            if name not in self._confirmations:
                self._confirmations[name] = {}
        return "/{} requires confirmation. Reply CONFIRM within {}s.".format(
            name, self.CONFIRMATION_TTL_SECS)

    def start_command_listener(self):
        """Start the long-poll thread. Call AFTER registering commands."""
        if not self._enabled or not self.commands_enabled:
            return
        # Boot-time skip: advance past queued updates
        self._skip_pending_updates()
        self._listener_thread = threading.Thread(
            target=self._listener_loop, daemon=True, name="telegram-listener"
        )
        self._listener_thread.start()
        logger.info("Telegram command listener started")

    # ---- Diagnostics ----

    def get_stats(self):
        """Return diagnostic stats dict."""
        with self._stats_lock:
            stats = dict(self._stats)
        stats["enabled"] = self._enabled
        stats["commands_enabled"] = self.commands_enabled
        stats["commands_registered"] = len(self._commands)
        stats["chat_id"] = self.chat_id[:4] + "..." if len(self.chat_id) > 4 else self.chat_id
        return stats

    # ---- Internal: send worker ----

    def _sender_loop(self):
        """Drain queue and deliver messages."""
        while not self._stop.is_set():
            try:
                text, parse_mode = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue
            self._send_now(text, parse_mode)

    def _send_now(self, text, parse_mode="Markdown"):
        """POST to Telegram sendMessage. Retry on 5xx, fail fast on 4xx."""
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                url = TELEGRAM_API_BASE.format(token=self.bot_token, method="sendMessage")
                body = urllib.parse.urlencode({
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                }).encode("utf-8")
                req = urllib.request.Request(url, data=body)
                with urllib.request.urlopen(req, timeout=self.REQUEST_TIMEOUT_SECS) as resp:
                    resp.read()
                with self._stats_lock:
                    self._stats["sent"] += 1
                with self._sent_lock:
                    self._sent_messages.append(text)
                return True
            except urllib.error.HTTPError as e:
                if 400 <= e.code < 500:
                    # 4xx: client error, don't retry
                    logger.error("Telegram 4xx (%d), not retrying", e.code)
                    with self._stats_lock:
                        self._stats["failed"] += 1
                    return False
                # 5xx: server error, retry with backoff
                if attempt < self.MAX_RETRIES:
                    delay = 1.0 * (2 ** attempt)
                    logger.warning("Telegram 5xx (%d), retry %d/%d in %.1fs",
                                   e.code, attempt + 1, self.MAX_RETRIES, delay)
                    time.sleep(delay)
                    continue
                logger.error("Telegram send failed after %d retries", self.MAX_RETRIES)
                with self._stats_lock:
                    self._stats["failed"] += 1
                return False
            except Exception as e:
                logger.error("Telegram send error: %s", e)
                with self._stats_lock:
                    self._stats["failed"] += 1
                return False
        return False

    # ---- Internal: command listener ----

    def _skip_pending_updates(self):
        """Advance past queued updates on startup to prevent replay."""
        try:
            url = TELEGRAM_API_BASE.format(token=self.bot_token, method="getUpdates")
            params = urllib.parse.urlencode({"offset": -1, "timeout": 0})
            req = urllib.request.Request("{}?{}".format(url, params))
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            results = data.get("result", [])
            if results:
                self._last_update_id = results[-1].get("update_id", 0)
                logger.info("Skipped %d pending updates (last_id=%d)",
                            len(results), self._last_update_id)
        except Exception as e:
            logger.warning("Failed to skip pending updates: %s", e)

    def _listener_loop(self):
        """Long-poll getUpdates and dispatch commands."""
        while not self._stop.is_set():
            try:
                updates = self._get_updates()
                for update in updates:
                    self._handle_update(update)
            except Exception as e:
                logger.error("Telegram listener error: %s", e)
                # Brief pause before retrying to avoid tight error loops
                self._stop.wait(2.0)

    def _get_updates(self):
        url = TELEGRAM_API_BASE.format(token=self.bot_token, method="getUpdates")
        params = urllib.parse.urlencode({
            "offset": self._last_update_id + 1,
            "timeout": self.POLL_TIMEOUT_SECS,
        })
        try:
            req = urllib.request.Request("{}?{}".format(url, params))
            with urllib.request.urlopen(req, timeout=self.POLL_TIMEOUT_SECS + 5) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return data.get("result", [])
        except Exception as e:
            logger.error("Failed to get Telegram updates: %s", e)
            return []

    def _handle_update(self, update):
        update_id = update.get("update_id", 0)
        if update_id > self._last_update_id:
            self._last_update_id = update_id

        message = update.get("message", {})
        chat_id = str(message.get("chat", {}).get("id", ""))
        text = (message.get("text") or "").strip()

        if not text or not chat_id:
            return

        # Chat ID allowlist -- silent drop on mismatch
        if chat_id != self.chat_id:
            return

        with self._stats_lock:
            self._stats["received"] += 1

        # Check for CONFIRM / YES reply for pending confirmation
        upper = text.upper()
        if upper in ("CONFIRM", "YES"):
            self._handle_confirmation(chat_id)
            return

        # Must be a / command
        if not text.startswith("/"):
            return  # ignore plain text silently

        parts = text.split()
        cmd_raw = parts[0][1:]  # strip leading /
        # Strip @BotName suffix
        if "@" in cmd_raw:
            cmd_raw = cmd_raw.split("@")[0]
        args = parts[1:]

        if cmd_raw not in self._commands:
            self.send("Unknown command: /{}. Try /help".format(_escape_md(cmd_raw)))
            return

        handler, _ = self._commands[cmd_raw]

        # Check if this command requires confirmation
        with self._confirm_lock:
            if cmd_raw in self._confirmations:
                # First call: set pending confirmation, call handler without __confirmed__
                self._confirmations[cmd_raw][chat_id] = time.time() + self.CONFIRMATION_TTL_SECS
                try:
                    result = handler(*args)
                    if result:
                        self.send(str(result))
                except Exception as e:
                    self.send("Error: {}".format(_escape_md(str(e))))
                return

        # Normal command: dispatch
        try:
            result = handler(*args)
            if result:
                self.send(str(result))
        except Exception as e:
            self.send("Error: {}".format(_escape_md(str(e))))

    def _handle_confirmation(self, chat_id):
        """Handle CONFIRM / YES reply."""
        with self._confirm_lock:
            found_name = None
            for name, pending in self._confirmations.items():
                if chat_id in pending:
                    if time.time() <= pending[chat_id]:
                        found_name = name
                        del pending[chat_id]
                        break
                    else:
                        # Expired
                        del pending[chat_id]

            if not found_name:
                self.send("Nothing to confirm (expired or no pending action).")
                return

        handler, _ = self._commands.get(found_name, (None, None))
        if not handler:
            return

        try:
            result = handler("__confirmed__")
            self.send("Confirmed: {}".format(result or "done"))
        except Exception as e:
            self.send("Error: {}".format(_escape_md(str(e))))


def build_from_env():
    """Factory: create TelegramNotifier from CADENCE_TELEGRAM_* env vars.

    Returns a disabled notifier if credentials are missing (no exception).
    """
    bot_token = os.environ.get("CADENCE_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("CADENCE_TELEGRAM_CHAT_ID", "")

    # Enabled by default if credentials are present, unless explicitly disabled
    explicit_enabled = os.environ.get("CADENCE_TELEGRAM_ENABLED", "")
    if explicit_enabled:
        enabled = explicit_enabled.lower() in ("true", "1", "yes")
    else:
        enabled = bool(bot_token) and bool(chat_id)

    commands_enabled = os.environ.get(
        "CADENCE_TELEGRAM_COMMANDS_ENABLED", "false"
    ).lower() in ("true", "1", "yes")

    return TelegramNotifier(
        bot_token=bot_token,
        chat_id=chat_id,
        enabled=enabled,
        commands_enabled=commands_enabled,
    )

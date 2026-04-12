"""Telegram notifier with queue-based sending, command listener, and confirmations."""

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

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"
DEDUP_WINDOW = 60  # seconds
MAX_RETRIES = 2
RETRY_DELAY = 2.0


class TelegramNotifier:
    """Thread-safe Telegram bot for notifications and commands."""

    def __init__(self, bot_token, chat_id, allowed_chat_ids=None,
                 commands_enabled=False):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.allowed_chat_ids = set(allowed_chat_ids or [str(chat_id)])
        self.commands_enabled = commands_enabled

        self._queue = queue.Queue(maxsize=100)
        self._sender_thread = None
        self._listener_thread = None
        self._stop = threading.Event()

        self._commands = {}  # name -> (handler, description)
        self._confirmations = {}  # name -> {chat_id -> expiry_time}
        self._confirm_lock = threading.Lock()

        self._recent_messages = {}  # hash -> timestamp (dedup)
        self._dedup_lock = threading.Lock()

        self._stats = {
            "sent": 0,
            "failed": 0,
            "received": 0,
            "deduped": 0,
        }
        self._stats_lock = threading.Lock()

        self._last_update_id = 0

        # Start sender thread
        self._sender_thread = threading.Thread(
            target=self._sender_loop, daemon=True, name="telegram-sender"
        )
        self._sender_thread.start()

    @property
    def enabled(self):
        return bool(self.bot_token) and bool(self.chat_id)

    def send(self, text):
        """Thread-safe, non-blocking send. Deduplicates within 60s window."""
        if not self.enabled:
            return

        msg_hash = hash(text)
        now = time.time()
        with self._dedup_lock:
            last = self._recent_messages.get(msg_hash)
            if last and (now - last) < DEDUP_WINDOW:
                with self._stats_lock:
                    self._stats["deduped"] += 1
                return
            self._recent_messages[msg_hash] = now
            # Clean old entries
            expired = [h for h, t in self._recent_messages.items()
                       if now - t > DEDUP_WINDOW]
            for h in expired:
                del self._recent_messages[h]

        try:
            self._queue.put_nowait(text)
        except queue.Full:
            logger.warning("Telegram send queue full, dropping message")

    def notify_startup(self, detail=""):
        self.send(f"[Cadence] Bot started. {detail}".strip())

    def notify_trade(self, detail):
        self.send(f"[Cadence] Trade: {detail}")

    def notify_kill_switch(self, reason):
        self.send(f"[Cadence] KILL SWITCH ACTIVATED: {reason}")

    def notify_status(self, status_dict):
        equity = status_dict.get("equity", {})
        current = equity.get("current", 0) / 100
        dd = status_dict.get("drawdown", {})
        dd_pct = dd.get("current_pct", 0)
        positions = status_dict.get("positions", {})
        pos_count = positions.get("count", 0)
        self.send(
            f"[Cadence] Status: equity=${current:.2f}, "
            f"drawdown={dd_pct:.1f}%, positions={pos_count}"
        )

    def notify_scanner_error(self, error):
        self.send(f"[Cadence] Scanner error: {error}")

    def notify_shutdown(self):
        self.send("[Cadence] Bot shutting down.")

    # -- Commands ------------------------------------------------------------

    def register_command(self, name, handler, description=""):
        """Register a command handler. name should not include the leading /."""
        self._commands[name] = (handler, description)

    def register_confirmation(self, name):
        """Mark a command as requiring two-step confirmation."""
        with self._confirm_lock:
            self._confirmations[name] = {}

    def start_command_listener(self):
        """Start long-polling for Telegram updates."""
        if not self.enabled or not self.commands_enabled:
            return
        self._listener_thread = threading.Thread(
            target=self._listener_loop, daemon=True, name="telegram-listener"
        )
        self._listener_thread.start()
        logger.info("Telegram command listener started")

    def get_stats(self):
        with self._stats_lock:
            return dict(self._stats)

    def stop(self):
        self._stop.set()

    # -- Internal send loop --------------------------------------------------

    def _sender_loop(self):
        while not self._stop.is_set():
            try:
                text = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue
            self._send_message(text)

    def _send_message(self, text):
        for attempt in range(MAX_RETRIES + 1):
            try:
                url = TELEGRAM_API.format(token=self.bot_token, method="sendMessage")
                data = urllib.parse.urlencode({
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                }).encode("utf-8")
                req = urllib.request.Request(url, data=data)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    resp.read()
                with self._stats_lock:
                    self._stats["sent"] += 1
                return
            except urllib.error.HTTPError as e:
                if 400 <= e.code < 500:
                    logger.error("Telegram 4xx error (%d), not retrying", e.code)
                    with self._stats_lock:
                        self._stats["failed"] += 1
                    return
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY * (2 ** attempt))
                    continue
                logger.error("Telegram send failed after retries: %s", e)
                with self._stats_lock:
                    self._stats["failed"] += 1
            except Exception as e:
                logger.error("Telegram send error: %s", e)
                with self._stats_lock:
                    self._stats["failed"] += 1
                return

    # -- Internal listener loop ----------------------------------------------

    def _listener_loop(self):
        while not self._stop.is_set():
            try:
                updates = self._get_updates()
                for update in updates:
                    self._handle_update(update)
            except Exception as e:
                logger.error("Telegram listener error: %s", e)
            self._stop.wait(2.0)

    def _get_updates(self):
        url = TELEGRAM_API.format(token=self.bot_token, method="getUpdates")
        params = urllib.parse.urlencode({
            "offset": self._last_update_id + 1,
            "timeout": 10,
        })
        try:
            req = urllib.request.Request(f"{url}?{params}")
            with urllib.request.urlopen(req, timeout=15) as resp:
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

        # Chat ID allowlist
        if chat_id not in self.allowed_chat_ids:
            logger.warning("Telegram message from unauthorized chat: %s", chat_id)
            return

        with self._stats_lock:
            self._stats["received"] += 1

        if not text.startswith("/"):
            return

        parts = text.split()
        cmd_name = parts[0][1:]  # strip leading /
        args = parts[1:]

        # Check for confirmation reply
        with self._confirm_lock:
            for conf_name, pending in self._confirmations.items():
                if chat_id in pending:
                    if time.time() <= pending[chat_id]:
                        if text.upper() == "CONFIRM":
                            del pending[chat_id]
                            handler, _ = self._commands.get(conf_name, (None, None))
                            if handler:
                                try:
                                    result = handler(*args)
                                    self.send(f"Confirmed: {result or 'done'}")
                                except Exception as e:
                                    self.send(f"Error: {e}")
                            return
                        else:
                            del pending[chat_id]
                            self.send("Cancelled.")
                            return
                    else:
                        del pending[chat_id]

        if cmd_name not in self._commands:
            self.send(f"Unknown command: /{cmd_name}")
            return

        handler, _ = self._commands[cmd_name]

        # Check if this command requires confirmation
        with self._confirm_lock:
            if cmd_name in self._confirmations:
                self._confirmations[cmd_name][chat_id] = time.time() + 30
                self.send(f"/{cmd_name} requires confirmation. Reply CONFIRM within 30s.")
                return

        try:
            result = handler(*args)
            if result:
                self.send(str(result))
        except Exception as e:
            self.send(f"Error: {e}")


def build_from_env():
    """Factory: create TelegramNotifier from CADENCE_TELEGRAM_* env vars."""
    bot_token = os.environ.get("CADENCE_TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("CADENCE_TELEGRAM_CHAT_ID", "")
    commands_enabled = os.environ.get("CADENCE_TELEGRAM_COMMANDS_ENABLED", "").lower() == "true"

    allowed_ids = None
    if chat_id:
        allowed_ids = [str(chat_id)]

    notifier = TelegramNotifier(
        bot_token=bot_token,
        chat_id=chat_id,
        allowed_chat_ids=allowed_ids,
        commands_enabled=commands_enabled,
    )
    return notifier

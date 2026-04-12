"""Tests for notifier.py. All mock urllib -- zero real network calls."""

import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock, call
from io import BytesIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.notifier import TelegramNotifier, build_from_env, _escape_md


# ---- Test helpers ----

def _make_authorized_message(text, chat_id="12345", update_id=1):
    """Build a fake Telegram update dict."""
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": {"id": int(chat_id)},
            "text": text,
        },
    }


def _make_notifier(commands=False, token="test-token", chat_id="12345"):
    """Create a notifier with send worker silenced for unit tests."""
    n = TelegramNotifier(token, chat_id, enabled=True, commands_enabled=commands)
    # Replace _send_now with a list-appending stub so we can inspect
    # what would be sent without hitting the network or needing the queue
    n._test_sent = []
    original_send_now = n._send_now
    def stub_send_now(text, parse_mode="Markdown"):
        n._test_sent.append(text)
        with n._stats_lock:
            n._stats["sent"] += 1
    n._send_now = stub_send_now
    return n


# ============================================================================
# Disabled path
# ============================================================================

class TestDisabled(unittest.TestCase):

    def test_disabled_no_token(self):
        n = TelegramNotifier("", "12345")
        self.assertFalse(n.enabled)
        n.stop()

    def test_disabled_no_chat_id(self):
        n = TelegramNotifier("token", "")
        self.assertFalse(n.enabled)
        n.stop()

    def test_disabled_explicit_false(self):
        n = TelegramNotifier("token", "12345", enabled=False)
        self.assertFalse(n.enabled)
        n.stop()

    def test_send_returns_false_when_disabled(self):
        n = TelegramNotifier("", "")
        self.assertFalse(n.send("test"))
        n.stop()

    def test_build_from_env_disabled_no_creds(self):
        with patch.dict(os.environ, {}, clear=True):
            n = build_from_env()
            self.assertFalse(n.enabled)
            n.stop()


# ============================================================================
# Send path
# ============================================================================

class TestSend(unittest.TestCase):

    def test_send_enqueues(self):
        n = _make_notifier()
        result = n.send("hello")
        self.assertTrue(result)
        # Give sender thread a moment to drain the queue
        time.sleep(0.15)
        self.assertIn("hello", n._test_sent)
        n.stop()

    def test_dedup_within_window(self):
        n = _make_notifier()
        r1 = n.send("same msg")
        r2 = n.send("same msg")
        self.assertTrue(r1)
        self.assertFalse(r2)
        stats = n.get_stats()
        self.assertEqual(stats["deduped"], 1)
        n.stop()

    def test_different_messages_not_deduped(self):
        n = _make_notifier()
        r1 = n.send("msg a")
        r2 = n.send("msg b")
        self.assertTrue(r1)
        self.assertTrue(r2)
        n.stop()

    @patch("cadence.notifier.urllib.request.urlopen")
    def test_send_now_success(self, mock_urlopen):
        """Direct test of _send_now with real method (not stubbed)."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok":true}'
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        n = TelegramNotifier("tok", "123")
        result = n._send_now("test message")
        self.assertTrue(result)
        self.assertEqual(mock_urlopen.call_count, 1)
        # Verify request
        req = mock_urlopen.call_args[0][0]
        self.assertIn(b"chat_id=123", req.data)
        self.assertIn(b"text=test+message", req.data)
        n.stop()

    @patch("cadence.notifier.urllib.request.urlopen")
    def test_send_now_4xx_fails_fast(self, mock_urlopen):
        """4xx should fail immediately, no retries."""
        import urllib.error
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="http://test", code=400, msg="Bad", hdrs={},
            fp=BytesIO(b"bad request"))

        n = TelegramNotifier("tok", "123")
        result = n._send_now("test")
        self.assertFalse(result)
        # Only 1 call (no retries)
        self.assertEqual(mock_urlopen.call_count, 1)
        stats = n.get_stats()
        self.assertEqual(stats["failed"], 1)
        n.stop()

    @patch("cadence.notifier.time.sleep")
    @patch("cadence.notifier.urllib.request.urlopen")
    def test_send_now_5xx_retries(self, mock_urlopen, mock_sleep):
        """5xx should retry with backoff."""
        import urllib.error
        error_500 = urllib.error.HTTPError(
            url="http://test", code=500, msg="Server Error", hdrs={},
            fp=BytesIO(b"error"))

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok":true}'
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)

        # Fail twice then succeed
        mock_urlopen.side_effect = [error_500, error_500, mock_resp]

        n = TelegramNotifier("tok", "123")
        result = n._send_now("test")
        self.assertTrue(result)
        self.assertEqual(mock_urlopen.call_count, 3)
        # Verify exponential backoff
        self.assertEqual(mock_sleep.call_count, 2)
        n.stop()


# ============================================================================
# Event helpers
# ============================================================================

class TestEventHelpers(unittest.TestCase):

    def test_notify_trade_string_form(self):
        n = _make_notifier()
        n.notify_trade("SPY IC filled at $2.85")
        time.sleep(0.15)
        self.assertTrue(any("SPY IC filled" in m for m in n._test_sent))
        n.stop()

    def test_notify_kill_switch_contains_kill_and_reason(self):
        n = _make_notifier()
        n.notify_kill_switch("drawdown exceeded 10%")
        time.sleep(0.15)
        sent = n._test_sent
        self.assertTrue(any("KILL" in m and "drawdown" in m for m in sent))
        n.stop()

    def test_notify_kill_switch_with_equity(self):
        n = _make_notifier()
        n.notify_kill_switch("test reason", equity_cents=50000)
        time.sleep(0.15)
        sent = n._test_sent
        self.assertTrue(any("$500.00" in m for m in sent))
        n.stop()

    def test_notify_status_dict_form(self):
        n = _make_notifier()
        n.notify_status({
            "equity": {"current": 100000},
            "drawdown": {"current_pct": 3.5},
            "positions": {"count": 2},
            "daily": {"pnl_cents": -500},
        })
        time.sleep(0.15)
        sent = n._test_sent
        self.assertTrue(any("Status" in m for m in sent))
        n.stop()

    def test_notify_startup_string_detail(self):
        """Backward compat: dashboard.py calls notify_startup(str)."""
        n = _make_notifier()
        n.notify_startup(detail="env=sandbox, auth=True")
        time.sleep(0.15)
        self.assertTrue(len(n._test_sent) > 0)
        self.assertTrue(any("env=sandbox" in m for m in n._test_sent))
        n.stop()

    def test_notify_startup_positional_string(self):
        """Regression: first positional arg as string must be treated as
        detail (not equity_cents). Previously this raised TypeError when
        the string got divided by 100."""
        n = _make_notifier()
        n.notify_startup("env=sandbox, auth=True")  # no keyword
        time.sleep(0.15)
        self.assertTrue(any("env=sandbox" in m for m in n._test_sent))
        n.stop()

    def test_notify_startup_structured_form(self):
        """The new structured form still works."""
        n = _make_notifier()
        n.notify_startup(equity_cents=123456, authenticated=True)
        time.sleep(0.15)
        sent = n._test_sent
        self.assertTrue(any("$1234.56" in m for m in sent))
        self.assertTrue(any("OK" in m for m in sent))
        n.stop()

    def test_notify_scanner_error_deduped(self):
        n = _make_notifier()
        n.notify_scanner_error("Connection refused")
        n.notify_scanner_error("Connection refused")
        time.sleep(0.15)
        # Only one message sent (dedup)
        error_msgs = [m for m in n._test_sent if "Connection refused" in m]
        self.assertEqual(len(error_msgs), 1)
        n.stop()

    def test_notify_shutdown(self):
        n = _make_notifier()
        n.notify_shutdown()
        time.sleep(0.15)
        self.assertTrue(any("Stopped" in m or "shutting" in m for m in n._test_sent))
        n.stop()

    def test_all_notify_methods_dont_raise_when_disabled(self):
        n = TelegramNotifier("", "")
        n.notify_startup(detail="test")
        n.notify_trade("trade detail")
        n.notify_kill_switch("reason")
        n.notify_partial_fill(None, 2, 4)
        n.notify_status({"equity": {"current": 0}, "drawdown": {"current_pct": 0}, "positions": {"count": 0}, "daily": {"pnl_cents": 0}})
        n.notify_scanner_error("err")
        n.notify_shutdown()
        n.stop()


# ============================================================================
# _escape_md
# ============================================================================

class TestEscapeMd(unittest.TestCase):

    def test_strips_star(self):
        self.assertEqual(_escape_md("*bold*"), "bold")

    def test_strips_underscore(self):
        self.assertEqual(_escape_md("_italic_"), "italic")

    def test_strips_backtick(self):
        self.assertEqual(_escape_md("`code`"), "code")

    def test_strips_bracket(self):
        self.assertEqual(_escape_md("[link](url)"), "link](url)")

    def test_empty_string(self):
        self.assertEqual(_escape_md(""), "")

    def test_none(self):
        self.assertEqual(_escape_md(None), "")

    def test_no_special_chars(self):
        self.assertEqual(_escape_md("plain text 123"), "plain text 123")


# ============================================================================
# get_stats
# ============================================================================

class TestStats(unittest.TestCase):

    def test_initial_stats(self):
        n = _make_notifier()
        stats = n.get_stats()
        self.assertEqual(stats["sent"], 0)
        self.assertEqual(stats["failed"], 0)
        self.assertEqual(stats["dropped"], 0)
        self.assertEqual(stats["deduped"], 0)
        self.assertTrue(stats["enabled"])
        self.assertFalse(stats["commands_enabled"])
        self.assertEqual(stats["commands_registered"], 0)
        n.stop()

    def test_stats_after_send(self):
        n = _make_notifier()
        n.send("test")
        time.sleep(0.15)
        stats = n.get_stats()
        self.assertEqual(stats["sent"], 1)
        self.assertEqual(stats["queued"], 1)
        n.stop()

    def test_chat_id_truncated(self):
        n = _make_notifier(chat_id="1234567890")
        stats = n.get_stats()
        self.assertEqual(stats["chat_id"], "1234...")
        n.stop()


# ============================================================================
# Command path
# ============================================================================

class TestCommands(unittest.TestCase):

    def test_commands_disabled_by_default(self):
        n = _make_notifier(commands=False)
        self.assertFalse(n.commands_enabled)
        n.stop()

    def test_register_command_appears_in_registered(self):
        n = _make_notifier(commands=True)
        n.register_command("status", lambda: "ok", "Show status")
        cmds = n.registered_commands()
        self.assertIn("status", cmds)
        self.assertEqual(cmds["status"], "Show status")
        n.stop()

    def test_status_command_dispatches(self):
        n = _make_notifier(commands=True)
        handler = MagicMock(return_value="Equity: $1000")
        n.register_command("status", handler, "Show status")

        update = _make_authorized_message("/status")
        n._handle_update(update)

        handler.assert_called_once()
        time.sleep(0.15)
        self.assertTrue(any("Equity" in m for m in n._test_sent))
        n.stop()

    def test_echo_passes_args(self):
        n = _make_notifier(commands=True)
        handler = MagicMock(return_value="echoed")
        n.register_command("echo", handler, "Echo args")

        update = _make_authorized_message("/echo hello world")
        n._handle_update(update)

        handler.assert_called_once_with("hello", "world")
        n.stop()

    def test_command_with_bot_suffix(self):
        """'/cmd@BotName' should dispatch to 'cmd'."""
        n = _make_notifier(commands=True)
        handler = MagicMock(return_value="ok")
        n.register_command("cmd", handler, "Test")

        update = _make_authorized_message("/cmd@MyBot")
        n._handle_update(update)

        handler.assert_called_once()
        n.stop()

    def test_wrong_chat_id_silent_drop(self):
        """Message from wrong chat_id should never fire handler."""
        n = _make_notifier(commands=True, chat_id="12345")
        handler = MagicMock(return_value="ok")
        n.register_command("status", handler)

        update = _make_authorized_message("/status", chat_id="99999")
        n._handle_update(update)

        handler.assert_not_called()
        # No error reply either
        time.sleep(0.15)
        self.assertEqual(len(n._test_sent), 0)
        n.stop()

    def test_unknown_command_replies_help_hint(self):
        n = _make_notifier(commands=True)

        update = _make_authorized_message("/nonexistent")
        n._handle_update(update)

        time.sleep(0.15)
        self.assertTrue(any("Unknown command" in m and "/help" in m
                            for m in n._test_sent))
        n.stop()

    def test_plain_text_ignored(self):
        """Non-command, non-CONFIRM text should be silently ignored."""
        n = _make_notifier(commands=True)
        handler = MagicMock()
        n.register_command("test", handler)

        update = _make_authorized_message("just a regular message")
        n._handle_update(update)

        handler.assert_not_called()
        time.sleep(0.15)
        self.assertEqual(len(n._test_sent), 0)
        n.stop()

    def test_handler_exception_caught_and_reported(self):
        n = _make_notifier(commands=True)
        def bad_handler():
            raise ValueError("something broke")
        n.register_command("broken", bad_handler, "Broken cmd")

        update = _make_authorized_message("/broken")
        n._handle_update(update)

        time.sleep(0.15)
        self.assertTrue(any("Error" in m and "something broke" in m
                            for m in n._test_sent))
        n.stop()

    def test_received_stat_incremented(self):
        n = _make_notifier(commands=True)
        n.register_command("test", lambda: "ok")

        update = _make_authorized_message("/test")
        n._handle_update(update)

        stats = n.get_stats()
        self.assertEqual(stats["received"], 1)
        n.stop()


# ============================================================================
# Confirmation flow
# ============================================================================

class TestConfirmation(unittest.TestCase):

    def test_confirmation_flow_full(self):
        """exec_live -> handler called without confirmed -> CONFIRM -> handler with confirmed."""
        n = _make_notifier(commands=True)
        results = []
        def exec_live_handler(*args):
            if "__confirmed__" in args:
                results.append("executed_live")
                return "LIVE TRADING ENABLED"
            return "Reply CONFIRM within 30s to enable live trading."

        n.register_command("exec_live", exec_live_handler, "Go live")
        n.register_confirmation("exec_live")

        # Step 1: user sends /exec_live
        n._handle_update(_make_authorized_message("/exec_live", update_id=1))
        time.sleep(0.15)
        # Handler was called, should have sent the prompt
        self.assertTrue(any("CONFIRM" in m or "Reply" in m for m in n._test_sent),
                        "Should send confirmation prompt")
        self.assertNotIn("executed_live", results)

        # Step 2: user replies CONFIRM
        n._handle_update(_make_authorized_message("CONFIRM", update_id=2))
        time.sleep(0.15)
        self.assertIn("executed_live", results)
        self.assertTrue(any("LIVE TRADING" in m for m in n._test_sent))
        n.stop()

    def test_expired_confirmation(self):
        """Expired confirmation should produce 'nothing to confirm'."""
        n = _make_notifier(commands=True)
        handler = MagicMock(return_value="prompt")
        n.register_command("exec_live", handler)
        n.register_confirmation("exec_live")

        # Step 1: send command to register pending confirmation
        n._handle_update(_make_authorized_message("/exec_live", update_id=1))

        # Step 2: expire the confirmation manually
        with n._confirm_lock:
            for name, pending in n._confirmations.items():
                for cid in pending:
                    pending[cid] = time.time() - 1  # expired

        # Step 3: send CONFIRM
        n._handle_update(_make_authorized_message("CONFIRM", update_id=2))
        time.sleep(0.15)
        self.assertTrue(any("Nothing to confirm" in m or "expired" in m
                            for m in n._test_sent))
        n.stop()

    def test_yes_also_confirms(self):
        """YES (case-insensitive) should also confirm."""
        n = _make_notifier(commands=True)
        results = []
        def handler(*args):
            if "__confirmed__" in args:
                results.append("done")
                return "confirmed"
            return "prompt"
        n.register_command("dangerous", handler)
        n.register_confirmation("dangerous")

        n._handle_update(_make_authorized_message("/dangerous", update_id=1))
        n._handle_update(_make_authorized_message("yes", update_id=2))
        time.sleep(0.15)
        self.assertIn("done", results)
        n.stop()


# ============================================================================
# build_from_env
# ============================================================================

class TestBuildFromEnv(unittest.TestCase):

    @patch.dict(os.environ, {
        "CADENCE_TELEGRAM_BOT_TOKEN": "test-token",
        "CADENCE_TELEGRAM_CHAT_ID": "12345",
        "CADENCE_TELEGRAM_COMMANDS_ENABLED": "true",
    })
    def test_builds_enabled(self):
        n = build_from_env()
        self.assertTrue(n.enabled)
        self.assertTrue(n.commands_enabled)
        self.assertIn("12345", n.allowed_chat_ids)
        n.stop()

    @patch.dict(os.environ, {}, clear=True)
    def test_builds_disabled_no_creds(self):
        n = build_from_env()
        self.assertFalse(n.enabled)
        n.stop()

    @patch.dict(os.environ, {
        "CADENCE_TELEGRAM_BOT_TOKEN": "tok",
        "CADENCE_TELEGRAM_CHAT_ID": "123",
        "CADENCE_TELEGRAM_ENABLED": "false",
    })
    def test_explicit_disabled(self):
        n = build_from_env()
        self.assertFalse(n.enabled)
        n.stop()

    @patch.dict(os.environ, {
        "CADENCE_TELEGRAM_BOT_TOKEN": "tok",
        "CADENCE_TELEGRAM_CHAT_ID": "123",
        "CADENCE_TELEGRAM_COMMANDS_ENABLED": "false",
    })
    def test_commands_disabled_by_default(self):
        n = build_from_env()
        self.assertTrue(n.enabled)
        self.assertFalse(n.commands_enabled)
        n.stop()


# ============================================================================
# Boot-time skip
# ============================================================================

class TestBootSkip(unittest.TestCase):

    @patch("cadence.notifier.urllib.request.urlopen")
    def test_skip_pending_advances_offset(self, mock_urlopen):
        fake_resp = MagicMock()
        fake_resp.read.return_value = b'{"ok":true,"result":[{"update_id":42}]}'
        fake_resp.__enter__ = MagicMock(return_value=fake_resp)
        fake_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = fake_resp

        n = TelegramNotifier("tok", "123", commands_enabled=True)
        n._skip_pending_updates()
        self.assertEqual(n._last_update_id, 42)
        n.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)

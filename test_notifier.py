"""Tests for notifier.py."""

import os
import sys
import time
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(__file__))
from notifier import TelegramNotifier, build_from_env


class TestTelegramNotifier(unittest.TestCase):

    def test_not_enabled_without_credentials(self):
        n = TelegramNotifier("", "")
        self.assertFalse(n.enabled)
        n.stop()

    def test_enabled_with_credentials(self):
        n = TelegramNotifier("tok", "123")
        self.assertTrue(n.enabled)
        n.stop()

    def test_send_disabled_noop(self):
        n = TelegramNotifier("", "")
        n.send("test")  # should not raise
        n.stop()

    def test_dedup_within_window(self):
        n = TelegramNotifier("tok", "123")
        n.send("same message")
        n.send("same message")
        stats = n.get_stats()
        self.assertEqual(stats["deduped"], 1)
        n.stop()

    def test_register_command(self):
        n = TelegramNotifier("tok", "123")
        handler = MagicMock(return_value="ok")
        n.register_command("test", handler, "test command")
        self.assertIn("test", n._commands)
        n.stop()

    def test_register_confirmation(self):
        n = TelegramNotifier("tok", "123")
        n.register_confirmation("dangerous")
        self.assertIn("dangerous", n._confirmations)
        n.stop()

    def test_notify_methods_dont_raise(self):
        n = TelegramNotifier("", "")  # disabled, should be noop
        n.notify_startup("test")
        n.notify_trade("test trade")
        n.notify_kill_switch("reason")
        n.notify_status({"equity": {"current": 50000}, "drawdown": {"current_pct": 2.0}, "positions": {"count": 1}})
        n.notify_scanner_error("err")
        n.notify_shutdown()
        n.stop()

    def test_stats(self):
        n = TelegramNotifier("tok", "123")
        stats = n.get_stats()
        self.assertEqual(stats["sent"], 0)
        self.assertEqual(stats["failed"], 0)
        n.stop()


class TestBuildFromEnv(unittest.TestCase):

    @patch.dict(os.environ, {
        "CADENCE_TELEGRAM_BOT_TOKEN": "test-token",
        "CADENCE_TELEGRAM_CHAT_ID": "12345",
        "CADENCE_TELEGRAM_COMMANDS_ENABLED": "true",
    })
    def test_builds_with_env(self):
        n = build_from_env()
        self.assertTrue(n.enabled)
        self.assertTrue(n.commands_enabled)
        self.assertIn("12345", n.allowed_chat_ids)
        n.stop()

    @patch.dict(os.environ, {}, clear=True)
    def test_builds_disabled_without_env(self):
        n = build_from_env()
        self.assertFalse(n.enabled)
        n.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)

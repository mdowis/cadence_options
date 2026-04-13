"""Tests for dashboard.py, including regression test 7 (.env loader)."""

import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dashboard import load_dotenv, _load_env_file, _mask, _resolve_tradier_creds


class TestLoadDotenv(unittest.TestCase):
    """Regression test 7: .env loader must check script directory
    if cwd doesn't have it."""

    def test_finds_env_in_first_dir(self):
        with tempfile.TemporaryDirectory() as d:
            env_path = os.path.join(d, ".env")
            with open(env_path, "w") as f:
                f.write("TEST_VAR=hello\n")
            result = load_dotenv(search_dirs=[d])
            self.assertEqual(result, env_path)

    def test_finds_env_in_second_dir_when_first_missing(self):
        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            # d1 has no .env, d2 has it
            env_path = os.path.join(d2, ".env")
            with open(env_path, "w") as f:
                f.write("TEST_VAR2=world\n")
            result = load_dotenv(search_dirs=[d1, d2])
            self.assertEqual(result, env_path)

    def test_returns_none_when_not_found(self):
        with tempfile.TemporaryDirectory() as d:
            result = load_dotenv(search_dirs=[d])
            self.assertIsNone(result)


class TestLoadEnvFile(unittest.TestCase):

    def test_parses_key_value(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("MY_KEY=my_value\n")
            f.write("# comment\n")
            f.write("\n")
            f.write('QUOTED="hello world"\n')
            path = f.name
        try:
            # Clear any existing values
            os.environ.pop("MY_KEY", None)
            os.environ.pop("QUOTED", None)
            _load_env_file(path)
            self.assertEqual(os.environ.get("MY_KEY"), "my_value")
            self.assertEqual(os.environ.get("QUOTED"), "hello world")
        finally:
            os.unlink(path)
            os.environ.pop("MY_KEY", None)
            os.environ.pop("QUOTED", None)

    def test_does_not_override_existing(self):
        os.environ["EXISTING_KEY"] = "original"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("EXISTING_KEY=overridden\n")
            path = f.name
        try:
            _load_env_file(path)
            self.assertEqual(os.environ["EXISTING_KEY"], "original")
        finally:
            os.unlink(path)
            os.environ.pop("EXISTING_KEY", None)


class TestMask(unittest.TestCase):

    def test_masks_long_string(self):
        self.assertEqual(_mask("abcdefghij"), "abcd******")

    def test_masks_short_string(self):
        self.assertEqual(_mask("abc"), "***")

    def test_masks_empty(self):
        self.assertEqual(_mask(""), "***")

    def test_masks_none(self):
        self.assertEqual(_mask(None), "***")


class TestResolveTradierCreds(unittest.TestCase):
    """Verify env-specific credentials take precedence with legacy fallback."""

    TRADIER_VARS = [
        "TRADIER_ACCESS_TOKEN", "TRADIER_ACCOUNT_ID",
        "TRADIER_SANDBOX_ACCESS_TOKEN", "TRADIER_SANDBOX_ACCOUNT_ID",
        "TRADIER_PRODUCTION_ACCESS_TOKEN", "TRADIER_PRODUCTION_ACCOUNT_ID",
    ]

    def _clean_env(self):
        """Return a dict with all Tradier vars cleared."""
        env = dict(os.environ)
        for k in self.TRADIER_VARS:
            env.pop(k, None)
        return env

    def test_sandbox_uses_sandbox_specific(self):
        env = self._clean_env()
        env["TRADIER_SANDBOX_ACCESS_TOKEN"] = "sbx-token"
        env["TRADIER_SANDBOX_ACCOUNT_ID"] = "sbx-acct"
        env["TRADIER_PRODUCTION_ACCESS_TOKEN"] = "prd-token"
        env["TRADIER_PRODUCTION_ACCOUNT_ID"] = "prd-acct"
        with patch.dict(os.environ, env, clear=True):
            token, acct, source = _resolve_tradier_creds("sandbox")
        self.assertEqual(token, "sbx-token")
        self.assertEqual(acct, "sbx-acct")
        self.assertEqual(source, "env-specific")

    def test_production_uses_production_specific(self):
        env = self._clean_env()
        env["TRADIER_SANDBOX_ACCESS_TOKEN"] = "sbx-token"
        env["TRADIER_SANDBOX_ACCOUNT_ID"] = "sbx-acct"
        env["TRADIER_PRODUCTION_ACCESS_TOKEN"] = "prd-token"
        env["TRADIER_PRODUCTION_ACCOUNT_ID"] = "prd-acct"
        with patch.dict(os.environ, env, clear=True):
            token, acct, source = _resolve_tradier_creds("production")
        self.assertEqual(token, "prd-token")
        self.assertEqual(acct, "prd-acct")
        self.assertEqual(source, "env-specific")

    def test_legacy_fallback_when_env_specific_missing(self):
        env = self._clean_env()
        env["TRADIER_ACCESS_TOKEN"] = "legacy-token"
        env["TRADIER_ACCOUNT_ID"] = "legacy-acct"
        with patch.dict(os.environ, env, clear=True):
            token, acct, source = _resolve_tradier_creds("sandbox")
        self.assertEqual(token, "legacy-token")
        self.assertEqual(acct, "legacy-acct")
        self.assertEqual(source, "legacy")

    def test_env_specific_wins_over_legacy(self):
        """If both env-specific and legacy are set, env-specific wins."""
        env = self._clean_env()
        env["TRADIER_SANDBOX_ACCESS_TOKEN"] = "sbx-token"
        env["TRADIER_SANDBOX_ACCOUNT_ID"] = "sbx-acct"
        env["TRADIER_ACCESS_TOKEN"] = "legacy-token"
        env["TRADIER_ACCOUNT_ID"] = "legacy-acct"
        with patch.dict(os.environ, env, clear=True):
            token, acct, source = _resolve_tradier_creds("sandbox")
        self.assertEqual(token, "sbx-token")
        self.assertEqual(source, "env-specific")

    def test_no_credentials_returns_empty(self):
        env = self._clean_env()
        with patch.dict(os.environ, env, clear=True):
            token, acct, source = _resolve_tradier_creds("sandbox")
        self.assertEqual(token, "")
        self.assertEqual(acct, "")
        self.assertEqual(source, "none")

    def test_partial_env_specific_falls_back(self):
        """If only the token (not account) is set for the env, fall back to legacy."""
        env = self._clean_env()
        env["TRADIER_SANDBOX_ACCESS_TOKEN"] = "sbx-token"
        # missing TRADIER_SANDBOX_ACCOUNT_ID
        env["TRADIER_ACCESS_TOKEN"] = "legacy-token"
        env["TRADIER_ACCOUNT_ID"] = "legacy-acct"
        with patch.dict(os.environ, env, clear=True):
            token, acct, source = _resolve_tradier_creds("sandbox")
        self.assertEqual(token, "legacy-token")
        self.assertEqual(acct, "legacy-acct")
        self.assertEqual(source, "legacy")

    def test_switching_env_picks_correct_creds(self):
        """Same configured env vars, different TRADIER_ENV -> different creds."""
        env = self._clean_env()
        env["TRADIER_SANDBOX_ACCESS_TOKEN"] = "sbx-token"
        env["TRADIER_SANDBOX_ACCOUNT_ID"] = "sbx-acct"
        env["TRADIER_PRODUCTION_ACCESS_TOKEN"] = "prd-token"
        env["TRADIER_PRODUCTION_ACCOUNT_ID"] = "prd-acct"
        with patch.dict(os.environ, env, clear=True):
            sbx = _resolve_tradier_creds("sandbox")
            prd = _resolve_tradier_creds("production")
        self.assertNotEqual(sbx[0], prd[0])
        self.assertNotEqual(sbx[1], prd[1])


if __name__ == "__main__":
    unittest.main(verbosity=2)

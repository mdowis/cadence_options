"""Tests for dashboard.py, including regression test 7 (.env loader)."""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dashboard import load_dotenv, _load_env_file, _mask


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


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Tests for tradier_client.py. Includes both unit tests (mocked) and
an optional live sandbox smoke test gated behind TRADIER_ACCESS_TOKEN."""

import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from http.client import HTTPResponse
from io import BytesIO

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from cadence.tradier_client import TradierClient, HTTPError


class FakeResponse:
    """Minimal urllib response stand-in."""

    def __init__(self, data, code=200):
        self._data = json.dumps(data).encode("utf-8") if isinstance(data, dict) else data
        self.code = code
        self.status = code

    def read(self):
        return self._data

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class TestTradierClientUnit(unittest.TestCase):
    """Unit tests with mocked HTTP."""

    def setUp(self):
        self.client = TradierClient("test-token", "test-acct", env="sandbox")

    def test_authenticated_true(self):
        self.assertTrue(self.client.authenticated)

    def test_authenticated_false_no_token(self):
        c = TradierClient("", "acct")
        self.assertFalse(c.authenticated)

    def test_authenticated_false_no_account(self):
        c = TradierClient("tok", "")
        self.assertFalse(c.authenticated)

    def test_invalid_env_raises(self):
        with self.assertRaises(ValueError):
            TradierClient("tok", "acct", env="invalid")

    def test_base_url_sandbox(self):
        c = TradierClient("t", "a", env="sandbox")
        self.assertEqual(c.base_url, "https://sandbox.tradier.com/v1")

    def test_base_url_production(self):
        c = TradierClient("t", "a", env="production")
        self.assertEqual(c.base_url, "https://api.tradier.com/v1")

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_quote(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({
            "quotes": {"quote": {"symbol": "SPY", "last": 450.0}}
        })
        result = self.client.get_quote("SPY")
        self.assertEqual(result["symbol"], "SPY")
        self.assertEqual(result["last"], 450.0)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_option_chain(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({
            "options": {"option": [
                {"symbol": "SPY260530C00450000", "strike": 450, "option_type": "call"},
                {"symbol": "SPY260530P00400000", "strike": 400, "option_type": "put"},
            ]}
        })
        result = self.client.get_option_chain("SPY", "2026-05-30")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["strike"], 450)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_option_chain_empty(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({"options": None})
        result = self.client.get_option_chain("SPY", "2026-05-30")
        self.assertEqual(result, [])

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_option_chain_single(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({
            "options": {"option": {"symbol": "SPY260530C00450000", "strike": 450}}
        })
        result = self.client.get_option_chain("SPY", "2026-05-30")
        self.assertEqual(len(result), 1)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_expirations(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({
            "expirations": {"date": ["2026-05-30", "2026-06-20"]}
        })
        result = self.client.get_expirations("SPY")
        self.assertEqual(len(result), 2)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_history(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({
            "history": {"day": [{"date": "2026-04-10", "close": 450.0}]}
        })
        result = self.client.get_history("SPY")
        self.assertEqual(len(result), 1)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_history_empty(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({"history": None})
        result = self.client.get_history("SPY")
        self.assertEqual(result, [])

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_balances(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({
            "balances": {"total_equity": 100000}
        })
        result = self.client.get_account_balances()
        self.assertEqual(result["balances"]["total_equity"], 100000)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_get_positions_empty(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({"positions": None})
        result = self.client.get_positions()
        self.assertEqual(result, [])

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_place_multileg_order(self, mock_urlopen):
        mock_urlopen.return_value = FakeResponse({
            "order": {"id": 12345, "status": "ok"}
        })
        legs = [
            ("SPY260530P00400000", "sell_to_open", 1),
            ("SPY260530P00390000", "buy_to_open", 1),
            ("SPY260530C00500000", "sell_to_open", 1),
            ("SPY260530C00510000", "buy_to_open", 1),
        ]
        result = self.client.place_multileg_order("SPY", legs, price=1.50, tag="test")
        self.assertIn("order", result)

        # Verify the request was formed correctly
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        self.assertEqual(req.method, "POST")
        body = req.data.decode("utf-8")
        self.assertIn("class=multileg", body)
        self.assertIn("option_symbol%5B0%5D=SPY260530P00400000", body)
        self.assertIn("side%5B0%5D=sell_to_open", body)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    def test_http_error_raised(self, mock_urlopen):
        error = urllib_http_error(401, "Unauthorized")
        mock_urlopen.side_effect = error
        with self.assertRaises(HTTPError) as ctx:
            self.client.get_quote("SPY")
        self.assertEqual(ctx.exception.status_code, 401)

    @patch("cadence.tradier_client.urllib.request.urlopen")
    @patch("cadence.tradier_client.time.sleep")
    def test_429_retry(self, mock_sleep, mock_urlopen):
        """429 should retry with backoff, then succeed."""
        error_429 = urllib_http_error(429, "Rate limited")
        mock_urlopen.side_effect = [
            error_429,
            FakeResponse({"quotes": {"quote": {"symbol": "SPY", "last": 450}}}),
        ]
        result = self.client.get_quote("SPY")
        self.assertEqual(result["symbol"], "SPY")
        mock_sleep.assert_called_once()

    @patch("cadence.tradier_client.urllib.request.urlopen")
    @patch("cadence.tradier_client.time.sleep")
    def test_429_exhausted(self, mock_sleep, mock_urlopen):
        """429 on all attempts should raise HTTPError."""
        mock_urlopen.side_effect = urllib_http_error(429, "Rate limited")
        with self.assertRaises(HTTPError) as ctx:
            self.client.get_quote("SPY")
        self.assertEqual(ctx.exception.status_code, 429)


def urllib_http_error(code, msg):
    """Create a urllib.error.HTTPError for testing."""
    import urllib.error
    resp = BytesIO(msg.encode("utf-8"))
    return urllib.error.HTTPError(
        url="http://test", code=code, msg=msg, hdrs={}, fp=resp
    )


# -- Live sandbox smoke test (skipped if no credentials) ---------------------

@unittest.skipUnless(
    os.environ.get("TRADIER_ACCESS_TOKEN") and os.environ.get("TRADIER_ACCOUNT_ID"),
    "Set TRADIER_ACCESS_TOKEN and TRADIER_ACCOUNT_ID to run live sandbox tests"
)
class TestTradierClientLive(unittest.TestCase):
    """Smoke tests against Tradier sandbox. NOT run in CI."""

    def setUp(self):
        self.client = TradierClient(
            access_token=os.environ["TRADIER_ACCESS_TOKEN"],
            account_id=os.environ["TRADIER_ACCOUNT_ID"],
            env=os.environ.get("TRADIER_ENV", "sandbox"),
        )

    def test_get_balances(self):
        result = self.client.get_account_balances()
        balances = result.get("balances", {})
        self.assertIn("total_equity", balances)
        print(f"  Balance: total_equity={balances['total_equity']}")

    def test_get_spy_quote(self):
        result = self.client.get_quote("SPY")
        self.assertIn("last", result)
        print(f"  SPY last={result['last']}")

    def test_get_spy_expirations(self):
        result = self.client.get_expirations("SPY")
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        print(f"  SPY expirations (first 5): {result[:5]}")

    def test_get_spy_option_chain_with_greeks(self):
        expirations = self.client.get_expirations("SPY")
        self.assertGreater(len(expirations), 0)
        # Pick first available expiration
        chain = self.client.get_option_chain("SPY", expirations[0], greeks=True)
        self.assertGreater(len(chain), 0)
        first = chain[0]
        self.assertIn("strike", first)
        # Check Greeks are present
        greeks = first.get("greeks", {})
        print(f"  Chain for {expirations[0]}: {len(chain)} options")
        if greeks:
            print(f"  First option greeks: delta={greeks.get('delta')}, "
                  f"iv={greeks.get('mid_iv')}")


if __name__ == "__main__":
    unittest.main(verbosity=2)

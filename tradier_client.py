"""Tradier REST API client. Stdlib only (urllib.request, json)."""

import json
import time
import urllib.request
import urllib.parse
import urllib.error
import logging

logger = logging.getLogger(__name__)

BASE_URLS = {
    "sandbox": "https://sandbox.tradier.com/v1",
    "production": "https://api.tradier.com/v1",
}

MAX_RETRIES = 3
RETRY_BASE_DELAY = 1.0  # seconds


class HTTPError(Exception):
    """Non-2xx response from Tradier."""

    def __init__(self, status_code, body, url=""):
        self.status_code = status_code
        self.body = body
        self.url = url
        super().__init__(f"HTTP {status_code}: {body[:200]}")


class TradierClient:
    """Thin wrapper around the Tradier REST API."""

    def __init__(self, access_token, account_id, env="sandbox"):
        if env not in BASE_URLS:
            raise ValueError(f"env must be 'sandbox' or 'production', got {env!r}")
        self.access_token = access_token or ""
        self.account_id = account_id or ""
        self.env = env
        self.base_url = BASE_URLS[env]

    @property
    def authenticated(self):
        return bool(self.access_token) and bool(self.account_id)

    # -- Public API methods --------------------------------------------------

    def get_quote(self, symbol):
        """Live quote for a symbol."""
        data = self._request("GET", "/markets/quotes", params={"symbols": symbol})
        quotes = data.get("quotes", {})
        quote = quotes.get("quote", {})
        # Tradier returns a list when multiple symbols, dict when single
        if isinstance(quote, list):
            return quote[0] if quote else {}
        return quote

    def get_option_chain(self, symbol, expiration, greeks=True):
        """Full option chain for a symbol/expiration. Returns list of options."""
        params = {
            "symbol": symbol,
            "expiration": expiration,
            "greeks": "true" if greeks else "false",
        }
        data = self._request("GET", "/markets/options/chains", params=params)
        options = data.get("options", {})
        if options is None:
            return []
        option_list = options.get("option", [])
        if isinstance(option_list, dict):
            return [option_list]
        return option_list or []

    def get_expirations(self, symbol):
        """Available expiration dates for a symbol. Returns list of date strings."""
        data = self._request("GET", "/markets/options/expirations", params={"symbol": symbol})
        expirations = data.get("expirations", {})
        if expirations is None:
            return []
        dates = expirations.get("date", [])
        if isinstance(dates, str):
            return [dates]
        return dates or []

    def get_history(self, symbol, interval="daily", start=None, end=None):
        """Historical prices. Returns list of day dicts."""
        params = {"symbol": symbol, "interval": interval}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        data = self._request("GET", "/markets/history", params=params)
        history = data.get("history", {})
        if history is None:
            return []
        days = history.get("day", [])
        if isinstance(days, dict):
            return [days]
        return days or []

    def get_account_balances(self):
        """Account equity and cash."""
        return self._request("GET", f"/accounts/{self.account_id}/balances")

    def get_positions(self):
        """Open positions."""
        data = self._request("GET", f"/accounts/{self.account_id}/positions")
        positions = data.get("positions", {})
        if positions is None or positions == "null":
            return []
        pos_list = positions.get("position", [])
        if isinstance(pos_list, dict):
            return [pos_list]
        return pos_list or []

    def place_multileg_order(self, symbol, legs, order_type="credit",
                             duration="day", price=None, tag=None):
        """Place a multi-leg order. legs: list of (option_symbol, side, quantity)."""
        form = {
            "class": "multileg",
            "symbol": symbol,
            "type": order_type,
            "duration": duration,
        }
        if price is not None:
            form["price"] = str(price)
        if tag:
            form["tag"] = tag
        for i, (opt_symbol, side, qty) in enumerate(legs):
            form[f"option_symbol[{i}]"] = opt_symbol
            form[f"side[{i}]"] = side
            form[f"quantity[{i}]"] = str(qty)
        data = self._request("POST", f"/accounts/{self.account_id}/orders", data=form)
        return data

    def get_order(self, order_id):
        """Order status."""
        data = self._request("GET", f"/accounts/{self.account_id}/orders/{order_id}")
        return data.get("order", data)

    def cancel_order(self, order_id):
        """Cancel an order."""
        return self._request("DELETE", f"/accounts/{self.account_id}/orders/{order_id}")

    # -- Internal ------------------------------------------------------------

    def _request(self, method, path, params=None, data=None):
        """Make an HTTP request to Tradier with retry on 429."""
        url = self.base_url + path
        if params:
            url += "?" + urllib.parse.urlencode(params)

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

        body = None
        if data:
            body = urllib.parse.urlencode(data).encode("utf-8")
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        for attempt in range(MAX_RETRIES + 1):
            req = urllib.request.Request(url, data=body, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    raw = resp.read().decode("utf-8")
                    if not raw.strip():
                        return {}
                    return json.loads(raw)
            except urllib.error.HTTPError as e:
                resp_body = ""
                try:
                    resp_body = e.read().decode("utf-8")
                except Exception:
                    pass
                if e.code == 429 and attempt < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning("Rate limited (429), retrying in %.1fs (attempt %d/%d)",
                                   delay, attempt + 1, MAX_RETRIES)
                    time.sleep(delay)
                    continue
                raise HTTPError(e.code, resp_body, url) from e
            except urllib.error.URLError as e:
                if attempt < MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * (2 ** attempt)
                    logger.warning("URL error %s, retrying in %.1fs", e, delay)
                    time.sleep(delay)
                    continue
                raise

        # Should not reach here, but just in case
        raise RuntimeError("Exhausted retries")

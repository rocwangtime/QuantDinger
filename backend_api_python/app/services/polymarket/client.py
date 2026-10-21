"""Bounded public HTTP reads. No wallet, signing, order, or funding endpoints."""

import time
import logging
from urllib.parse import urlsplit, urlunsplit

import requests

from .engine import market_from_gamma, normalize_book

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"


class PublicClient:
    def __init__(self, session=None, *, session_factory=None, max_session_age=300, failure_limit=2):
        self.session_factory = session_factory or requests.Session
        self.session = session if session is not None else self.session_factory()
        self.session_started = time.monotonic()
        self.max_session_age = max_session_age
        self.failure_limit = failure_limit
        self.failures = 0
        self.last_error = None

    def close(self):
        self.session.close()

    def _renew(self):
        self.session.close()
        self.session = self.session_factory()
        self.session_started = time.monotonic()
        self.failures = 0

    def _json(self, method, url, **kwargs):
        if time.monotonic() - self.session_started >= self.max_session_age:
            self._renew()
        response = None
        try:
            response = self.session.request(method, url, timeout=(3, 8), allow_redirects=False, **kwargs)
            if 300 <= getattr(response, 'status_code', 200) < 400:
                raise requests.HTTPError('redirect response')
            response.raise_for_status()
            if len(response.content) > 8 * 1024 * 1024:
                raise ValueError("polymarket.responseTooLarge")
            content_type = getattr(response, 'headers', {}).get('Content-Type', '').split(';')[0].lower()
            if content_type and content_type != 'application/json' and not content_type.endswith('+json'):
                raise ValueError('polymarket.nonJsonResponse')
            result = response.json()
        except (requests.RequestException, ValueError, TypeError) as exc:
            # Public diagnostics only: no response body, URL query, userinfo, or headers.
            location = urlsplit(getattr(response, 'headers', {}).get('Location', '')) if response is not None else None
            self.last_error = dict(exception_class=type(exc).__name__,
                http_status=getattr(response, 'status_code', 200) if response is not None else None,
                host=urlsplit(url).hostname,
                location=urlunsplit((location.scheme, location.hostname or '', location.path, '', '')) if location else None)
            logging.getLogger(__name__).warning('Polymarket public HTTP failure %s', self.last_error)
            self.failures += 1
            if self.failures >= self.failure_limit:
                self._renew()
            error = ('polymarket.invalidApiResponse' if isinstance(exc, (TypeError, requests.exceptions.JSONDecodeError))
                     or isinstance(exc, ValueError) and not isinstance(exc, requests.RequestException)
                     else 'polymarket.publicApiUnavailable')
            raise ValueError(error) from None
        self.failures = 0
        return result

    def market(self, market_id):
        identity = str(market_id)
        if not identity.isdigit() or len(identity) > 20:
            raise ValueError("polymarket.invalidMarketId")
        return market_from_gamma(self._json("GET", f"{GAMMA}/markets/{identity}"))

    def discover(self, *, market_ids=None, limit=8):
        selected, excluded = [], []
        if market_ids:
            for identity in market_ids:
                try:
                    selected.append(self.market(identity))
                except ValueError as exc:
                    excluded.append({"marketId": str(identity), "reason": str(exc)})
        else:
            rows = self._json("GET", f"{GAMMA}/markets", params={"active": "true", "closed": "false",
                              "limit": 100, "order": "volume24hr", "ascending": "false"})
            if not isinstance(rows, list):
                raise ValueError("polymarket.invalidApiResponse")
            for row in rows:
                try:
                    selected.append(market_from_gamma(row))
                except (ValueError, KeyError, TypeError) as exc:
                    excluded.append({"marketId": str(row.get("id", "")), "reason": str(exc)})
                if len(selected) >= limit:
                    break
        # Deduplicate by onchain condition, not just the catalogue row ID.
        conditions, unique = set(), []
        for market in selected:
            if market["conditionId"] not in conditions:
                unique.append(market)
                conditions.add(market["conditionId"])
        return unique[:limit], excluded

    def capture(self, markets):
        observed = int(time.time() * 1000)
        frames = {market["id"]: {"observedMs": observed, "source": "clob-rest-snapshot", "books": {}}
                  for market in markets}
        assets = {asset: market for market in markets for asset in (market["yesAssetId"], market["noAssetId"])}
        try:
            rows = self._json("POST", f"{CLOB}/books", json=[{"token_id": asset} for asset in assets])
            if not isinstance(rows, list):
                raise ValueError("polymarket.invalidApiResponse")
            seen = set()
            for row in rows:
                asset = str(row.get("asset_id", ""))
                if asset not in assets or asset in seen:
                    raise ValueError("polymarket.bookIdentityMismatch")
                seen.add(asset)
                market = assets[asset]
                try:
                    frames[market["id"]]["books"][asset] = normalize_book(row, market, asset, observed)
                except (ValueError, TypeError, KeyError) as exc:
                    frames[market["id"]]["error"] = str(exc)
            if seen != set(assets):
                for market in markets:
                    if not {market["yesAssetId"], market["noAssetId"]} <= seen:
                        frames[market["id"]]["error"] = "polymarket.observationMissing"
        except (ValueError, TypeError, KeyError) as exc:
            for frame in frames.values():
                frame["error"] = str(exc)
        completed = int(time.time() * 1000)
        for frame in frames.values():
            frame["requestStartedMs"] = observed
            frame["observedMs"] = completed
            frame["requestDurationMs"] = completed - observed
        return frames

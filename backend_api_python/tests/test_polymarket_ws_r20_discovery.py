from datetime import datetime, timedelta, timezone

import pytest
import requests

from app.services.polymarket import observer, updown
from app.services.polymarket.client import PublicClient


def response(code=200, body=b'[]', **headers):
    row = requests.Response()
    row.status_code, row._content = code, body
    row.headers.update(headers)
    return row


class Session:
    def __init__(self, row):
        self.row, self.closed, self.calls = row, False, 0

    def request(self, *args, **kwargs):
        self.calls += 1
        assert kwargs['allow_redirects'] is False
        return self.row

    def close(self):
        self.closed = True


def test_redirect_renews_pool_and_records_public_diagnostics(caplog):
    old = Session(response(301, b'<html>', Location='https://gamma-api.polymarket.com/series?token=private'))
    fresh = Session(response())
    client = PublicClient(old, session_factory=lambda: fresh)
    for _ in range(2):
        with pytest.raises(ValueError, match='publicApiUnavailable'):
            client._json('GET', 'https://gamma-api.polymarket.com/series')
    assert old.closed and fresh.calls == 0  # no extra REST retries
    assert client.last_error == dict(exception_class='HTTPError', http_status=301,
        host='gamma-api.polymarket.com', location='https://gamma-api.polymarket.com/series')
    assert 'private' not in caplog.text
    assert client._json('GET', 'https://gamma-api.polymarket.com/series') == []


def test_non_json_and_invalid_json_are_errors_and_session_has_max_age(monkeypatch):
    old = Session(response(body=b'<html>', **{'Content-Type': 'text/html'}))
    fresh = Session(response(body=b'not json', **{'Content-Type': 'application/json'}))
    client = PublicClient(old, session_factory=lambda: fresh)
    with pytest.raises(ValueError, match='invalidApiResponse'):
        client._json('GET', 'https://gamma-api.polymarket.com/series')
    client.session_started -= 301
    with pytest.raises(ValueError, match='invalidApiResponse'):
        client._json('GET', 'https://gamma-api.polymarket.com/series')
    assert old.closed and client.last_error['exception_class'] == 'JSONDecodeError'


def test_failed_discovery_retains_only_live_hot_markets_and_alerts_after_two_minutes():
    now = datetime.now(timezone.utc)
    good = dict(id='live', market_type='up_down', asset='BTC', interval='5m',
                endDate=(now + timedelta(seconds=90)).isoformat())
    expired = dict(good, id='expired', endDate=(now - timedelta(seconds=1)).isoformat())
    state = observer.DiscoveryHealth()
    state.previous = [good, expired]
    rejected = [dict(series=updown.series_slug(*k.split(':')[1:]), reason='unavailable')
                for k in observer.HOT_SERIES]
    markets, status = state.update([], rejected, now, 10)
    assert markets == [good] and not status['level1_alert']
    markets, status = state.update([], rejected, now + timedelta(seconds=121), 131)
    assert markets == [] and status['level1_alert']
    _, status = state.update([good], [], now, 132)
    assert not status['level1_alert']

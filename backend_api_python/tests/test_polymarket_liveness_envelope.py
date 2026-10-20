"""Part A: orjson liveness envelope and no parse cache in hot-group children.

The orjson path must return exactly what the regex scan returns. Real frames
come from the public BTC up/down 5m market channel (2026-10-20); set
QD_PM_FRAME_CORPUS to a full corpus (.json or .json.gz list) to check more.
"""
import gzip
import json
import os
import random
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.services.polymarket import ws_payloads
from app.services.polymarket.ws_payloads import (
    ParsedFrames, _fast_envelope, _regex_envelope, cheap_envelope)

FIXTURE = Path(__file__).parent / 'fixtures' / 'polymarket' / 'btc5m_frames_20261020.json.gz'
ASSETS = ('103854419362010527824643031444647473033880322181408548844307608875643251735129',
          '82484581550145249587888716535615116089482329913750340157299670692852181990416')


def _fill(template, index):
    return (template.replace('__A0__', ASSETS[0]).replace('__A1__', ASSETS[1])
            .replace('__H', f'{index}x').replace('__TS__', str(1792485000000 + index)))


def _frames():
    data = json.load(gzip.open(FIXTURE, 'rt'))
    frames = [_fill(t, i) for i, t in enumerate(data['frames'])]
    extra = os.getenv('QD_PM_FRAME_CORPUS')
    if extra:
        opener = gzip.open if extra.endswith('.gz') else open
        frames += [_fill(t, i) for i, t in enumerate(json.load(opener(extra, 'rt')))]
    return frames


def test_real_frames_match_regex_and_take_fast_path():
    frames = _frames()
    kinds = {json.loads(f)[0]['event_type'] if f.startswith('[') else json.loads(f)['event_type'] for f in frames}
    assert {'price_change', 'book', 'last_trade_price'} <= kinds
    for raw in frames:
        expected = _regex_envelope(raw)
        assert expected
        assert _fast_envelope(raw) == expected
        assert cheap_envelope(raw) == expected
        assert cheap_envelope(raw.encode()) == expected


EDGE_CASES = [
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h","bids":[],"asks":[]}',
    '{"asset_id":"a","hash":"h","bids":[{"price":"1","size":"2"}],"timestamp":"1","event_type":"book"}',
    '{"hash":"h","asset_id":"a","timestamp":"1","event_type":"book"}',
    '{"asset_id":"a","timestamp":"1","event_type":"book"}',
    '{"event_type":"book","asset_id":"a","timestamp":1792485000000,"hash":"h"}',
    '{"event_type":"book","asset_id":"a","timestamp":null,"hash":"h"}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":""}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"has h"}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h,x"}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h}x"}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h\\u00a0x"}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h\\"x"}',
    '{"event_type":"book","asset_id":"a","timestamp":1.5,"hash":"h"}',
    '{"event_type":"book","asset_id":"a","timestamp":true,"hash":"h"}',
    '{"event_type":"book","asset_id":7,"timestamp":"1","hash":"h"}',
    '{"event_type":"book","timestamp":"1","hash":"h"}',
    '{"asset_id":"a","timestamp":"1","hash":"h"}',
    '{"event_type":"","asset_id":"a"}',
    '{"event_type":"book","asset_id":"a","meta":{"timestamp":"9"},"timestamp":"1","hash":"h"}',
    '{"event_type":"book","asset_id":"a","bids":[{"price":"1","size":"2","hash":"z"}],"timestamp":"1"}',
    '{"payload":{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h"}}',
    '{"type":"book","payload":{"asset_id":"a","timestamp":"1","hash":"h"}}',
    '{"market":"m","price_changes":[{"asset_id":"a","price":".5","size":"1","side":"BUY","hash":"h1"},'
    '{"asset_id":"b","price":".5","size":"1","side":"SELL","hash":"h2"}],"timestamp":"1","event_type":"price_change"}',
    '{"event_type":"price_change","timestamp":"1","price_changes":[{"asset_id":"a","hash":"h1"},{"asset_id":"b"}]}',
    '{"event_type":"price_change","hash":"top","price_changes":[{"asset_id":"a"},{"asset_id":"b","hash":"h2"}],"timestamp":"1"}',
    '{"event_type":"price_change","price_changes":[{"asset_id":"a","hash":"h1"}],"timestamp":"1","hash":"late"}',
    '{"event_type":"price_change","asset_id":"top","price_changes":[{"price":".5","size":"1"}],"timestamp":"1"}',
    '{"event_type":"price_change","price_changes":[{"asset_id":"a","hash":"h1","timestamp":"7"}],"timestamp":"1"}',
    '{"event_type":"price_change","price_changes":[],"timestamp":"1"}',
    '{"event_type":"price_change","price_changes":{"asset_id":"a","hash":"h"},"timestamp":"1"}',
    '{"event_type":"price_change","priceChanges":[{"asset_id":"a","hash":"h1"}],"timestamp":"1"}',
    '{"event_type":"price_change","price_changes":[{"asset_id":"a","hash":"h1","x":{"asset_id":"n"}}],"timestamp":"1"}',
    '{"event_type":"price_change","price_changes":["a"],"timestamp":"1"}',
    '{"event_type":"last_trade_price","asset_id":"a","price":".5","side":"BUY","size":"3","timestamp":"1",'
    '"transaction_hash":"0xabc","fee_rate_bps":"0","market":"m"}',
    '{"event_type":"tick_size_change","asset_id":"a","old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"1"}',
    '[{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h"},{"event_type":"book","asset_id":"b","timestamp":"2","hash":"g"}]',
    '[{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h"},3]',
    '[]', '{}', '[[{"event_type":"book","asset_id":"a","timestamp":"1"}]]',
    'PONG', 'not json', '{"event_type":"book","asset_id":"a"', '"string"', '17', 'null',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h","note":"{\\"asset_id\\":\\"x\\"}"}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":"h","big":123456789012345678901234567890}',
    '{"event_type":"book","asset_id":"a","timestamp":"1","hash":[]}',
    '{"event_type":"price_change","price_changes":[{"asset_id":"a","hash":"h1"},{"hash":"h2","asset_id":"b"}],"timestamp":"1"}',
    '{"event_type":"price_change","price_changes":[{"hash":"h1","asset_id":"a"},{"asset_id":"b"}],"timestamp":"1"}',
]


@pytest.mark.parametrize('raw', EDGE_CASES)
def test_edge_cases_match_regex(raw):
    assert cheap_envelope(raw) == _regex_envelope(raw)


def test_random_mutations_of_real_frames_match_regex():
    rng = random.Random(20261020)
    frames = _frames()[:400]
    values = [None, True, 1.5, 7, '', 'a b', 'x,y', 'ok', {'hash': 'n'}, [{'asset_id': 'n'}], []]
    for _ in range(5000):
        data = json.loads(rng.choice(frames))
        target = data[0] if isinstance(data, list) else data
        rows = target.get('price_changes')
        holder = rng.choice(rows) if rows and rng.random() < 0.5 else target
        key = rng.choice(['event_type', 'asset_id', 'timestamp', 'hash', 'price_changes', 'bids', 'extra'])
        roll = rng.random()
        if roll < 0.2:
            holder.pop(key, None)
        elif roll < 0.4:
            items = list(holder.items())
            rng.shuffle(items)
            holder.clear()
            holder.update(items)
        else:
            holder[key] = rng.choice(values)
        raw = json.dumps(data, separators=(',', ':'))
        assert cheap_envelope(raw) == _regex_envelope(raw), raw


def test_without_orjson_uses_regex(monkeypatch):
    raw = _frames()[5]
    expected = _regex_envelope(raw)
    monkeypatch.setattr(ws_payloads, 'orjson', None)
    assert _fast_envelope(raw) is None
    assert cheap_envelope(raw) == expected


def test_uncached_parse_matches_cached_parse():
    cached, direct = ParsedFrames(), ParsedFrames(cache=False)
    for raw in _frames()[:300]:
        first, second = cached.parse(raw), direct.parse(raw)
        assert first == second
        assert type(first) is type(second)
        for left, right in zip(first if isinstance(first, list) else [first],
                               second if isinstance(second, list) else [second]):
            assert left.changes == right.changes
            assert left.digest == right.digest
            assert left.book == right.book
    metrics = direct.metrics()
    assert metrics['entries'] == 0 and metrics['charged_bytes'] == 0 and metrics['hits'] == 0
    assert metrics['misses'] == 300
    assert direct.parse({'already': 'parsed'}) == {'already': 'parsed'}
    assert direct.parse(bytearray(b'{"event_type":"book","asset_id":"a"}'))['asset_id'] == 'a'


def test_hot_children_drop_the_parse_cache_cold_keeps_it():
    from app.services.polymarket.ws_group_process import configure_parse_cache
    hot = SimpleNamespace(parsed_frames=ParsedFrames())
    cold = SimpleNamespace(parsed_frames=ParsedFrames())
    assert configure_parse_cache(hot, dict(key='up_down:BTC:5m', hot=True)).cache is False
    assert configure_parse_cache(cold, dict(key='cold', hot=False)).cache is True

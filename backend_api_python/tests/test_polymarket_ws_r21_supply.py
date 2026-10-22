"""Business supply is independent of physical promotion and natural quiet."""
import json
import time
from datetime import datetime, timezone

from app.services.polymarket import observer
from app.services.polymarket.ws_redundancy import FeedMerge
from app.services.polymarket.ws_warm_book import WarmBook
from tests.test_polymarket_liveness_silence import _wired


def setup_supply(monkeypatch):
    manager, primary, spare, requests, retired = _wired()
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    monkeypatch.setattr(observer, '_PIPELINE', pipeline)
    spare['shadow'] = WarmBook(['ay', 'an'])
    events = [dict(event_type='book', asset_id=a, timestamp=1000, hash='initial'+a,
        bids=[dict(price='.4', size='2')], asks=[dict(price='.6', size='1')]) for a in ['ay','an']]
    manager.merges[0] = FeedMerge({})
    manager.merges[0].filter(events, primary['source'], {'ay', 'an'})
    pipeline.receive(events)
    manager._remember_books(primary, events)
    manager.continuity.publication(pipeline.recording.metadata, {'ay', 'an'})
    primary['inbox'].last_data_at = primary['inbox'].last_byte_at = time.monotonic()
    manager.liveness_receive(0, spare, json.dumps(events), datetime.now(timezone.utc), spare['inbox'])
    return manager, primary, spare, pipeline, requests, retired


def emit(manager, spare, timestamp=1100, size='3', received=None):
    event = dict(event_type='price_change', timestamp=timestamp, price_changes=[
        dict(asset_id=a, side='BUY', price='.4', size=size, hash=str(timestamp)+a) for a in ['ay', 'an']])
    spare['inbox'].last_data_at = time.monotonic()
    manager.liveness_receive(0, spare, json.dumps(event), received or datetime.now(timezone.utc), spare['inbox'])


def test_receiving_spare_supplies_before_four_second_physical_promotion(monkeypatch):
    m, primary, spare, pipeline, requests, retired = setup_supply(monkeypatch)
    primary['inbox'].last_data_at = primary['inbox'].last_byte_at = time.monotonic()-.3
    emit(m, spare)
    assert all(pipeline.books[a]['bids']['.4'] == '3' for a in ['ay','an'])
    assert m.states[0] is primary and not retired and not requests and m.backup_promotions == 0
    # Supply continues with the same source even after >2048 backup checkpoints.
    for n in range(1101, 3200):
        emit(m, spare, n, str(n))
    primary['inbox'].last_byte_at = time.monotonic()-5
    emit(m, spare, 3200, '7')
    from unittest.mock import patch
    now = time.monotonic()
    with patch.object(observer.time, 'monotonic', return_value=now+.5):
        emit(m, spare, 3201, '7')
    with patch.object(observer.time, 'monotonic', return_value=now+1.1):
        emit(m, spare, 3202, '7')
    assert m.states[0] is spare and not requests and not spare.get('need_book')
    assert pipeline.books['ay']['bids']['.4'] == '7'


def test_no_speculative_supply_on_recent_primary_or_unverified_book(monkeypatch):
    m, primary, spare, pipeline, *_ = setup_supply(monkeypatch)
    emit(m, spare)
    assert pipeline.books['ay']['bids']['.4'] == '2' and m.states[0] is primary
    primary['inbox'].last_data_at = time.monotonic()-.3
    primary['book_checkpoints'].prune(set())
    emit(m, spare, 1200)
    assert pipeline.books['ay']['bids']['.4'] == '2' and m.states[0] is primary


def test_supply_generation_and_subscription_changes_require_new_proof(monkeypatch):
    m, primary, spare, pipeline, *_ = setup_supply(monkeypatch)
    primary['inbox'].last_data_at = time.monotonic()-.3
    emit(m, spare)
    primary['inbox'].epoch += 1
    primary['book_checkpoints'].prune(set())
    emit(m, spare, 1200, '99')
    assert pipeline.books['ay']['bids']['.4'] == '3'
    primary['inbox'].resync.set()
    emit(m, spare, 1300, '88')
    assert pipeline.books['ay']['bids']['.4'] == '3'


def test_supply_keeps_per_level_watermarks_and_does_not_invent_publications(monkeypatch):
    m, primary, spare, pipeline, *_ = setup_supply(monkeypatch)
    primary['inbox'].last_data_at = time.monotonic()-.3
    emit(m, spare)
    # A later delivery for another level is valid even with an earlier timestamp.
    event = dict(event_type='price_change', timestamp=1050, price_changes=[
        dict(asset_id='ay', side='SELL', price='.6', size='9', hash='late')])
    m.liveness_receive(0, spare, json.dumps(event), datetime.now(timezone.utc), spare['inbox'])
    assert pipeline.books['ay'] == {'bids': {'.4':'3'}, 'asks': {'.6':'9'}}
    previous = dict(m.continuity.published['ay'])
    # A control tick or an empty envelope cannot pretend to be fresh market data.
    m.liveness_receive(0, spare, '[]', datetime.now(timezone.utc), spare['inbox'])
    assert m.continuity.published['ay'] == previous
    assert m.states[0] is primary


def test_subscription_change_during_detached_parse_cannot_publish(monkeypatch):
    m, primary, spare, pipeline, *_ = setup_supply(monkeypatch)
    primary['inbox'].last_data_at = time.monotonic()-.3
    shadow = spare['shadow']
    receive = shadow.receive
    def crossed(event, at):
        receive(event, at)
        spare['inbox'].epoch += 1
    monkeypatch.setattr(shadow, 'receive', crossed)
    emit(m, spare)
    assert spare['inbox'].resync.is_set()
    assert pipeline.books['ay']['bids']['.4'] == '2'
    assert m.states[0] is primary


def test_cached_supply_proof_cannot_bypass_missing_shared_book_gate(monkeypatch):
    m, primary, spare, pipeline, *_ = setup_supply(monkeypatch)
    primary['inbox'].last_data_at = time.monotonic()-.3
    emit(m, spare)
    assert pipeline.books['ay']['bids']['.4'] == '3'
    # The no-ready-spare REST path may remove shared books without an epoch
    # change. A later PONG cannot turn a bare delta into a full baseline.
    pipeline.books.pop('ay')
    pipeline.recording.metadata.pop('ay')
    emit(m, spare, 1200, '99')
    assert 'ay' not in pipeline.books
    assert pipeline.books['an']['bids']['.4'] == '99'


def test_diagnostic_snapshot_of_promoted_spare_is_not_a_ready_backup(monkeypatch):
    m, primary, spare, _, _, _ = setup_supply(monkeypatch)
    # A close callback may already hold a backup_items() reference when the
    # worker promotes that object and releases its private ladder.
    primary['connected'] = False
    assert m._promote_liveness_backup(0, spare)
    assert m.states[0] is spare
    assert not m.backup_ready(spare)
    assert m.readiness_blockers(spare)


def test_diagnostics_accept_a_registered_spare_before_its_queue_is_connected(monkeypatch):
    m, _, _, _, _, _ = setup_supply(monkeypatch)
    pending = dict(primary_index=0, assets=['ay', 'an'], redundant=True, connected=False)
    assert not m.backup_ready(pending)
    assert 'disconnected' in m.readiness_blockers(pending)

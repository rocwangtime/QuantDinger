from datetime import datetime, timezone

from app.services.polymarket.ws_warm_book import BookCheckpoints, WarmBook

NOW = datetime.now(timezone.utc)


def book(ts=1000, digest='h1'):
    return dict(event_type='book', asset_id='a', timestamp=ts, hash=digest,
                bids=[dict(price='.4', size='2')], asks=[dict(price='.6', size='1')])


def change(ts=1001, digest='h2'):
    return dict(event_type='price_change', timestamp=ts,
                price_changes=[dict(asset_id='a', side='BUY', price='.4', size='3', hash=digest)])


def test_complete_equal_books_retain_baseline_and_detect_divergence():
    shadow, peer = WarmBook(['a']), BookCheckpoints()
    shadow.receive(book(), NOW)
    peer.receive(book())
    live = {'bids': {'.4': '2'}, 'asks': {'.6': '1'}}
    at = datetime.fromtimestamp(1, timezone.utc)
    assert shadow.baseline('a', peer, live, at) == 'retained'
    live['bids']['.4'] = '99'
    assert shadow.baseline('a', peer, live, at) is None


def test_continuous_backup_ahead_delivers_its_assembled_book():
    shadow, peer = WarmBook(['a']), BookCheckpoints()
    shadow.receive(book(), NOW)
    peer.receive(book())
    shadow.receive(change(), NOW)
    result = shadow.baseline('a', peer, {'bids': {'.4': '2'}, 'asks': {'.6': '1'}},
                             datetime.fromtimestamp(1, timezone.utc))
    assert result['timestamp'] == 1001 and result['bids'] == [dict(price='.4', size='3')]


def test_behind_backup_with_common_checkpoint_keeps_newer_primary_book():
    shadow, peer = WarmBook(['a']), BookCheckpoints()
    shadow.receive(book(), NOW)
    peer.receive(book())
    peer.receive(change())
    assert shadow.baseline('a', peer, {'bids': {'.4': '3'}, 'asks': {'.6': '1'}},
                           datetime.fromtimestamp(1.001, timezone.utc)) == 'retained'


def test_missing_snapshot_hash_or_common_watermark_requires_rest():
    shadow, peer = WarmBook(['a']), BookCheckpoints()
    shadow.receive(change(), NOW)
    peer.receive(book())
    assert shadow.baseline('a', peer, {}, datetime.fromtimestamp(1, timezone.utc)) is None
    shadow.receive(book(digest='different'), NOW)
    assert shadow.baseline('a', peer, {}, datetime.fromtimestamp(1, timezone.utc)) is None
    shadow.prune([])
    assert not shadow.books and not shadow.checkpoints.latest


def test_no_code_close_promotes_complete_warm_book_without_rest(monkeypatch):
    import json
    import time
    from app.services.polymarket import observer
    from app.services.polymarket.ws_redundancy import FeedMerge
    from tests.test_polymarket_liveness_silence import _wired

    manager, primary, spare, requests, _ = _wired()
    pipeline = observer.Pipeline()
    pipeline.recording.enabled = False
    monkeypatch.setattr(observer, '_PIPELINE', pipeline)
    spare['shadow'] = WarmBook(['ay', 'an'])
    events = [dict(book(), asset_id=a) for a in ('ay', 'an')]
    manager.merges[0] = FeedMerge({})
    manager.merges[0].filter(events, 'primary', {'ay', 'an'})
    pipeline.receive(events)
    manager._remember_books(primary, events)
    manager.continuity.publication(pipeline.recording.metadata, {'ay', 'an'})
    manager.liveness_receive(0, spare, json.dumps(events), NOW, spare['inbox'])
    assert manager.states[0] is primary
    primary.update(connected=False, failed_at=time.monotonic())
    manager.liveness_receive(0, spare, json.dumps(events), NOW, spare['inbox'])
    assert manager.states[0] is spare and not spare.get('need_book') and requests == []
    assert spare['baseline_handoff']['verified'] == ['an', 'ay']
    # The next valid delta must immediately reach the retained baseline.
    event = dict(change(), price_changes=[dict(asset_id='ay', side='BUY', price='.4', size='3', hash='h2')])
    pipeline.receive(manager.merges[0].filter(event, spare['source'], {'ay', 'an'}))
    assert pipeline.books['ay']['bids']['.4'] == '3'

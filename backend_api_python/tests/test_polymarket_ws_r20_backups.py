import time
from types import SimpleNamespace

from app.services.polymarket import observer
from tests.test_polymarket_liveness_silence import _wired, _feed
from tests.test_polymarket_ws_rotation import feed_manager, wait_for
from tests.test_polymarket_ws_round7 import make_hot


def test_fresh_envelope_does_not_hide_94_second_stale_pong():
    m, primary, spare, *_ = _wired()
    spare['socket'] = SimpleNamespace(transport=SimpleNamespace(pongs=4, pong_at=time.monotonic()-94, last={}))
    _feed(m, spare)
    assert not m.backup_ready(spare)
    assert 'stale_pong' in m.readiness_blockers(spare)
    spare['socket'].transport.pong_at = time.monotonic()
    _feed(m, spare)
    assert m.backup_ready(spare)


def test_selection_prefers_lowest_queue_and_rechecks_after_promotion_pressure():
    m, primary, spare, *_ = _wired()
    _feed(m, spare)
    other = dict(spare, source='reserve', inbox=observer.ReceiveQueue(16, 64))
    other['inbox'].queue_wait_seconds = .11
    spare['inbox'].queue_wait_seconds = .8
    m.reserves[(0, 2)] = other
    assert m.best_backup(0) is other
    primary['inbox'].queue_wait_seconds = 2
    now = time.monotonic()
    assert not m.primary_pressure(0, now)
    assert m.primary_pressure(0, now+1.01)
    assert primary['failure_reason'] == 'ingress_pressure'
    other['socket'] = SimpleNamespace(transport=SimpleNamespace(pongs=1, pong_at=now-94, last={}))
    assert m.best_backup(0) is spare


def test_configured_five_spares_are_reachable_without_overwriting_reserves(feed_manager, monkeypatch):
    m, _, sockets = feed_manager
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_HOT_BACKUP_COUNT', '5')
    make_hot(m)
    for n in range(5):
        m.backup_next_connect_at = 0
        for _, spare in m.backup_items():
            spare['initial_sent_at'] = time.monotonic()-11
        m.tick()
        wait_for(lambda: len(m.backup_items()) == n+1)
        wait_for(lambda: all(c.get('initial_sent_at') is not None for _, c in m.backup_items()))
    assert len(sockets) == 6 and m.backup_target() == 5
    removed = m.backups[0]
    m.remove_backup(0, removed)
    m.retire(removed)
    assert len(m.backup_items()) == 4 and m.backups[0] is not removed


def test_six_live_sources_preserve_identical_trade_print_multiplicity():
    from app.services.polymarket.ws_redundancy import FeedMerge
    merge = FeedMerge({})
    event = dict(event_type='last_trade_price',asset_id='a',timestamp=1000,price='.5',size='1')
    sources = ['primary','b1','b2','b3','b4','b5']
    published = []
    for _ in range(2):
        for source in sources:
            result = merge.filter(event,source,{'a'})
            if result:
                published.append(result)
    assert published == [event,event]

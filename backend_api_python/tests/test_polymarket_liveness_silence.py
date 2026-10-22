"""Liveness spare promotes over a byte-silent primary (PR A, 2026-10-20 09:36 ETH15m)."""
import time
from unittest.mock import patch
from datetime import datetime, timezone
import pytest

from app.services.polymarket import observer
from tests.test_polymarket_ws_fix_b import _liveness_manager, _price_change


def _wired(monkeypatch=None):
    manager, primary, backup, shadow = _liveness_manager()
    snaps, retired = [], []
    manager.request_snapshot = lambda index, state, assets, pressure=False: snaps.append(list(assets)) or True
    manager.retire = lambda state: retired.append(state)
    manager.redundancy_tick = lambda now: None
    manager.update_hot_coverage = lambda now: None
    manager._arm_book_fallback = lambda index, state: None
    manager.check_continuity = lambda *a, **k: None
    primary['subscribe_initial'] = False
    return manager, primary, backup, snaps, retired


def _feed(manager, backup):
    backup['inbox'].last_data_at = time.monotonic()
    manager.liveness_receive(0, backup, _price_change('ay', 'an'), datetime.now(timezone.utc), backup['inbox'])


def _sustained_feed(manager, backup):
    """Two newer spare envelopes span a second while primary bytes stay flat."""
    now = time.monotonic()
    with patch.object(observer.time, 'monotonic', return_value=now-1):
        _feed(manager, backup)
    with patch.object(observer.time, 'monotonic', return_value=now-.5):
        _feed(manager, backup)
    _feed(manager, backup)


def test_silent_primary_promotes_a_receiving_spare_within_the_window():
    manager, primary, backup, snaps, retired = _wired()
    primary['inbox'].last_byte_at = time.monotonic()
    _feed(manager, backup)
    assert manager.states[0] is primary and manager.backup_promotions == 0
    # 5s of primary byte silence (no data, no PONG) while the spare keeps reading.
    primary['inbox'].last_byte_at = time.monotonic() - 5
    started = time.monotonic()
    _sustained_feed(manager, backup)
    assert time.monotonic() - started < 1
    assert manager.states[0] is backup and manager.backup_promotions == 1 and retired == [primary]
    assert primary['data_suspect'] is True and primary['failure_reason'] == 'byte_silence'
    assert backup['need_book'] == {'ay', 'an'} and snaps == [['ay', 'an']]
    row = manager.liveness_silence['up_down:BTC:5m']
    assert row['promotions'] == 1 and row['suppressed'] == 0 and 4.9 <= row['last_silence_s'] < 6


def test_short_silence_does_not_promote():
    manager, primary, backup, *_ = _wired()
    primary['inbox'].last_byte_at = time.monotonic() - 3.5
    _feed(manager, backup)
    assert manager.states[0] is primary and not primary.get('data_suspect')
    assert manager.liveness_silence == {}


def test_quiet_market_silences_both_and_never_promotes():
    manager, primary, backup, *_ = _wired()
    _feed(manager, backup)
    # Both quiet: the spare's last envelope is older than the primary's last byte.
    backup['liveness_at'] = time.monotonic() - 10
    backup['inbox'].last_data_at = time.monotonic() - 10
    primary['inbox'].last_byte_at = time.monotonic() - 6
    assert manager._liveness_byte_silence(0, backup) is False
    # Spare envelope newer than the primary byte but older than 1s: no.
    backup['liveness_at'] = time.monotonic() - 2
    assert manager._liveness_byte_silence(0, backup) is False
    assert manager.states[0] is primary and not primary.get('data_suspect')


def test_spare_received_nothing_newer_than_the_primary():
    manager, primary, backup, *_ = _wired()
    _feed(manager, backup)
    primary['inbox'].last_byte_at = time.monotonic() - 5
    # Envelope processed now but received before the primary went quiet (backlogged spare).
    backup['inbox'].last_data_at = time.monotonic() - 6
    backup['liveness_at'] = time.monotonic()
    assert manager._liveness_byte_silence(0, backup) is False
    assert manager.states[0] is primary


def test_unhealthy_spare_does_not_promote():
    manager, primary, backup, *_ = _wired()
    _feed(manager, backup)
    backup['inbox'].delivery_gap = True
    primary['inbox'].last_byte_at = time.monotonic() - 6
    _feed(manager, backup)
    assert manager.states[0] is primary and not primary.get('data_suspect')
    assert manager.backup_promotions == 0


def test_transport_reads_count_as_bytes():
    manager, primary, backup, *_ = _wired()
    _feed(manager, backup)
    primary['inbox'].last_byte_at = time.monotonic() - 6

    class _T:
        last_read_at = time.monotonic()

    class _S:
        transport = _T()
    primary['socket'] = _S()
    _feed(manager, backup)
    assert manager.states[0] is primary and not primary.get('data_suspect')


def test_new_primary_is_protected_even_within_previous_primary_cooldown():
    manager, primary, backup, *_ = _wired()
    _feed(manager, backup)
    primary['inbox'].last_byte_at = time.monotonic() - 5
    _sustained_feed(manager, backup)
    assert manager.states[0] is backup
    # A new primary/spare pair in the same group goes silent 10s later.
    manager2, primary2, backup2, *_ = _wired()
    manager2.liveness_silence = manager.liveness_silence
    _feed(manager2, backup2)
    primary2['inbox'].last_byte_at = time.monotonic() - 5
    _sustained_feed(manager2, backup2)
    assert manager2.states[0] is backup2 and primary2.get('data_suspect')
    row = manager2.liveness_silence['up_down:BTC:5m']
    assert row['promotions'] == 2 and row['suppressed'] == 0
    # The failed physical generation cannot trigger a duplicate promotion.
    _feed(manager2, backup2)
    assert manager2.states[0] is backup2 and row['promotions'] == 2


def test_status_reports_silence_counters_without_private_fields():
    manager, primary, backup, *_ = _wired()
    _feed(manager, backup)
    primary['inbox'].last_byte_at = time.monotonic() - 5
    _sustained_feed(manager, backup)
    shown = {k: {f: v for f, v in row.items() if not f.startswith('_')} for k, row in manager.liveness_silence.items()}
    assert set(shown['up_down:BTC:5m']) == {'promotions', 'suppressed', 'last_at', 'last_silence_s',
        'reason', 'tcp_pending_bytes', 'queue_wait_seconds', 'remote_silence_confirmed'}


@pytest.mark.parametrize('copy_delay', [0, .1, .3, .5, .8])
@pytest.mark.parametrize('pong_age', [1, 6, 9])
def test_first_spare_quote_after_natural_quiet_does_not_retire_healthy_primary(monkeypatch, copy_delay, pong_age):
    clock = [100.0]
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    manager, primary, backup, _, retired = _wired()
    primary['inbox'].last_byte_at = clock[0]-pong_age
    _feed(manager, backup)
    assert manager.states[0] is primary and not retired
    clock[0] += copy_delay
    primary['inbox'].last_byte_at = clock[0]  # Its normal copy finally arrives.
    _feed(manager, backup)
    assert manager.states[0] is primary and manager.backup_promotions == 0


def test_a_primary_byte_resets_peer_evidence_instead_of_using_an_old_quiet_episode(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    m, primary, spare, *_ = _wired()
    primary['inbox'].last_byte_at = 94
    _feed(m, spare)
    clock[0] = 100.5
    primary['inbox'].last_byte_at = clock[0]
    _feed(m, spare)
    clock[0] = 106
    spare['socket'].transport.pong()
    _feed(m, spare)
    assert m.states[0] is primary
    for at in (106.5, 106.99):
        clock[0] = at
        _feed(m, spare)
        assert m.states[0] is primary
    clock[0] = 107.01
    _feed(m, spare)
    assert m.states[0] is spare


def test_separated_spare_quotes_do_not_accumulate_as_continuous_failure_evidence(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    m, primary, spare, *_ = _wired()
    primary['inbox'].last_byte_at = 94
    for at in (100, 102, 104):
        clock[0] = at
        _feed(m, spare)
        assert m.states[0] is primary


def test_simultaneous_spare_checks_record_one_physical_failure(monkeypatch):
    import threading
    clock = [100.0]
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    m, primary, spare, *_ = _wired()
    primary['inbox'].last_byte_at = 94
    _feed(m, spare)
    clock[0] = 100.5
    _feed(m, spare)
    clock[0] = spare['liveness_at'] = spare['inbox'].last_data_at = 101
    actual_reason = m.silence_reason
    def reason(state):
        time.sleep(.01)  # TCP evidence collection can release the GIL too.
        return actual_reason(state)
    monkeypatch.setattr(m, 'silence_reason', reason)
    barrier = threading.Barrier(2)
    results = []
    def check():
        barrier.wait()
        results.append(m._liveness_byte_silence(0, spare))
    threads = [threading.Thread(target=check) for _ in range(2)]
    for thread in threads:thread.start()
    for thread in threads:thread.join(1)
    assert sorted(results) == [False, True]
    assert m.liveness_silence['up_down:BTC:5m']['promotions'] == 1
    assert m.primary_failures['up_down:BTC:5m']['total'] == 1

import time
from app.services.polymarket import observer
from tests.test_polymarket_liveness_silence import _wired, _sustained_feed


def test_recvq_stall_is_ingress_pressure_and_can_promote(monkeypatch):
    m, primary, spare, *_ = _wired()
    monkeypatch.setattr(observer, 'socket_pending_bytes', lambda _: 700000)
    primary['inbox'].last_byte_at = time.monotonic()-27
    _sustained_feed(m, spare)
    assert m.states[0] is spare
    assert primary['failure_reason'] == 'ingress_pressure'
    evidence = m.liveness_silence['up_down:BTC:5m']
    assert evidence['tcp_pending_bytes'] == 700000 and not evidence['remote_silence_confirmed']


def test_promotion_transfers_pending_business_gap_instead_of_reporting_zero():
    m, primary, spare, *_ = _wired()
    primary.update(failure_recorded=True, outage_at=time.monotonic()-25, outage_wall='wall',
                   outage_events=[dict(merged_gap_ms=None, merged_gap_final=False)])
    primary['inbox'].last_byte_at = time.monotonic()-27
    _sustained_feed(m, spare)
    assert m.states[0] is spare and len(spare['outage_events']) == 1
    assert spare['outage_events'][0]['merged_gap_ms'] is None
    assert not spare['outage_events'][0]['merged_gap_final']

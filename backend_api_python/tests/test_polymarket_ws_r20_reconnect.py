from app.services.polymarket.ws_rotation import reconnect_delay
from app.services.polymarket.ws_redundancy import DisconnectClusters


def test_retries_prioritize_hot_then_cold_then_spares_and_back_off(monkeypatch):
    monkeypatch.setattr('app.services.polymarket.ws_rotation.random.uniform', lambda low, high: high/2)
    hot = [reconnect_delay(1, 'primary', n) for n in range(4)]
    cold = reconnect_delay(1, 'cold')
    spare = reconnect_delay(1, 'backup')
    assert max(hot) < cold < spare
    assert len(set(hot)) == 4
    assert spare < reconnect_delay(3, 'backup') < reconnect_delay(6, 'backup') <= 30


def test_global_disconnect_detects_fourteen_cross_process_closes_once():
    tracker = DisconnectClusters()
    rows = [dict(at=1000+n*.00001, source=f's{n}', feed=f'f{n%5}', code=None,
                 groups=[f'g{n%4}']) for n in range(14)]
    result = tracker.update(rows, 1001)
    assert result['count_rolling_hour'] == 1 and result['events'][0]['connections'] == 14
    assert tracker.update(rows, 1002)['count_rolling_hour'] == 1
    assert not result['cause_confirmed']
    assert DisconnectClusters().update([dict(r, code=1013) for r in rows], 1001)['count_rolling_hour'] == 0

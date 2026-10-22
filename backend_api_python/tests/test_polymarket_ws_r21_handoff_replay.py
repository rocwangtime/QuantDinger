import importlib.util
from pathlib import Path


def test_real_worker_handoff_with_silent_primary_uses_warm_book_without_rest():
    script = Path(__file__).parents[1] / 'scripts/polymarket_ws_handoff_replay.py'
    spec = importlib.util.spec_from_file_location('r21_handoff_replay', script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.replay(silence=.1, rest_delay=.1)
    assert result['valid'] and result['ready_backups'] == 2 and result['code'] is None
    assert result['rest_requests'] == 0 and not result['need_book_seen']
    assert result['event']['recovery'] == 'warm_backup'
    assert result['event']['merged_gap_ms'] < 1000


def test_replay_pong_is_a_received_byte_and_updates_the_actual_callback():
    from app.services.polymarket.ws_group_process import ReplaySocket
    from app.services.polymarket.ws_transport import TransportDiagnostics
    transport = TransportDiagnostics()
    received = []
    socket = ReplaySocket('offline', on_message=lambda sock, raw: (received.append(raw), sock.transport.pong()))
    socket.transport = transport
    socket.send('PING')
    assert received == ['PONG'] and transport.pongs == 1
    assert transport.last_read_at is not None
    last_read = transport.last_read_at
    socket.stall(1)
    socket.send('PING')
    assert received == ['PONG'] and transport.last_read_at == last_read


def test_ready_copy_still_receives_when_another_copy_is_not_connected(monkeypatch):
    import threading
    import time
    from types import SimpleNamespace
    from app.services.polymarket import ws_group_process as group
    received = []
    socket = SimpleNamespace(stopped=threading.Event(), emit=received.append)
    manager = SimpleNamespace(backup_items=lambda: [(0, {'socket': None}), (0, {'socket': socket})])
    monkeypatch.setattr(group, '_schedule', lambda spec: [(0, 'backup', (0, 0, ['ay', 'an'], 2, 1792485000000))])
    group._drive(manager, dict(start_mono=time.monotonic()))
    assert len(received) == 1 and 'ay' in received[0] and 'an' in received[0]


def test_cold_replay_bootstraps_without_waiting_for_nonexistent_hot_copies(monkeypatch):
    import threading
    from types import SimpleNamespace
    from app.services.polymarket import ws_group_process as group
    received = []
    socket = SimpleNamespace(assets=['cold'], emit=received.append)
    manager = SimpleNamespace(states={0: {'socket': socket}}, backup_items=lambda: [],
        hot_shard=lambda index: False, backup_target=lambda: 2, tick=lambda **kwargs: None)
    monkeypatch.setattr(group, '_STOP_CHILD', threading.Event())
    # Any wait indicates it is incorrectly trying to build hot copies for cold.
    monkeypatch.setattr(group.time, 'sleep', lambda delay: (_ for _ in ()).throw(AssertionError('unexpected wait')))
    assert group._bootstrap(manager) and len(received) == 1

import time
from app.services.polymarket.ws_group_process import ReplaySocket, _replay_stall_seconds
def test_stall_drops_frames_and_pongs(monkeypatch):
    got = []
    sock = ReplaySocket('x', on_message=lambda s, raw: got.append(raw))
    sock.emit('a'); sock.send('PING')
    pongs = sock.transport.pongs
    sock.stall(0.2)
    sock.emit('b'); sock.send('PING')
    assert got == ['a'] and sock.transport.pongs == pongs
    time.sleep(0.25)
    sock.emit('c')
    assert got == ['a', 'c']
    monkeypatch.setenv('QD_REPLAY_KICK_MODE', 'stall:30')
    assert _replay_stall_seconds() == 30
    monkeypatch.setenv('QD_REPLAY_KICK_MODE', '')
    assert _replay_stall_seconds() == 0

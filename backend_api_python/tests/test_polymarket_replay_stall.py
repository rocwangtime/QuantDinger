import time
from app.services.polymarket.ws_group_process import ReplaySocket, _replay_stall_seconds
def test_stall_drops_frames_and_pongs(monkeypatch):
    got = []
    def received(sock, raw):
        got.append(raw)
        if raw == 'PONG':
            sock.transport.pong()
    sock = ReplaySocket('x', on_message=received)
    sock.emit('a'); sock.send('PING')
    pongs = sock.transport.pongs
    sock.stall(0.2)
    sock.emit('b'); sock.send('PING')
    assert got == ['a', 'PONG'] and sock.transport.pongs == pongs == 1
    time.sleep(0.25)
    sock.emit('c')
    assert got == ['a', 'PONG', 'c']
    monkeypatch.setenv('QD_REPLAY_KICK_MODE', 'stall:30')
    assert _replay_stall_seconds() == 30
    monkeypatch.setenv('QD_REPLAY_KICK_MODE', '')
    assert _replay_stall_seconds() == 0

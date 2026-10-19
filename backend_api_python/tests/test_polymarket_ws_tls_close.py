"""Cross-thread close must not free a TLS fd the reader still owns.

Production saw SSL WRONG_VERSION_NUMBER and DECRYPTION_FAILED_OR_BAD_RECORD_MAC
right after a BTC 5m primary 1013 with no ready backup: the backup was promoted
and the new handshake reused the fd that retire / PONG timeout / the heartbeat
had already closed out from under the reader.
"""
import socket
import threading
from types import SimpleNamespace

import websocket

from app.services.polymarket import observer
from app.services.polymarket.ws_transport import TransportDiagnostics


def _reusable(target):
    """True when `target` is already closed, so the next sockets can take it."""
    held = []
    try:
        for _ in range(512):
            fresh = socket.socket()
            held.append(fresh)
            number = fresh.fileno()
            if number == target:
                return True
            if number > target:
                return False
        return False
    finally:
        for item in held:
            item.close()


class _Raw:
    """Underlying fd. shutdown wakes a blocked recv; close is what frees the number."""

    def __init__(self, sock):
        self.sock = sock
        self.closed_by = []

    def fileno(self):
        return self.sock.fileno()

    def shutdown(self, how):
        self.sock.shutdown(how)

    def close(self):
        if self.closed_by:
            return
        self.closed_by.append(threading.current_thread().name)
        self.sock.close()

    def recv(self, size):
        self.entered.set()
        return self.sock.recv(size)


class _Wire:
    """websocket-client WebSocket: shutdown() closes the fd and drops it."""

    def __init__(self, raw):
        self.sock = raw
        self.connected = True

    def shutdown(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None
        self.connected = False

    def close(self, **kwargs):
        self.shutdown()


class _App:
    def __init__(self, raw):
        self.sock = _Wire(raw)
        self.keep_running = True
        self.stop_ping = threading.Event()

    def close(self, **kwargs):
        self.keep_running = False
        if self.sock is not None:
            self.sock.close()
            self.sock = None


def _blocked_reader():
    left, right = socket.socketpair()
    right.close()
    raw = _Raw(left)
    raw.entered = threading.Event()
    release = threading.Event()

    def run():
        try:
            raw.recv(1)
        except OSError:
            pass
        release.wait(2)
        raw.close()

    thread = threading.Thread(target=run, name='polymarket-receive')
    thread.start()
    assert raw.entered.wait(2)
    return raw, _App(raw), left.fileno(), release, thread


def test_foreign_close_lets_the_next_socket_reuse_the_fd():
    """The failure mode: another thread's close frees the number immediately."""
    left, right = socket.socketpair()
    right.close()
    number = left.fileno()
    left.close()
    assert _reusable(number)


def test_retire_keeps_the_fd_until_the_reader_closes_it():
    raw, app, number, release, thread = _blocked_reader()
    try:
        manager = observer.MarketConnections()
        manager.retire(dict(socket=app, inbox=None))
        assert raw.closed_by == []
        assert _reusable(number) is False
        assert app.keep_running is False
    finally:
        release.set()
        thread.join(2)
    assert raw.closed_by == ['polymarket-receive']


def test_pong_timeout_keeps_the_fd_until_the_reader_closes_it():
    raw, app, number, release, thread = _blocked_reader()
    try:
        manager = observer.MarketConnections()
        manager.record_primary_failure = lambda *args, **kwargs: None
        transport = TransportDiagnostics()
        transport.pending_ping_at = 0
        transport.last_read_at = 0
        app.transport = transport
        inbox = SimpleNamespace(last_byte_at=0, invalid=threading.Event())
        state = dict(connected=True, socket=app, inbox=inbox, assets=[], redundant=False)
        manager.states[0] = state
        manager.pong_checked_at = 0
        manager.pong_timeout_tick(100)
        assert raw.closed_by == []
        assert _reusable(number) is False
        assert state['pong_dead'] is True
    finally:
        release.set()
        thread.join(2)
    assert raw.closed_by == ['polymarket-receive']


def test_heartbeat_error_shuts_the_socket_down_without_closing_it():
    raw, app, number, release, thread = _blocked_reader()
    try:
        live = observer.read_ahead_app(websocket.WebSocketApp)('ws://127.0.0.1')
        live.sock = app.sock
        live.keep_running = True
        live.ping_interval = 0
        live.subscription_ready.set()
        live.stop_ping = SimpleNamespace(wait=lambda _seconds: False)
        live.on_error = lambda _app, error: None

        def fail(_raw):
            raise OSError('send failed')

        live.sock.send = fail
        live._send_ping()
        assert raw.closed_by == []
        assert _reusable(number) is False
        assert live.keep_running is False
    finally:
        release.set()
        thread.join(2)


def test_retired_heartbeat_does_not_send_or_close():
    sent, closed = [], []
    app = observer.read_ahead_app(websocket.WebSocketApp)('ws://127.0.0.1')
    raw = SimpleNamespace(shutdown=lambda how: None, close=lambda: closed.append(True))
    app.sock = SimpleNamespace(send=sent.append, sock=raw)
    app.keep_running = True
    app.ping_interval = 0
    app.generation_retired = True
    app.subscription_ready.set()
    calls = []

    def wait(_seconds):
        calls.append(1)
        return len(calls) > 1

    app.stop_ping = SimpleNamespace(wait=wait)
    app._send_ping()
    assert sent == []
    assert closed == []

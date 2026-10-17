"""TCP evidence, independent text heartbeats and remote-close preservation."""
import io
import json
import socket
import struct
import threading
from types import SimpleNamespace

import pytest
import websocket

from app.services.polymarket import observer, ws_transport


def test_linux_tcp_info_offsets_and_short_kernel_struct(monkeypatch):
    data = bytearray(136)
    data[6] = 0x7d
    for offset, fmt, value in [(64, 'I', 456789), (68, 'I', 12345), (96, 'I', 98765),
                               (100, 'I', 9), (128, 'Q', 1 << 34)]:
        struct.pack_into('=' + fmt, data, offset, value)

    class Socket:
        def getsockname(self):
            return ('127.0.0.1', 4321)
        def getsockopt(self, level, option, *size):
            if option == socket.TCP_INFO:
                return bytes(data)
            return 1 if option == socket.SO_KEEPALIVE else 524288
        def fileno(self):
            return -1  # ioctl unavailable, but TCP_INFO evidence is retained.
    app = SimpleNamespace(sock=SimpleNamespace(sock=Socket()))
    monkeypatch.setattr(ws_transport.sys, 'platform', 'linux')
    metrics = ws_transport.socket_metrics(app)
    assert metrics == dict(local_port=4321, rcvbuf_bytes=524288, keepalive=True,
                           rcv_ssthresh=456789, rtt_us=12345, rcv_space=98765,
                           total_retrans=9, tcp_bytes_received=1 << 34, snd_wscale=13, rcv_wscale=7, extensions='')
    del data[104:]
    assert 'tcp_bytes_received' not in ws_transport.socket_metrics(app)
    assert ws_transport.socket_metrics(None) == {}


def test_socket_metrics_real_loopback_receive_backlog():
    server = socket.socket()
    client = socket.socket()
    accepted = None
    try:
        server.bind(('127.0.0.1', 0))
        server.listen()
        client.connect(server.getsockname())
        accepted, _ = server.accept()
        accepted.sendall(b'x' * 4096)
        # MSG_PEEK waits until data arrives without draining the receive queue.
        assert client.recv(1, socket.MSG_PEEK) == b'x'
        metrics = ws_transport.socket_metrics(SimpleNamespace(sock=SimpleNamespace(sock=client)))
        assert metrics['tcp_pending_bytes'] == 4096
        assert metrics['tcp_bytes_received'] >= 4096
        assert metrics['rcvbuf_bytes'] > 0
        assert metrics['local_port'] == client.getsockname()[1]
    finally:
        if accepted:
            accepted.close()
        client.close()
        server.close()


def test_transport_history_is_bounded_and_measures_actual_reads_and_scheduling(monkeypatch):
    monkeypatch.setattr(ws_transport, 'socket_metrics', lambda app: {'tcp_pending_bytes': 4})
    probe = ws_transport.TransportDiagnostics()
    app = SimpleNamespace(read_bytes=100, read_calls=1)
    probe.sample(app, 100)
    app.read_bytes, app.read_calls = 6100, 5
    assert probe.sample(app, 103)['read_bytes_per_second'] == 2000
    assert probe.last['sample_gap_seconds'] == 3
    assert probe.last['scheduler_lag_seconds'] == 2
    for now in range(104, 180):
        probe.sample(app, now)
    assert len(probe.samples) == 60
    probe.ping_at = 100
    monkeypatch.setattr(ws_transport.time, 'monotonic', lambda: 100.025)
    probe.pong()
    assert probe.pong_rtt_seconds == pytest.approx(.025)
    assert probe.pongs == 1
    json.dumps(list(probe.samples))


def test_rcvbuf_defaults_to_autotuning_and_override_is_opt_in(monkeypatch):
    monkeypatch.delenv('POLYMARKET_OBSERVER_WS_RCVBUF_BYTES', raising=False)
    assert ws_transport.receive_buffer_options() == ()
    for value in ['0', '-10', 'invalid']:
        monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RCVBUF_BYTES', value)
        assert ws_transport.receive_buffer_options() == ()
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_RCVBUF_BYTES', '1048576')
    assert ws_transport.receive_buffer_options() == ((socket.SOL_SOCKET, socket.SO_RCVBUF, 1048576),)


def test_text_heartbeat_and_sampling_run_without_market_tick(monkeypatch):
    clock = [100]
    sent, samples = [], []
    app = observer.read_ahead_app(websocket.WebSocketApp)('ws://127.0.0.1')
    app.keep_running = True
    app.ping_interval = 10
    app.sock = SimpleNamespace(send=sent.append)
    app.subscription_ready.set()
    class Stop:
        def wait(self, seconds):
            assert seconds == 1
            clock[0] += seconds
            return clock[0] > 121
    app.stop_ping = Stop()
    monkeypatch.setattr(observer.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(app.transport, 'sample', lambda app, now: samples.append(now))
    app._send_ping()  # No manager.tick(), discovery or database completion.
    assert sent == ['PING', 'PING']
    assert samples == list(range(101, 122))
    assert app.transport.pings == 2
    assert app.text_heartbeat_active is False


def test_text_heartbeat_send_failure_wakes_reader(monkeypatch):
    app = observer.read_ahead_app(websocket.WebSocketApp)('ws://127.0.0.1')
    app.keep_running, app.ping_interval = True, 0
    app.stop_ping = SimpleNamespace(wait=lambda _: False)
    errors, shutdowns = [], []
    app.on_error = lambda app, error: errors.append(str(error))
    def fail(raw):
        raise OSError('send failed')
    app.sock = SimpleNamespace(send=fail, shutdown=lambda: shutdowns.append(True))
    app.subscription_ready.set()
    app._send_ping()
    assert errors == ['send failed']
    assert shutdowns == [True]
    assert app.text_heartbeat_active is False


def test_received_1013_preserves_code_reason_and_failure_evidence(monkeypatch, caplog):
    monkeypatch.setattr(observer, '_STOP', threading.Event())
    monkeypatch.setattr(observer, '_PIPELINE', None)
    monkeypatch.setattr(observer, 'STATUS', dict(reconnect_count=0, slow_consumer_count=0))
    monkeypatch.setenv('POLYMARKET_OBSERVER_WS_SHARDS', '1')
    # Real WebSocketApp callback dispatch, including the library's ABNF error path.
    manager = observer.MarketConnections()
    monkeypatch.setattr(threading.Thread, 'start', lambda self: None)
    manager.sync([dict(id='a', yesAssetId='ay', noAssetId='an')])
    state = manager.states[0]
    app = state['socket']
    frame = websocket.ABNF(fin=1, opcode=8, data=struct.pack('!H', 1013) + b'slow consumer: send buffer full')
    with caplog.at_level('INFO'):
        app.on_error(app, frame)
        # Library teardown clears the socket and passes no close args.
        app.sock = None
        app.on_close(app, None, None)
        manager.tick()
    assert observer.STATUS['slow_consumer_count'] == 1
    assert observer.STATUS['last_close_code'] == 1013
    assert observer.STATUS['last_close_reason'] == 'slow consumer: send buffer full'
    assert 'code=1013 slow_consumer=True' in caplog.text
    evidence = observer.STATUS['ws_last_transport_failures'][0]
    assert evidence['code'] == 1013
    assert evidence['assets'] == 2
    assert evidence['sample']['at']
    assert 'transport failure shard=0' in caplog.text
    assert ws_transport.remote_close(RuntimeError('network')) == (None, None)


def test_monitor_timeout_kills_descendants_and_next_sample_runs(tmp_path):
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location('p34_monitor', Path(__file__).resolve().parents[2] / 'deploy/p34_monitor.py')
    monitor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(monitor)
    script = tmp_path / 'script.sh'
    marker = tmp_path / 'leaked'
    # Parent and child both ignore TERM, child would write after timeout.
    script.write_text(f"trap '' TERM\n(sleep .4; touch '{marker}') &\nwait\n")
    metadata = io.StringIO()
    with (tmp_path / 'out').open('w') as output:
        assert monitor.run_sample(['bash'], script, output, .1, metadata) == 124
        script.write_text('echo recovered\n')
        assert monitor.run_sample(['bash'], script, output, 2, metadata) == 0
    threading.Event().wait(.5)
    assert not marker.exists()
    assert 'recovered' in (tmp_path / 'out').read_text()
    assert '"event": "timeout"' in metadata.getvalue()


def test_monitor_slow_lane_does_not_block_tcp_and_reports_missed_slots(tmp_path):
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location('p34_monitor', Path(__file__).resolve().parents[2] / 'deploy/p34_monitor.py')
    monitor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(monitor)
    script = tmp_path / 'script.sh'
    script.write_text('sleep 30\n')
    fast = tmp_path / 'fast.sh'
    fast.write_text('echo tcp\n')
    stop = threading.Event()
    slow = threading.Thread(target=monitor.lane, args=('observer', ['bash'], script, tmp_path, .05, .15, .4, stop))
    slow.start()
    monitor.lane('tcp', ['bash'], fast, tmp_path, .05, .1, .4, stop)
    slow.join(1)
    assert not slow.is_alive()
    assert (tmp_path / 'tcp.log').read_text().count('tcp') >= 3
    assert '"event": "missed"' in (tmp_path / 'observer_heartbeat.jsonl').read_text()
    assert '"event": "done"' in (tmp_path / 'observer_heartbeat.jsonl').read_text()


def test_real_websocket_text_ping_pong_and_remote_close_lifecycle(monkeypatch):
    import base64
    import hashlib
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen(1)
    wire, errors, closes = [], [], []
    done, pong = threading.Event(), threading.Event()

    def exact(conn, size):
        data = b''
        while len(data) < size:
            chunk = conn.recv(size - len(data))
            if not chunk:
                raise EOFError('client closed')
            data += chunk
        return data

    def serve():
        try:
            conn, _ = server.accept()
            with conn:
                conn.settimeout(4)
                request = b''
                while b'\r\n\r\n' not in request:
                    request += conn.recv(4096)
                assert b'permessage-deflate' not in request
                key = next(line.split(b':', 1)[1].strip() for line in request.split(b'\r\n')
                           if line.lower().startswith(b'sec-websocket-key'))
                accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
                conn.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                             b'Sec-WebSocket-Accept: ' + accept + b'\r\n\r\n')
                for _ in range(2):
                    opcode, size = exact(conn, 2)
                    assert size & 128
                    mask = exact(conn, 4)
                    payload = exact(conn, size & 127)
                    payload = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
                    wire.append((opcode & 15, payload))
                conn.sendall(b'\x81\x04PONG')
                assert pong.wait(2)
                reason = struct.pack('!H', 1013) + b'slow consumer: send buffer full'
                conn.sendall(bytes([0x88, len(reason)]) + reason)
                exact(conn, 2)
        except Exception as exc:
            errors.append(exc)
        finally:
            done.set()

    def receive(app, raw):
        if raw == b'PONG':
            app.transport.pong()
            pong.set()
    def opened(app):
        app.send('{"assets_ids":["a"],"type":"market"}')
        app.subscription_ready.set()
    app = observer.read_ahead_app(websocket.WebSocketApp)(
        f'ws://127.0.0.1:{server.getsockname()[1]}', on_message=receive,
        on_open=opened,
        on_error=lambda app, error: closes.append(ws_transport.remote_close(error)))
    serving = threading.Thread(target=serve, daemon=True)
    serving.start()
    client = threading.Thread(target=lambda: app.run_forever(ping_interval=.1, skip_utf8_validation=True), daemon=True)
    client.start()
    try:
        assert done.wait(5)
        client.join(3)
        assert not client.is_alive()
        assert not errors
        assert wire == [(1, b'{"assets_ids":["a"],"type":"market"}'), (1, b'PING')]
        assert app.transport.pongs == 1
        assert app.transport.pong_rtt_seconds >= 0
        assert closes == [(1013, 'slow consumer: send buffer full')]
        assert not app.text_heartbeat_active
        assert app.ping_thread is None
    finally:
        app.close()
        server.close()
        serving.join(1)
        client.join(1)

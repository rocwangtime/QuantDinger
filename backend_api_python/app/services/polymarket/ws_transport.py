"""Bounded public WS transport evidence; no book, balancer or database locks."""
import socket
import struct
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone


def socket_metrics(app):
    result = {}
    try:
        sock = app.sock.sock
        result.update(local_port=sock.getsockname()[1],
                      rcvbuf_bytes=sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
                      keepalive=bool(sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)))
        headers = app.sock.getheaders() if hasattr(app.sock, 'getheaders') else {}
        result['extensions'] = (headers or {}).get('sec-websocket-extensions', '')
        if sys.platform == 'linux':
            info = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 256)
            # Linux UAPI struct tcp_info; trailing fields depend on kernel version.
            for name, offset, fmt in [('rtt_us', 68, 'I'), ('rcv_ssthresh', 64, 'I'),
                                      ('rcv_space', 96, 'I'), ('total_retrans', 100, 'I'),
                                      ('tcp_bytes_received', 128, 'Q'), ('rcv_ooopack', 224, 'I'),
                                      ('snd_wnd', 228, 'I'), ('rcv_wnd', 232, 'I')]:
                if len(info) >= offset + struct.calcsize(fmt):
                    result[name] = struct.unpack_from('=' + fmt, info, offset)[0]
            if len(info) >= 7:
                result.update(snd_wscale=info[6] & 15, rcv_wscale=info[6] >> 4)
            import array
            import fcntl
            import termios
            pending = array.array('i', [0])
            fcntl.ioctl(sock.fileno(), termios.FIONREAD, pending, True)
            result['tcp_pending_bytes'] = pending[0]
    except (AttributeError, OSError, ValueError, TypeError, IndexError):
        pass
    return result


class TransportDiagnostics:
    def __init__(self):
        self.lock = threading.RLock()
        self.samples = deque(maxlen=60)
        self.last_at = None
        self.read_bytes = 0
        self.pings = self.pongs = 0
        self.ping_at = self.pong_at = None
        self.pong_rtt_seconds = None
        self.last = {}

    def sample(self, app, now=None):
        with self.lock:
            return self._sample(app, now)

    def _sample(self, app, now=None):
        now = time.monotonic() if now is None else now
        metrics = socket_metrics(app)
        total = getattr(app, 'read_bytes', 0)
        elapsed = now - self.last_at if self.last_at is not None else None
        metrics.update(at=datetime.now(timezone.utc).isoformat(),
                       sample_gap_seconds=elapsed,
                       scheduler_lag_seconds=max(0, elapsed - 1) if elapsed is not None else 0,
                       read_bytes=total, read_calls=getattr(app, 'read_calls', 0),
                       read_bytes_per_second=(total - self.read_bytes) / elapsed if elapsed and elapsed > 0 else None,
                       pings=self.pings, pongs=self.pongs, pong_rtt_seconds=self.pong_rtt_seconds,
                       pong_age_seconds=now - self.pong_at if self.pong_at is not None else None)
        self.last_at, self.read_bytes = now, total
        self.last = metrics
        self.samples.append(metrics)
        return metrics

    def pong(self):
        now = time.monotonic()
        self.pongs += 1
        self.pong_at = now
        self.pong_rtt_seconds = now - self.ping_at if self.ping_at is not None else None


def receive_buffer_options():
    """Leave Linux receive autotuning enabled unless explicitly overridden."""
    import os
    try:
        size = int(os.getenv('POLYMARKET_OBSERVER_WS_RCVBUF_BYTES', '0'))
    except ValueError:
        size = 0
    return ((socket.SOL_SOCKET, socket.SO_RCVBUF, min(size, 16 * 1024 * 1024)),) if size > 0 else ()


def remote_close(error):
    """websocket-client 1.9.0 passes a received close ABNF to on_error."""
    data = getattr(error, 'data', None)
    if getattr(error, 'opcode', None) == 8 and isinstance(data, bytes) and len(data) >= 2:
        return struct.unpack('!H', data[:2])[0], data[2:].decode('utf-8', errors='replace')
    return None, None

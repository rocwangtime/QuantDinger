"""Bounded public WS transport evidence; no book, balancer or database locks."""
import socket
import struct
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone


def safe_handshake_headers(app):
    """Only public diagnostics; never enumerate/log cookies or auth headers."""
    try:
        headers = app.sock.getheaders() or {}
        allowed = {'cf-ray', 'server', 'date', 'sec-websocket-extensions'}
        return {str(k).lower(): str(v)[:256] for k, v in headers.items() if str(k).lower() in allowed}
    except (AttributeError, OSError, TypeError, ValueError):
        return {}


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
        self.last_read_at = None
        self.pings = self.pongs = 0
        self.ping_at = self.pong_at = None
        self.pending_ping_at = None
        self.pong_rtt_seconds = None
        self.last = {}
        self.headers = {}
        self.read_buckets = deque(maxlen=61)
        self.burst_buckets = deque(maxlen=11)
        self.fast_samples = deque(maxlen=100)
        self.dispatch_max_seconds = 0

    def capture_headers(self, app):
        with self.lock:
            self.headers.update(safe_handshake_headers(app))

    def read(self, size, now=None):
        # One bucket per second bounds memory independently of traffic volume.
        now = time.monotonic() if now is None else now
        second = int(now)
        with self.lock:
            if size:
                self.last_read_at = now
            if self.read_buckets and self.read_buckets[-1][0] == second:
                self.read_buckets[-1][1] += size
            else:
                self.read_buckets.append([second, size])
            bucket = int(now * 10)
            if self.burst_buckets and self.burst_buckets[-1][0] == bucket:
                self.burst_buckets[-1][1] += size
            else:
                self.burst_buckets.append([bucket, size])

    def dispatch(self, seconds):
        with self.lock:
            self.dispatch_max_seconds = max(self.dispatch_max_seconds, seconds)

    def sample_fast(self, pending_bytes, queue_depth, queue_age, scheduler_lag, now=None):
        now = time.monotonic() if now is None else now
        with self.lock:
            self.fast_samples.append(dict(monotonic=now, at=datetime.now(timezone.utc).isoformat(),
                scheduler_lag_ms=max(0, scheduler_lag)*1000, tcp_pending_bytes=pending_bytes,
                queue_depth=queue_depth, queue_age_ms=max(0, queue_age)*1000,
                reader_dispatch_max_ms=self.dispatch_max_seconds*1000))
            self.dispatch_max_seconds = 0

    def fast_metrics(self, details=False):
        with self.lock:
            rows = list(self.fast_samples)
            metrics = dict(resolution_ms=100, capacity=100, samples=len(rows),
                scheduler_lag_max_ms=max((r['scheduler_lag_ms'] for r in rows), default=0),
                tcp_pending_max_bytes=max((r['tcp_pending_bytes'] or 0 for r in rows), default=0),
                queue_age_max_ms=max((r['queue_age_ms'] for r in rows), default=0),
                reader_dispatch_max_ms=max((r['reader_dispatch_max_ms'] for r in rows), default=0),
                latest=rows[-1] if rows else None)
            if details:
                metrics['samples_100ms'] = rows
            return metrics

    def ping(self, now=None):
        now = time.monotonic() if now is None else now
        with self.lock:
            self.ping_at = now
            self.pings += 1
            if self.pending_ping_at is None:
                self.pending_ping_at = now

    def pong_timed_out(self, now, last_byte_at=None, timeout=22):
        with self.lock:
            if self.pending_ping_at is None:
                return False
            last = max(at for at in (self.pending_ping_at, self.last_read_at, last_byte_at) if at is not None)
            return now - self.pending_ping_at >= timeout and now - last >= timeout

    def burst(self, now=None):
        now = time.monotonic() if now is None else now
        with self.lock:
            buckets = [(tick, size) for tick, size in self.burst_buckets if tick >= int(now * 10) - 10]
            return dict(bytes_previous_1s=sum(size for _, size in buckets), resolution_ms=100,
                        window_error_ms=100, peak_100ms_bytes=max((size for _, size in buckets), default=0),
                        buckets_100ms=buckets)

    def sample(self, app, now=None):
        with self.lock:
            return self._sample(app, now)

    def _sample(self, app, now=None):
        now = time.monotonic() if now is None else now
        metrics = socket_metrics(app)
        self.headers.update(safe_handshake_headers(app))
        metrics.update(read_bytes_60s=sum(size for second, size in self.read_buckets if second > now - 60),
                       read_window_resolution_seconds=1)
        total = getattr(app, 'read_bytes', 0)
        elapsed = now - self.last_at if self.last_at is not None else None
        metrics.update(at=datetime.now(timezone.utc).isoformat(),
                       sample_gap_seconds=elapsed,
                       scheduler_lag_seconds=max(0, elapsed - 1) if elapsed is not None else 0,
                       byte_silence_seconds=now - self.last_read_at if self.last_read_at is not None else None,
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
        with self.lock:
            self.pongs += 1
            self.pong_at = now
            self.pending_ping_at = None
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
    """Preserve both 1.9.0 ABNF closes and newer typed close exceptions."""
    code = getattr(error, 'status_code', None)
    if isinstance(code, int) and 1000 <= code < 5000 and hasattr(error, 'reason'):
        return code, error.reason
    data = getattr(error, 'data', None)
    if getattr(error, 'opcode', None) == 8 and isinstance(data, bytes) and len(data) >= 2:
        return struct.unpack('!H', data[:2])[0], data[2:].decode('utf-8', errors='replace')
    return None, None

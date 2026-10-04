import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.services.llm import LLMProvider, LLMService


def test_cancellable_openai_transport_streams_and_closes():
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            size = int(self.headers.get('Content-Length') or 0)
            payload = json.loads(self.rfile.read(size))
            assert payload['stream'] is True
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.end_headers()
            self.wfile.write(b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n')
            self.wfile.write(b'data: {"choices":[{"delta":{"content":" world"},"finish_reason":"stop"}]}\n\n')
            self.wfile.write(b'data: [DONE]\n\n')
            self.wfile.flush()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        service = object.__new__(LLMService)
        service.selection = {}
        service.get_max_tokens = lambda: 128
        service._llm_use_system_proxy = lambda: False
        service._llm_proxy_url = lambda: ''
        service._record_usage = lambda *_args: None
        stream = service._stream_openai_compatible_async(
            [{'role': 'user', 'content': 'test'}], 'gpt-test', 0.2, 'test-key',
            f'http://127.0.0.1:{server.server_port}', 5, LLMProvider.OPENAI,
        )
        assert ''.join(service._consume_cancellable_async(stream, lambda: False)) == 'hello world'
    finally:
        server.shutdown()
        server.server_close()


def test_cancellable_transport_interrupts_wait_for_response_headers():
    started_request = threading.Event()
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            started_request.set()
            time.sleep(2)
            try:
                self.send_response(200)
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                pass

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        service = object.__new__(LLMService)
        service.selection = {}
        service.get_max_tokens = lambda: 128
        service._llm_use_system_proxy = lambda: False
        service._llm_proxy_url = lambda: ''
        service._record_usage = lambda *_args: None
        stream = service._stream_openai_compatible_async(
            [{'role': 'user', 'content': 'test'}], 'gpt-test', 0.2, 'test-key',
            f'http://127.0.0.1:{server.server_port}', 5, LLMProvider.OPENAI,
        )
        began = time.monotonic()
        assert list(service._consume_cancellable_async(
            stream, lambda: started_request.is_set() and time.monotonic() - began > 0.1,
            poll_seconds=0.01,
        )) == []
        assert started_request.is_set()
        assert time.monotonic() - began < 1
    finally:
        server.shutdown()
        server.server_close()

"""Standalone deterministic mock HTTP server for shardfetch (stdlib only).

Behavior is driven solely by the trailing numeric id in the URL path,
e.g. ``/u/0007`` -> id 7, so reruns are fully deterministic:

* ids in FAIL_IDS always answer HTTP 500 (retry as often as you like);
* ids in TIMEOUT_IDS hang the connection until the *client* times out;
* every other id answers HTTP 200 after a small optional delay.

It also exposes:

* ``GET /stats``  -> JSON with ``peak_concurrency`` and per-URL counts;
* ``POST /reset`` -> reset the recorded statistics.

Run directly with ``python demo/mock_site.py`` (fixed port 8099), or
import :class:`MockSiteServer` and start it on a background thread.
"""

import json
import select
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 8099

# Deterministic failure plan used by demo/urls.txt and the tests:
# 3 permanent 500s and 2 permanent timeouts among the 60 URLs.
FAIL_IDS = frozenset({7, 23, 41})
TIMEOUT_IDS = frozenset({13, 52})


class ServerState:
    """Thread-safe in-flight peak and per-URL request counters."""

    def __init__(self):
        self._lock = threading.Lock()
        self.in_flight = 0
        self.peak_concurrency = 0
        self.request_counts = {}

    def enter(self, url):
        with self._lock:
            self.in_flight += 1
            if self.in_flight > self.peak_concurrency:
                self.peak_concurrency = self.in_flight
            self.request_counts[url] = self.request_counts.get(url, 0) + 1

    def leave(self):
        with self._lock:
            self.in_flight -= 1

    def snapshot(self):
        with self._lock:
            return {
                "in_flight": self.in_flight,
                "peak_concurrency": self.peak_concurrency,
                "request_counts": dict(self.request_counts),
            }

    def reset(self):
        with self._lock:
            self.in_flight = 0
            self.peak_concurrency = 0
            self.request_counts = {}


def trailing_id(path):
    """Extract the trailing numeric id from paths like ``/u/0007``."""
    tail = path.rstrip("/").rsplit("/", 1)[-1]
    return int(tail)


def make_handler(state, delay):
    class MockHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            return

        def do_GET(self):
            if self.path == "/stats":
                self._write_json(200, state.snapshot())
                return
            if self.path.startswith("/u/"):
                self._handle_url()
                return
            self._write_json(404, {"error": "unknown path: %s" % self.path})

        def do_POST(self):
            if self.path == "/reset":
                state.reset()
                self._write_json(200, {"reset": True})
            else:
                self._write_json(404, {"error": "unknown path: %s" % self.path})

        def _handle_url(self):
            state.enter(self.path)
            try:
                url_id = trailing_id(self.path)
                if url_id in FAIL_IDS:
                    self._write_json(500, {"id": url_id, "error": "permanent failure"})
                    return
                if url_id in TIMEOUT_IDS:
                    self._hang_until_closed()
                    return
                if delay:
                    time.sleep(delay)
                self._write_json(200, {"id": url_id, "ok": True})
            finally:
                state.leave()

        def _hang_until_closed(self):
            # Stay in-flight but notice client disconnects promptly instead
            # of sleeping for a long wall-clock period.
            connection = self.connection
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                readable, _, _ = select.select([connection], [], [], 0.05)
                if readable:
                    return

        def _write_json(self, status, payload):
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()

    return MockHandler


class MockSiteServer:
    """Importable wrapper that serves on a background thread."""

    def __init__(self, port=PORT, delay=0.0):
        self.port = port
        self.state = ServerState()
        self.httpd = ThreadingHTTPServer(
            ("127.0.0.1", port), make_handler(self.state, delay)
        )
        self.httpd.daemon_threads = True
        self._thread = None

    def start(self):
        self._thread = threading.Thread(
            target=self.httpd.serve_forever, name="mock-site", daemon=True
        )
        self._thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def reset_stats(self):
        self.state.reset()

    def stats(self):
        return self.state.snapshot()

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False


def main(argv=None):
    server = MockSiteServer(PORT)
    print("mock site listening on http://127.0.0.1:%d" % PORT, flush=True)
    try:
        server.httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

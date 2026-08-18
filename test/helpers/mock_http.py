#!/usr/bin/env python3
"""
Mock HTTP server for source/poller tests.

Provides a configurable in-process HTTP server that returns JSON responses,
used by dedup and poller tests without requiring an external service.
"""

from __future__ import annotations

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any


class MockHandler(BaseHTTPRequestHandler):
    """Configurable mock HTTP server used by dedup / poller tests."""
    _response_data: bytes = b'[]'
    _response_code: int = 200

    def do_GET(self):
        self.send_response(self._response_code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(self._response_data)

    def log_message(self, format, *args):
        pass


def start_mock(items: list[dict[str, Any]]) -> tuple[HTTPServer, int]:
    """Start a mock HTTP server returning *items* as JSON and return (server, port)."""
    MockHandler._response_data = json.dumps(items).encode()
    MockHandler._response_code = 200
    srv = HTTPServer(("127.0.0.1", 0), MockHandler)
    port = srv.server_address[1]
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, port

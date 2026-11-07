"""Minimal process health contract. No exchange client or order capability exists."""

import argparse
import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer

from tradeforge.config.settings import load_settings

SERVICES = frozenset({"market_gateway", "quant_core", "execution_guardian", "dashboard"})


def snapshot(service: str) -> dict[str, str | bool]:
    """Reject unsafe configuration; never expose arbitrary environment values."""
    if service not in SERVICES:
        raise ValueError("UNKNOWN_SERVICE")
    settings = load_settings()
    return {
        "service": service,
        "phase": "PHASE_01",
        "mode": settings.mode.value,
        "live": settings.live,
        "orders_enabled": False,
        "ready": False,
        "reason": "FOUNDATION_ONLY",
    }


def create_server(service: str, port: int, *, exclusive: bool = False) -> HTTPServer:
    """Bind exclusively to loopback after safety validation; port zero aids local tests."""
    state = snapshot(service)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            status = {"/health": 200, "/readiness": 503}.get(self.path, 404)
            body = dict(state)
            if self.path == "/health":
                body["alive"] = True
            payload = json.dumps(body, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            # Requests may contain credentials; the foundation never records them.
            return

    server = HTTPServer(("127.0.0.1", port), Handler, bind_and_activate=False)
    try:
        if exclusive:
            server.allow_reuse_address = False
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        server.server_bind()
        server.server_activate()
    except BaseException:
        server.server_close()
        raise
    server.timeout = 1
    return server


def main(service: str, default_port: int) -> int:
    """Run an offline check or local health server; neither can trade."""
    parser = argparse.ArgumentParser(description=f"Tradeforge {service}: non-live foundation")
    parser.add_argument("--check", action="store_true", help="Print offline safety status and exit")
    parser.add_argument("--port", type=int, default=default_port, help="Loopback health port")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535:
        parser.error("port must be between 0 and 65535")
    try:
        state = snapshot(service)
    except ValueError as error:
        parser.error(str(error))
    if args.check:
        print(json.dumps(state, sort_keys=True))
        return 0
    try:
        with create_server(service, args.port) as server:
            print(json.dumps(state, sort_keys=True), flush=True)
            try:
                server.serve_forever(poll_interval=0.25)
            except KeyboardInterrupt:
                pass
    except OSError:
        parser.error("LOCAL_BIND_FAILED: health port unavailable")
    return 0

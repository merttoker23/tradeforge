"""The real reconciliation uses a scoped GET connection and still checks both views."""

import json
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from urllib.parse import urlsplit

import pytest

from tradeforge.execution.binance_client import HOST, BinanceTestnetClient, Credentials, send_https
from tradeforge.execution.position_lifecycle import PositionLifecycle


@pytest.mark.parametrize("mode", ["ok", "changed", "unavailable"])
def test_reconciliation_reuses_only_read_connection_and_preserves_final_checks(
    migrated, monkeypatch, mode
):
    connections, requests = [], []
    account_reads = 0

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            nonlocal account_reads
            path = urlsplit(self.path).path
            requests.append(path)
            if path == "/fapi/v3/account":
                account_reads += 1
            position = {
                "symbol": "BTCUSDT",
                "positionSide": "BOTH",
                "positionAmt": "1",
                "entryPrice": "100",
                "markPrice": "100",
                "initialMargin": "10",
                "marginAsset": "USDT",
            }
            positions = [position] if mode == "changed" and account_reads == 2 else []
            body = (
                {"positions": positions, "totalMarginBalance": "1000", "availableBalance": "1000"}
                if path == "/fapi/v3/account"
                else positions
                if path == "/fapi/v3/positionRisk"
                else []
            )
            status = 503 if mode == "unavailable" and account_reads == 2 else 200
            raw = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def local(host, timeout):
        assert host == HOST and timeout == 3
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=timeout)
        connections.append(connection)
        return connection

    monkeypatch.setattr("http.client.HTTPSConnection", local)
    client = BinanceTestnetClient(
        Credentials(api_key="offline-key", api_secret="offline-secret"),
        mode="testnet",
        opted_in=True,
        read_only=False,
    )
    recovery = PositionLifecycle(migrated, client, latch_failure=False)
    try:
        recovery.reconcile()
        assert recovery.ready is (mode == "ok"), recovery.health()
        assert account_reads == 2
        assert len(connections) == 1
        assert connections[0].sock is None
        assert client.transport is send_https and not client.read_only
        health = recovery.health()
        assert health["account_requests"]
        assert all(
            row["method"] == "GET" and row["duration_ms"] >= 0 for row in health["account_requests"]
        )
        assert "offline-key" not in str(health) and "offline-secret" not in str(health)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

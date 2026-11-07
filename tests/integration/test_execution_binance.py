"""Fixed-host signed testnet boundary. All requests are captured offline."""

import hashlib
import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal as D
from urllib.parse import parse_qs

import pytest

from tests.risk_fixtures import change


def client(replies, *, mode="testnet"):
    from tradeforge.execution.binance_client import BinanceTestnetClient, Credentials, Response

    calls = []

    def send(method, path, params, headers):
        calls.append((method, path, params, headers))
        status, body = replies.pop(0)
        return Response(status, {}, json.dumps(body).encode())

    result = BinanceTestnetClient(
        Credentials(api_key="offline-key", api_secret="offline-secret"),
        mode=mode,
        opted_in=True,
        read_only=False,
        transport=send,
        clock=lambda: datetime(2026, 10, 20, 12, tzinfo=UTC),
    )
    return result, calls


@pytest.mark.parametrize("mode", ["live", "paper", "disabled"])
def test_signed_client_rejects_every_non_testnet_mode(mode):
    with pytest.raises(ValueError, match="TESTNET_OPT_IN_REQUIRED"):
        client([], mode=mode)


def test_credentials_are_redacted_and_query_signature_covers_exact_decimal_wire(proposal):
    _, intent = proposal
    from tradeforge.domain.execution import StopPlan

    intent = change(
        intent,
        qty=D("1.2300"),
        price=D("100.01"),
        stop_plan=StopPlan(qty=D("1.2300"), stop_price=D("90")),
    )
    c, calls = client(
        [
            (
                200,
                {
                    "symbol": intent.symbol,
                    "clientOrderId": intent.client_order_id,
                    "orderId": 101,
                    "side": "BUY",
                    "origQty": "1.2300",
                    "executedQty": "0",
                    "avgPrice": "0",
                    "status": "NEW",
                    "updateTime": 1792497600000,
                },
            )
        ]
    )
    assert c.place(intent).filled_qty == 0
    method, path, encoded, headers = calls[0]
    unsigned, signature = encoded.rsplit("&signature=", 1)
    assert hmac.new(b"offline-secret", unsigned.encode(), hashlib.sha256).hexdigest() == signature
    fields = parse_qs(unsigned)
    assert (method, path) == ("POST", "/fapi/v1/order")
    assert fields["quantity"] == ["1.2300"] and fields["price"] == ["100.01"]
    assert fields["type"] == ["LIMIT"] and fields["timeInForce"] == ["IOC"]
    assert "offline-secret" not in repr(c) and "offline-key" not in repr(c.credentials)
    assert headers["X-MBX-APIKEY"] == "offline-key"


@pytest.mark.parametrize(
    "status,body,error",
    [
        (503, {"code": -1000, "msg": "secret body"}, "AckUnknown"),
        (408, {}, "AckUnknown"),
        (429, {"code": -1003}, "ExchangeRejected"),
        (400, {"code": -2019}, "ExchangeRejected"),
    ],
)
def test_http_failures_are_classified_and_never_echo_raw_messages(proposal, status, body, error):
    from tradeforge.execution import broker

    c, calls = client([(status, body)])
    with pytest.raises(getattr(broker, error)) as caught:
        c.place(proposal[1])
    assert "secret body" not in str(caught.value)
    assert len(calls) == 1


def test_close_all_stop_uses_algo_api_and_never_conflicting_quantity_or_reduce_only(proposal):
    intent = proposal[1]
    c, calls = client(
        [
            (
                200,
                {
                    "clientAlgoId": "stop_1",
                    "symbol": intent.symbol,
                    "algoStatus": "NEW",
                    "closePosition": True,
                    "side": "SELL",
                    "orderType": "STOP_MARKET",
                    "triggerPrice": "90",
                    "algoId": 9,
                },
            )
        ]
    )
    assert c.protect(intent, "stop_1")
    fields = parse_qs(calls[0][2])
    assert calls[0][1] == "/fapi/v1/algoOrder"
    assert fields["closePosition"] == ["true"]
    assert "quantity" not in fields and "reduceOnly" not in fields
    assert fields["triggerPrice"] == ["90"]


def test_real_http_transport_signs_only_to_fixed_testnet_host(proposal, monkeypatch):
    from http.client import HTTPConnection
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from threading import Thread

    from tradeforge.execution.binance_client import HOST, BinanceTestnetClient, Credentials

    intent = proposal[1]
    received = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(self.rfile.read(int(self.headers["Content-Length"])).decode())
            body = json.dumps(
                {
                    "symbol": intent.symbol,
                    "clientOrderId": intent.client_order_id,
                    "orderId": 101,
                    "side": "BUY",
                    "origQty": str(intent.qty),
                    "executedQty": "0",
                    "avgPrice": "0",
                    "status": "NEW",
                    "updateTime": int(intent.as_of.timestamp() * 1000),
                }
            ).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    with HTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def local(host, timeout):
            assert host == HOST == "demo-fapi.binance.com"
            return HTTPConnection("127.0.0.1", server.server_port, timeout=timeout)

        monkeypatch.setattr(
            "tradeforge.execution.binance_client.http.client.HTTPSConnection", local
        )
        try:
            c = BinanceTestnetClient(
                Credentials(api_key="offline", api_secret="offline"),
                mode="testnet",
                opted_in=True,
                read_only=False,
            )
            assert c.place(intent).order_id == "101"
            assert parse_qs(received[0])["newClientOrderId"] == [intent.client_order_id]
        finally:
            server.shutdown()
            thread.join(timeout=2)


def test_trade_lookup_throttle_after_accepted_post_is_unknown_not_rejection(proposal):
    from tradeforge.execution.broker import AckUnknown

    intent = proposal[1]
    c, calls = client(
        [
            (
                200,
                {
                    "symbol": intent.symbol,
                    "clientOrderId": intent.client_order_id,
                    "orderId": 101,
                    "side": "BUY",
                    "origQty": str(intent.qty),
                    "executedQty": str(intent.qty),
                    "avgPrice": "100",
                    "status": "FILLED",
                    "updateTime": int(intent.as_of.timestamp() * 1000),
                },
            ),
            (429, {"code": -1003}),
        ]
    )
    with pytest.raises(AckUnknown):
        c.place(intent)
    assert [x[0] for x in calls] == ["POST", "GET"]

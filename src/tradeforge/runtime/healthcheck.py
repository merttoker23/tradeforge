"""Fixed loopback liveness probe; no service imports, credentials or authority."""

import argparse
import http.client

PORTS = {"market": 8101, "quant": 8102, "dashboard": 8104}


def health(role: str) -> int:
    connection = http.client.HTTPConnection("127.0.0.1", PORTS[role], timeout=3)
    try:
        connection.request("GET", "/health")
        response = connection.getresponse()
        return 0 if response.status == 200 else 1
    except (OSError, http.client.HTTPException):
        return 1
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Tradeforge local liveness probe")
    parser.add_argument("--service", required=True, choices=tuple(PORTS))
    return health(parser.parse_args().service)


if __name__ == "__main__":
    raise SystemExit(main())

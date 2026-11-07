"""The login can load before authentication without exposing the terminal assets."""

import asyncio
from html.parser import HTMLParser

from aiohttp import ClientSession, web

from tests.unit.test_dashboard_sessions import session_auth
from tradeforge.dashboard.api import HEADERS, create_app


class LoginDocument(HTMLParser):
    def __init__(self):
        super().__init__()
        self.styles = []
        self.scripts = []
        self.forms = []
        self.inputs = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "link" and attrs.get("rel") == "stylesheet":
            self.styles.append(attrs["href"])
        if tag == "script" and "src" in attrs:
            self.scripts.append(attrs["src"])
        if tag == "form":
            self.forms.append(attrs)
        if tag == "input":
            self.inputs.append(attrs)


def test_login_loads_its_styles_and_script_without_a_session():
    async def run():
        runner = web.AppRunner(
            create_app(sessions=session_auth(), external_origin="https://trade.example.com")
        )
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        host = {"Host": "trade.example.com"}
        try:
            async with ClientSession(headers=host) as client:
                async with client.get(url + "/login") as response:
                    assert response.status == 200
                    document = LoginDocument()
                    document.feed(await response.text())
                    for key, value in HEADERS.items():
                        assert response.headers[key] == value
                assert document.styles
                assert document.scripts
                assert document.forms[0]["method"] == "post"
                assert document.forms[0]["action"] == "/login"
                token = next(i for i in document.inputs if i.get("name") == "token")
                assert token["type"] == "password"
                assert token["autocomplete"] == "current-password"
                assert token["maxlength"] == "128"
                for asset in document.styles + document.scripts:
                    async with client.get(url + asset, allow_redirects=False) as response:
                        assert response.status == 200
                        expected = "text/css" if asset.endswith(".css") else "text/javascript"
                        assert response.content_type == expected
                        assert await response.read()
                        for key, value in HEADERS.items():
                            assert response.headers[key] == value
                for asset in ("/", "/assets/app.css", "/assets/app.js", "/assets/chart.js"):
                    async with client.get(url + asset, allow_redirects=False) as response:
                        assert response.status == 303
                        assert response.headers["Location"] == "/login"
                async with client.get(url + "/api/session") as response:
                    assert response.status == 401
        finally:
            await runner.cleanup()

    asyncio.run(run())


def test_login_failures_keep_status_codes_and_never_issue_cookies():
    async def run():
        runner = web.AppRunner(
            create_app(sessions=session_auth(), external_origin="https://trade.example.com")
        )
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        host = {"Host": "trade.example.com", "Origin": "https://trade.example.com"}
        try:
            async with ClientSession(headers=host) as client:
                for attempt in range(9):
                    async with client.post(
                        url + "/login", data={"token": "invalid"}, allow_redirects=False
                    ) as response:
                        assert response.status == (401 if attempt < 8 else 429)
                        assert "Set-Cookie" not in response.headers
                        assert "invalid" not in await response.text()
                async with client.post(
                    url + "/login",
                    headers={"Origin": "https://evil.invalid"},
                    data={"token": "x" * 64},
                    allow_redirects=False,
                ) as response:
                    assert response.status == 403
                    assert "Set-Cookie" not in response.headers
        finally:
            await runner.cleanup()

    asyncio.run(run())

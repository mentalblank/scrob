import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("SECRET_KEY", "test-secret")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

import httpx

import schemas
from core import plex
from routers import auth


_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _mock_client(transport: httpx.MockTransport):
    return patch.object(
        plex.httpx,
        "AsyncClient",
        side_effect=lambda **kw: _REAL_ASYNC_CLIENT(
            transport=transport, **{k: v for k, v in kw.items() if k != "verify"}
        ),
    )


class PlexAccountAuthClientTests(unittest.IsolatedAsyncioTestCase):
    """The plex.tv account flow: one app-level client identifier, a PIN the
    user authorises in a browser, then a per-server token discovered from
    /api/v2/resources."""

    async def test_create_and_check_pin(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.method == "POST" and request.url.path == "/api/v2/pins":
                self.assertEqual(request.url.params["strong"], "true")
                self.assertEqual(request.headers["X-Plex-Client-Identifier"], plex.PLEX_AUTH_CLIENT_ID)
                return httpx.Response(201, json={"id": 42, "code": "WXYZ"})
            if request.url.path == "/api/v2/pins/42":
                return httpx.Response(200, json={"id": 42, "authToken": "tok-123"})
            return httpx.Response(404)

        with _mock_client(httpx.MockTransport(handler)):
            pin = await plex.create_auth_pin()
            self.assertEqual(pin, {"id": 42, "code": "WXYZ"})
            token = await plex.check_auth_pin("42")
        self.assertEqual(token, "tok-123")

    async def test_check_pin_pending_returns_none(self) -> None:
        handler = lambda r: httpx.Response(200, json={"id": 1, "authToken": None})
        with _mock_client(httpx.MockTransport(handler)):
            self.assertIsNone(await plex.check_auth_pin("1"))

    def test_build_auth_url_carries_code_forward_and_product(self) -> None:
        url = plex.build_auth_url("PINC", "https://scrob.local/plex-callback")
        self.assertTrue(url.startswith("https://app.plex.tv/auth#?"))
        self.assertIn(f"clientID={plex.PLEX_AUTH_CLIENT_ID}", url)
        self.assertIn("code=PINC", url)
        self.assertIn("forwardUrl=", url)
        self.assertIn("product", url)

    async def test_get_account_maps_fields(self) -> None:
        handler = lambda r: httpx.Response(
            200, json={"id": 777, "username": "neo", "title": "Thomas", "email": "n@zion.io", "thumb": "t.png"}
        )
        with _mock_client(httpx.MockTransport(handler)):
            acct = await plex.get_account("tok")
        self.assertEqual(acct, {"id": "777", "username": "neo", "email": "n@zion.io", "thumb": "t.png"})

    def _resources(self, connections):
        return [
            {
                "name": "Home",
                "clientIdentifier": "machine-1",
                "provides": "server",
                "owned": True,
                "accessToken": "srv-tok-1",
                "connections": connections,
            },
            {"name": "Some Player", "clientIdentifier": "p1", "provides": "player", "connections": []},
        ]

    def _handler(self, connections, reachable):
        """Serves /api/v2/resources, and answers a probe only for `reachable`."""
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/v2/resources":
                return httpx.Response(200, json=self._resources(connections))
            probed = str(request.url).rstrip("/")
            return httpx.Response(200 if probed in reachable else 502, json={})
        return handler

    async def test_get_servers_ignores_non_servers_and_maps_the_reachable_connection(self) -> None:
        conns = [
            {"uri": "https://direct", "local": False, "relay": False, "protocol": "https"},
            {"uri": "https://relay", "local": False, "relay": True, "protocol": "https"},
        ]
        with _mock_client(httpx.MockTransport(self._handler(conns, {"https://direct"}))):
            servers = await plex.get_servers("acct-tok")
        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0]["client_identifier"], "machine-1")
        self.assertEqual(servers[0]["token"], "srv-tok-1")
        self.assertEqual(servers[0]["url"], "https://direct")
        self.assertTrue(servers[0]["owned"])

    async def test_get_servers_prefers_a_direct_connection_over_a_relay(self) -> None:
        # Relay is ranked last, so a reachable direct connection wins even when
        # the relay is listed first and also answers.
        conns = [
            {"uri": "https://relay", "local": False, "relay": True, "protocol": "https"},
            {"uri": "https://direct", "local": False, "relay": False, "protocol": "https"},
        ]
        with _mock_client(httpx.MockTransport(self._handler(conns, {"https://relay", "https://direct"}))):
            servers = await plex.get_servers("acct-tok")
        self.assertEqual(servers[0]["url"], "https://direct")

    async def test_get_servers_skips_a_connection_that_does_not_answer(self) -> None:
        conns = [
            {"uri": "https://blocked", "local": False, "relay": False, "protocol": "https"},
            {"uri": "https://relay", "local": False, "relay": True, "protocol": "https"},
        ]
        with _mock_client(httpx.MockTransport(self._handler(conns, {"https://relay"}))):
            servers = await plex.get_servers("acct-tok")
        self.assertEqual(servers[0]["url"], "https://relay")

    async def test_get_servers_drops_a_server_when_nothing_answers(self) -> None:
        # No fallback to an unreachable URI - storing one would create a
        # connection that can never sync.
        conns = [{"uri": "https://blocked", "local": False, "relay": False, "protocol": "https"}]
        with _mock_client(httpx.MockTransport(self._handler(conns, set()))):
            servers = await plex.get_servers("acct-tok")
        self.assertEqual(servers, [])


class PlexConnectionSchemaTests(unittest.TestCase):
    def test_create_schema_carries_plex_login_fields(self):
        body = schemas.MediaServerConnectionCreate(
            type="plex", name="Home", url="https://local", token="srv-tok",
            plex_auth_token="acct-tok", plex_account_id="5", plex_machine_identifier="m1",
        )
        self.assertEqual(body.plex_auth_token, "acct-tok")
        self.assertEqual(body.plex_account_id, "5")
        self.assertEqual(body.plex_machine_identifier, "m1")

    def test_response_schema_omits_account_token(self):
        self.assertNotIn("plex_auth_token", schemas.MediaServerConnectionResponse.model_fields)


if __name__ == "__main__":
    unittest.main()

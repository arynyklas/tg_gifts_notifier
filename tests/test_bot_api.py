from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading

import certifi
import pytest

from src.bot_api import BotApiClient, BotApiError
from src.config import Config, TelegramConfig

Responder = Callable[[str, dict], tuple[int, object]]


class _FakeBotApiServer:
    """Local HTTP server standing in for api.telegram.org."""

    def __init__(self, responder: Responder) -> None:
        self.calls: list[tuple[str, dict]] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self) -> None:
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))

                outer.calls.append((self.path, body))

                status, payload = responder(self.path, body)
                raw = json.dumps(payload).encode()

                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, format: str, *args) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        )

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]

        return f"http://{host}:{port}"

    def __enter__(self) -> "_FakeBotApiServer":
        self._thread.start()

        return self

    def __exit__(self, *args) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    @property
    def tokens_used(self) -> list[str]:
        return [
            path.split("/")[1].removeprefix("bot")
            for path in (call[0] for call in self.calls)
        ]


@pytest.fixture
def serve(config: Config, logger) -> Iterator[Callable[[Responder], BotApiClient]]:
    servers: list[_FakeBotApiServer] = []
    clients: list[BotApiClient] = []

    def factory(responder: Responder) -> BotApiClient:
        server = _FakeBotApiServer(responder)

        servers.append(server)
        server.__enter__()

        telegram = TelegramConfig.model_validate(
            config.telegram.model_dump() | {"bot_api_base_url": server.base_url}
        )
        client = BotApiClient(telegram, logger)

        clients.append(client)
        setattr(client, "server", server)  # noqa: B010

        return client

    yield factory

    for server in servers:
        server.__exit__()


async def test_send_message_returns_the_message_id(serve):
    client = serve(
        lambda path, body: (200, {"ok": True, "result": {"message_id": 4242}})
    )

    try:
        assert await client.send_message(-100, "hello", reply_to_message_id=7) == 4242

    finally:
        await client.aclose()

    path, body = client.server.calls[0]

    assert path.endswith("/sendMessage")
    assert body["chat_id"] == -100
    assert body["text"] == "hello"
    assert body["parse_mode"] == "HTML"
    assert body["link_preview_options"] == {"is_disabled": True}
    assert body["reply_parameters"] == {
        "message_id": 7,
        "allow_sending_without_reply": True,
    }


async def test_rate_limit_rotates_tokens_and_honours_retry_after(serve):
    state = {"calls": 0}

    def responder(path: str, body: dict) -> tuple[int, object]:
        state["calls"] += 1

        if state["calls"] == 1:
            return 429, {
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests",
                "parameters": {"retry_after": 0},
            }

        return 200, {"ok": True, "result": {"message_id": 1}}

    client = serve(responder)

    try:
        assert await client.send_message(-100, "hello") == 1

    finally:
        await client.aclose()

    used = client.server.tokens_used

    assert len(used) == 2
    assert used[0] != used[1]  # the rate-limited token is not reused immediately


async def test_deterministic_error_fails_without_burning_every_token(serve):
    client = serve(
        lambda path, body: (
            403,
            {"ok": False, "error_code": 403, "description": "Forbidden"},
        )
    )

    try:
        with pytest.raises(BotApiError) as excinfo:
            await client.send_message(-100, "hello")

    finally:
        await client.aclose()

    assert excinfo.value.code == 403
    assert len(client.server.calls) == 1


async def test_exhausted_attempts_raise_a_summary_error(serve):
    client = serve(
        lambda path, body: (
            429,
            {"ok": False, "error_code": 429, "parameters": {"retry_after": 0}},
        )
    )

    try:
        with pytest.raises(BotApiError) as excinfo:
            await client.send_message(-100, "hello")

    finally:
        await client.aclose()

    assert excinfo.value.code == -1
    assert "all bot tokens failed for sendMessage" in excinfo.value.description
    assert len(client.server.calls) == 3  # max(3, len(bot_tokens))


async def test_malformed_body_is_retried(serve):
    state = {"calls": 0}

    def responder(path: str, body: dict) -> tuple[int, object]:
        state["calls"] += 1

        if state["calls"] == 1:
            return 200, ["not", "an", "object"]

        return 200, {"ok": True, "result": {"message_id": 9}}

    client = serve(responder)

    try:
        assert await client.send_message(-100, "hello") == 9

    finally:
        await client.aclose()

    assert len(client.server.calls) == 2


async def test_not_modified_edit_is_swallowed_and_other_errors_propagate(serve):
    def responder(path: str, body: dict) -> tuple[int, object]:
        if body["message_id"] == 1:
            return 400, {
                "ok": False,
                "error_code": 400,
                "description": "Bad Request: message is not modified",
            }

        return 400, {
            "ok": False,
            "error_code": 400,
            "description": "Bad Request: message to edit not found",
        }

    client = serve(responder)

    try:
        assert await client.edit_message_text(-100, 1, "same text") is False

        with pytest.raises(BotApiError):
            await client.edit_message_text(-100, 2, "text")

    finally:
        await client.aclose()


async def test_successful_edit_returns_true(serve):
    client = serve(lambda path, body: (200, {"ok": True, "result": {"message_id": 1}}))

    try:
        assert await client.edit_message_text(-100, 1, "new text") is True

    finally:
        await client.aclose()

    path, body = client.server.calls[0]

    assert path.endswith("/editMessageText")
    assert body["text"] == "new text"
    assert body["parse_mode"] == "HTML"


async def test_every_request_carries_the_certifi_bundle(serve, monkeypatch):
    """`verify` only takes effect per request.

    niquests overrides both `AsyncSession(verify=...)` and `session.verify` with
    the `verify=True` default of each request method, which silently falls back
    to the OS trust store — where the root signing api.telegram.org may be
    missing. Moving `verify` back to the session would fail exactly like that,
    so pin it here.
    """
    client = serve(lambda path, body: (200, {"ok": True, "result": {"message_id": 1}}))
    seen: list[object] = []
    original = client._http.post

    async def spy(url, **kwargs):
        seen.append(kwargs.get("verify"))

        return await original(url, **kwargs)

    monkeypatch.setattr(client._http, "post", spy)

    try:
        await client.send_message(-100, "hello")

    finally:
        await client.aclose()

    assert seen == [certifi.where()]

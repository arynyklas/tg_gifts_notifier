from collections.abc import Callable, Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import threading
from types import SimpleNamespace

import certifi
from niquests import exceptions as niquests_errors
import pytest

from src import bot_api
from src.bot_api import BotApiClient, BotApiError, _redact
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
def serve(config: Config, logger) -> Iterator[Callable[..., BotApiClient]]:
    servers: list[_FakeBotApiServer] = []
    clients: list[BotApiClient] = []

    def factory(responder: Responder, tokens: list[str] | None = None) -> BotApiClient:
        server = _FakeBotApiServer(responder)

        servers.append(server)
        server.__enter__()

        overrides: dict[str, object] = {"bot_api_base_url": server.base_url}

        if tokens is not None:
            overrides["bot_tokens"] = tokens

        telegram = TelegramConfig.model_validate(
            config.telegram.model_dump() | overrides
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


def _record_sleeps(monkeypatch) -> list[float]:
    """Capture `bot_api`'s sleeps without touching the global `asyncio.sleep`.

    Patching `src.bot_api.asyncio.sleep` would replace the attribute on the
    asyncio module itself, which deadlocks niquests and the test HTTP server.
    `bot_api` uses `asyncio` for nothing but the sleep, so the module reference
    is swapped instead.
    """
    delays: list[float] = []

    async def recorder(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(bot_api, "asyncio", SimpleNamespace(sleep=recorder))

    return delays


async def test_rate_limit_rotates_tokens_and_honours_retry_after(serve, monkeypatch):
    """The retry_after must actually be slept through.

    With `retry_after: 0` this test passed even with the `asyncio.sleep` deleted,
    so the delay is recorded and asserted instead.
    """
    state = {"calls": 0}

    def responder(path: str, body: dict) -> tuple[int, object]:
        state["calls"] += 1

        if state["calls"] == 1:
            return 429, {
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests",
                "parameters": {"retry_after": 3},
            }

        return 200, {"ok": True, "result": {"message_id": 1}}

    delays = _record_sleeps(monkeypatch)

    client = serve(responder)

    try:
        assert await client.send_message(-100, "hello") == 1

    finally:
        await client.aclose()

    used = client.server.tokens_used

    assert len(used) == 2
    assert used[0] != used[1]  # the rate-limited token is not reused immediately
    assert delays == [3]


async def test_rate_limit_without_parameters_falls_back_to_one_second(
    serve, monkeypatch
):
    state = {"calls": 0}

    def responder(path: str, body: dict) -> tuple[int, object]:
        state["calls"] += 1

        if state["calls"] == 1:
            return 429, {
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests",
            }

        return 200, {"ok": True, "result": {"message_id": 2}}

    delays = _record_sleeps(monkeypatch)

    client = serve(responder)

    try:
        assert await client.send_message(-100, "hello") == 2

    finally:
        await client.aclose()

    assert delays == [1]


async def test_bad_request_fails_without_burning_every_token(serve):
    client = serve(
        lambda path, body: (
            400,
            {"ok": False, "error_code": 400, "description": "Bad Request: chat"},
        )
    )

    try:
        with pytest.raises(BotApiError) as excinfo:
            await client.send_message(-100, "hello")

    finally:
        await client.aclose()

    assert excinfo.value.code == 400
    assert len(client.server.calls) == 1


async def test_revoked_token_is_rotated_past_on_every_send(serve):
    """A 401 is scoped to one token; legacy moved on to the next one."""

    def responder(path: str, body: dict) -> tuple[int, object]:
        if "/botrevoked/" in path:
            return 401, {"ok": False, "error_code": 401, "description": "Unauthorized"}

        return 200, {"ok": True, "result": {"message_id": 5}}

    client = serve(responder, tokens=["good", "revoked"])

    try:
        for _ in range(4):
            assert await client.send_message(-100, "hello") == 5

    finally:
        await client.aclose()

    assert "revoked" in client.server.tokens_used


@pytest.mark.parametrize("code", [401, 403, 404, 500, 502])
async def test_server_or_forbidden_error_is_retried_on_the_next_token(serve, code):
    state = {"calls": 0}

    def responder(path: str, body: dict) -> tuple[int, object]:
        state["calls"] += 1

        if state["calls"] == 1:
            return code, {"ok": False, "error_code": code, "description": "Oops"}

        return 200, {"ok": True, "result": {"message_id": 6}}

    client = serve(responder, tokens=["first", "second"])

    try:
        assert await client.send_message(-100, "hello") == 6

    finally:
        await client.aclose()

    assert client.server.tokens_used == ["first", "second"]


async def test_exhausted_attempts_raise_the_last_error(serve):
    state = {"calls": 0}

    def responder(path: str, body: dict) -> tuple[int, object]:
        state["calls"] += 1

        return 502, {
            "ok": False,
            "error_code": 502,
            "description": f"Bad Gateway {state['calls']}",
        }

    client = serve(responder)

    try:
        with pytest.raises(BotApiError) as excinfo:
            await client.send_message(-100, "hello")

    finally:
        await client.aclose()

    assert excinfo.value.code == 502
    assert excinfo.value.description == "Bad Gateway 3"
    assert len(client.server.calls) == 3  # max(3, len(bot_tokens))


async def test_persistent_rate_limit_skips_the_sleep_after_the_last_attempt(
    serve, monkeypatch
):
    """Sleeping after the final 429 only delayed the raise by `retry_after`."""
    client = serve(
        lambda path, body: (
            429,
            {
                "ok": False,
                "error_code": 429,
                "description": "Too Many Requests",
                "parameters": {"retry_after": 30},
            },
        )
    )
    delays = _record_sleeps(monkeypatch)

    try:
        with pytest.raises(BotApiError) as excinfo:
            await client.send_message(-100, "hello")

    finally:
        await client.aclose()

    assert excinfo.value.code == 429
    assert len(client.server.calls) == 3
    assert delays == [30, 30]


async def test_exhausted_attempts_without_any_api_error_raise_a_summary(serve):
    client = serve(lambda path, body: (200, ["not", "an", "object"]))

    try:
        with pytest.raises(BotApiError) as excinfo:
            await client.send_message(-100, "hello")

    finally:
        await client.aclose()

    assert excinfo.value.code == -1
    assert "all bot tokens failed for sendMessage" in excinfo.value.description


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
        assert await client.edit_message_text(-100, 1, "same text") is None

        with pytest.raises(BotApiError):
            await client.edit_message_text(-100, 2, "text")

    finally:
        await client.aclose()


async def test_successful_edit_returns_normally(serve):
    client = serve(lambda path, body: (200, {"ok": True, "result": {"message_id": 1}}))

    try:
        assert await client.edit_message_text(-100, 1, "new text") is None

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


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)

        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def test_transport_failures_never_log_the_bot_token(config: Config):
    """The request path carries the token, and niquests puts the path in `str(ex)`.

    Logging the exception verbatim wrote a live token into `logs/main.log` on
    every DNS failure, refused connection, TLS failure and read timeout.
    """
    token = "123456789:AAFAKE_SECRET"
    capture = _Capture()
    logger = logging.getLogger("tests.bot_api.redaction")

    logger.handlers.clear()
    logger.addHandler(capture)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    telegram = TelegramConfig.model_validate(
        config.telegram.model_dump()
        | {
            "bot_tokens": [token],
            "bot_api_base_url": "https://127.0.0.1:9",
            "request_timeout": 2.0,
        }
    )
    client = BotApiClient(telegram, logger)

    try:
        with pytest.raises(BotApiError):
            await client.send_message(-100, "hello")

    finally:
        await client.aclose()
        logger.removeHandler(capture)

    messages = [record.getMessage() for record in capture.records]

    assert messages
    assert not [message for message in messages if token in message]
    assert [message for message in messages if "/bot<redacted>/" in message]


def test_every_niquests_exception_is_caught_by_the_single_handler():
    """`_request` catches `RequestException` alone, so nothing tainted escapes.

    The old triple let `RetryError`, `SSLError`, `InvalidURL` and friends through
    into the notifier's `except OSError`, which logged the tainted traceback.
    """
    assert issubclass(niquests_errors.RequestException, OSError)

    for name in (
        "Timeout",
        "ConnectionError",
        "JSONDecodeError",
        "RetryError",
        "InvalidURL",
        "TooManyRedirects",
        "ChunkedEncodingError",
        "SSLError",
        "ProxyError",
    ):
        assert issubclass(
            getattr(niquests_errors, name), niquests_errors.RequestException
        )


def test_redact_leaves_everything_but_the_token():
    assert (
        _redact(
            "Max retries exceeded with url: /bot123:SECRET/sendMessage (Caused by X)"
        )
        == "Max retries exceeded with url: /bot<redacted>/sendMessage (Caused by X)"
    )

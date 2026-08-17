from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from telethon import TelegramClient, errors
from telethon.sessions import Session

from src.config import Config, TelegramConfig
from src.main import _build_parser, _login, _print_qr, _run_login
from src.userbot import build_client

ACCEPTED = "hunter2"  # the only value the fake client signs in with
QR_URL = (
    "tg://login?token=AQAAAAABBBBCCCCDDDDEEEEFFFF0011223344556677889900aabbccddeeff"
)


class FakeQrLogin:
    def __init__(self, outcomes: list[object]) -> None:
        self._outcomes = outcomes
        self.url = QR_URL
        self.recreated = 0
        self.waits = 0
        self.expires = datetime.now(tz=UTC) + timedelta(seconds=30)

    # Mirrors telethon's QRLogin.wait signature.
    async def wait(self, timeout: float | None = None) -> object:  # noqa: ASYNC109
        self.waits += 1
        outcome = self._outcomes.pop(0)

        if isinstance(outcome, BaseException):
            raise outcome

        return outcome

    async def recreate(self) -> None:
        self.recreated += 1
        self.url = f"{QR_URL}-{self.recreated}"


class FakeUser:
    def __init__(self, username: str) -> None:
        self.username = username
        self.id = 777


class FakeClient:
    def __init__(
        self,
        *,
        authorized: bool = False,
        outcomes: list[object] | None = None,
        accepted_password: str | None = None,
        qr_login_error: BaseException | None = None,
        start_error: BaseException | None = None,
    ) -> None:
        self.accepted_password = accepted_password
        self.qr_login_error = qr_login_error
        self.start_error = start_error
        self.authorized = authorized
        self.connected = False
        self.disconnected = False
        self.started = False
        self.passwords: list[str] = []
        self.qr = FakeQrLogin(outcomes or [FakeUser("scanned")])

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def get_me(self) -> FakeUser:
        return FakeUser("already")

    async def qr_login(self) -> FakeQrLogin:
        if self.qr_login_error is not None:
            raise self.qr_login_error

        return self.qr

    async def sign_in(self, password: str | None = None) -> FakeUser:
        self.passwords.append(password or "")

        if self.accepted_password is not None and password != self.accepted_password:
            raise errors.PasswordHashInvalidError(request=None)

        return FakeUser("two-factor")

    def start(self):
        self.started = True

        async def _started() -> None:
            if self.start_error is not None:
                raise self.start_error

        return _started()


def _as_client(fake: FakeClient) -> TelegramClient:
    return cast(TelegramClient, fake)


def test_login_flag_defaults_to_phone():
    assert _build_parser().parse_args([]).login is None
    assert _build_parser().parse_args(["--login"]).login == "phone"
    assert _build_parser().parse_args(["--login", "qr"]).login == "qr"


def test_login_flag_rejects_unknown_mode():
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["--login", "bogus"])


def test_qr_is_rendered_as_a_square_block(capsys):
    _print_qr(QR_URL)

    lines = capsys.readouterr().out.splitlines()

    assert len(lines) > 10
    assert len({len(line) for line in lines}) == 1  # square, so it stays scannable
    assert "█" in lines[0]


async def test_qr_login_succeeds_on_first_scan(logger, capsys):
    fake = FakeClient(outcomes=[FakeUser("scanned")])

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.connected is True
    assert fake.qr.waits == 1
    assert fake.qr.recreated == 0
    assert QR_URL in capsys.readouterr().out


async def test_qr_login_regenerates_an_expired_code(logger):
    fake = FakeClient(outcomes=[TimeoutError(), TimeoutError(), FakeUser("scanned")])

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.qr.waits == 3
    assert fake.qr.recreated == 2


async def test_qr_login_gives_up_after_the_last_code(logger):
    fake = FakeClient(outcomes=[TimeoutError() for _ in range(6)])

    assert await _login(_as_client(fake), logger, "qr") == 1

    assert fake.qr.waits == 6


async def test_qr_login_prompts_for_the_two_step_password(logger, monkeypatch):
    monkeypatch.setattr("src.main.getpass", lambda prompt="": ACCEPTED)

    fake = FakeClient(
        outcomes=[
            errors.SessionPasswordNeededError(request=None),
            FakeUser("two-factor"),
        ]
    )

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.passwords == [ACCEPTED]
    assert fake.qr.recreated == 0


def _prompts(monkeypatch, values: list[str]) -> None:
    queue = list(values)

    monkeypatch.setattr("src.main.getpass", lambda prompt="": queue.pop(0))


async def test_qr_login_retries_a_rejected_password(logger, monkeypatch):
    _prompts(monkeypatch, ["typo", ACCEPTED])

    fake = FakeClient(
        accepted_password=ACCEPTED,
        outcomes=[errors.SessionPasswordNeededError(request=None)],
    )

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.passwords == ["typo", ACCEPTED]


async def test_qr_login_stops_asking_after_the_last_attempt(logger, monkeypatch):
    _prompts(monkeypatch, ["a", "b", "c", "never asked"])

    fake = FakeClient(
        accepted_password=ACCEPTED,
        outcomes=[errors.SessionPasswordNeededError(request=None)],
    )

    assert await _login(_as_client(fake), logger, "qr") == 1

    assert fake.passwords == ["a", "b", "c"]


async def test_qr_login_does_not_send_an_empty_password(logger, monkeypatch):
    _prompts(monkeypatch, ["", ACCEPTED])

    fake = FakeClient(
        accepted_password=ACCEPTED,
        outcomes=[errors.SessionPasswordNeededError(request=None)],
    )

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.passwords == [ACCEPTED]


async def test_pending_password_session_skips_the_qr_code(logger, monkeypatch, capsys):
    """A scanned code stays accepted: asking for another scan cannot help."""
    _prompts(monkeypatch, [ACCEPTED])

    fake = FakeClient(
        accepted_password=ACCEPTED,
        qr_login_error=errors.SessionPasswordNeededError(request=None),
    )

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.passwords == [ACCEPTED]
    assert fake.qr.waits == 0
    assert "█" not in capsys.readouterr().out


async def test_pending_password_session_reports_a_lost_password(logger, monkeypatch):
    _prompts(monkeypatch, ["a", "b", "c"])

    fake = FakeClient(
        accepted_password=ACCEPTED,
        qr_login_error=errors.SessionPasswordNeededError(request=None),
    )

    assert await _login(_as_client(fake), logger, "qr") == 1


async def test_qr_login_short_circuits_an_authorized_session(logger):
    fake = FakeClient(authorized=True)

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.qr.waits == 0


async def test_phone_login_uses_start_and_disconnects(logger):
    fake = FakeClient()

    assert await _login(_as_client(fake), logger, "phone") == 0

    assert fake.started is True
    assert fake.disconnected is True


def test_qr_login_client_must_receive_updates(config: Config, tmp_path, logger):
    """QRLogin.wait() completes only on an UpdateLoginToken update."""
    telegram = TelegramConfig.model_validate(
        config.telegram.model_dump() | {"session_path": tmp_path / "userbot.session"}
    )

    notifier_client = build_client(telegram, logger)
    login_client = build_client(telegram, logger, receive_updates=True)

    try:
        assert notifier_client._no_updates is True
        assert login_client._no_updates is False

    finally:
        cast(Session, notifier_client.session).close()
        cast(Session, login_client.session).close()


async def test_run_login_always_disconnects(config: Config, logger, monkeypatch):
    """The client owns a connection either way; `_run_login` has to close it."""
    fake = FakeClient(authorized=True)

    monkeypatch.setattr("src.main.build_client", lambda *args, **kwargs: fake)

    assert await _run_login("qr", config, logger) == 0

    assert fake.disconnected is True


async def test_run_login_reports_a_spent_sign_in_instead_of_a_traceback(
    config: Config, logger, monkeypatch
):
    """`start()` aborts with a bare `RuntimeError` after three rejected codes.

    `main()` cannot catch it — `_run_login` is called above its `try` — so the
    ordinary "I mistyped the code" outcome was a raw traceback plus pending-task
    noise.
    """
    fake = FakeClient(
        start_error=RuntimeError("3 consecutive sign-in attempts failed. Aborting")
    )

    monkeypatch.setattr("src.main.build_client", lambda *args, **kwargs: fake)

    assert await _run_login("phone", config, logger) == 1

    assert fake.disconnected is True

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from telethon import TelegramClient, errors
from telethon.sessions import Session

from src.config import Config, TelegramConfig
from src.main import _build_parser, _login, _print_qr
from src.userbot import build_client

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
        self, *, authorized: bool = False, outcomes: list[object] | None = None
    ) -> None:
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
        return self.qr

    async def sign_in(self, password: str | None = None) -> FakeUser:
        self.passwords.append(password or "")

        return FakeUser("two-factor")

    def start(self):
        self.started = True

        async def _started() -> None:
            return None

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
    assert fake.disconnected is True
    assert fake.qr.waits == 1
    assert fake.qr.recreated == 0
    assert QR_URL in capsys.readouterr().out


async def test_qr_login_regenerates_an_expired_code(logger):
    fake = FakeClient(outcomes=[TimeoutError(), TimeoutError(), FakeUser("scanned")])

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.qr.waits == 3
    assert fake.qr.recreated == 2
    assert fake.disconnected is True


async def test_qr_login_gives_up_after_the_last_code(logger):
    fake = FakeClient(outcomes=[TimeoutError() for _ in range(6)])

    assert await _login(_as_client(fake), logger, "qr") == 1

    assert fake.qr.waits == 6
    assert fake.disconnected is True


async def test_qr_login_prompts_for_the_two_step_password(logger, monkeypatch):
    monkeypatch.setattr("src.main.getpass", lambda prompt="": "hunter2")

    fake = FakeClient(
        outcomes=[
            errors.SessionPasswordNeededError(request=None),
            FakeUser("two-factor"),
        ]
    )

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.passwords == ["hunter2"]
    assert fake.qr.recreated == 0


async def test_qr_login_short_circuits_an_authorized_session(logger):
    fake = FakeClient(authorized=True)

    assert await _login(_as_client(fake), logger, "qr") == 0

    assert fake.qr.waits == 0
    assert fake.disconnected is True


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

import asyncio
from types import SimpleNamespace
from typing import cast

import pytest
from telethon import TelegramClient

from src import main
from src.userbot import AuthorizationLostError


class _AsyncioWithFakeSleep:
    """`main`'s view of `asyncio`, with only `sleep` replaced.

    Patching `main.asyncio.sleep` would replace the attribute on the asyncio
    module itself and break every other awaiting library in the process, so the
    module reference `main` resolves is swapped instead.
    """

    def __init__(self, recorded: list[float]) -> None:
        self._recorded = recorded

    def __getattr__(self, name: str) -> object:
        return getattr(asyncio, name)

    async def sleep(self, delay: float) -> None:
        self._recorded.append(delay)

        if len(self._recorded) > 8:  # a runaway restart loop must fail, not hang
            raise asyncio.CancelledError


@pytest.fixture
def sleeps(monkeypatch) -> list[float]:
    recorded: list[float] = []

    monkeypatch.setattr(main, "asyncio", _AsyncioWithFakeSleep(recorded))

    return recorded


def _supervised(factory, logger, fatal):
    return main.supervise("probe", factory, logger, fatal)


async def _settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


async def test_supervise_restarts_a_crashing_task_with_doubling_backoff(logger, sleeps):
    calls = 0

    async def factory() -> None:
        nonlocal calls

        calls += 1

        if calls <= 2:
            raise RuntimeError("boom")

        await asyncio.Event().wait()  # the third attempt stays up

    fatal = asyncio.Event()
    task = asyncio.create_task(_supervised(factory, logger, fatal))

    await _settle()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert calls == 3
    assert sleeps == [1.0, 2.0]
    assert fatal.is_set() is False


async def test_supervise_restarts_a_task_that_returns(logger, sleeps):
    calls = 0

    async def factory() -> None:
        nonlocal calls

        calls += 1

        if calls >= 3:
            await asyncio.Event().wait()

    fatal = asyncio.Event()
    task = asyncio.create_task(_supervised(factory, logger, fatal))

    await _settle()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert calls == 3  # a loop returning is a crash, not a clean exit
    assert fatal.is_set() is False


async def test_supervise_lets_cancellation_through(logger, sleeps):
    started = asyncio.Event()

    async def factory() -> None:
        started.set()

        await asyncio.Event().wait()

    fatal = asyncio.Event()
    task = asyncio.create_task(_supervised(factory, logger, fatal))

    await started.wait()

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2.0)

    assert sleeps == []


async def test_supervise_stops_restarting_on_a_lost_authorization(logger, sleeps):
    """A revoked session must end the run instead of being retried forever."""
    calls = 0

    async def factory() -> None:
        nonlocal calls

        calls += 1

        raise AuthorizationLostError("SESSION_REVOKED")

    fatal = asyncio.Event()

    await asyncio.wait_for(_supervised(factory, logger, fatal), timeout=2.0)

    assert calls == 1
    assert fatal.is_set() is True
    assert sleeps == []


async def _fake_resolve_peer(client, chat_id, logger):
    return SimpleNamespace(chat_id=chat_id)


class _FakeBotApi:
    def __init__(self, cfg, logger) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


async def test_detect_exits_two_when_a_task_reports_a_lost_authorization(
    config, logger, tmp_path, monkeypatch
):
    """The fatal event has to become exit code 2.

    Without it a revoked session left the process up with dead notifications,
    exiting 0 whenever the operator finally stopped it.
    """
    store = main.StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)

    monkeypatch.setattr(main, "resolve_peer", _fake_resolve_peer)
    monkeypatch.setattr(main, "BotApiClient", _FakeBotApi)
    monkeypatch.setattr(main, "_install_signal_handlers", lambda stop, log: None)

    async def poll_loop(self) -> None:
        raise AuthorizationLostError("AUTH_KEY_UNREGISTERED")

    async def idle(self) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(main.Detector, "poll_loop", poll_loop)
    monkeypatch.setattr(main.Detector, "updates_loop", idle)
    monkeypatch.setattr(main.Detector, "upgrades_loop", idle)
    monkeypatch.setattr(main.StarGiftsStore, "run_saver", idle)

    code = await asyncio.wait_for(
        main._detect(cast(TelegramClient, None), store, config, logger), timeout=5.0
    )

    assert code == 2


async def test_detect_reports_a_broken_template_as_a_config_error(
    config, logger, tmp_path, monkeypatch
):
    """A template `KeyError` must be reported where it is raised.

    Catching it in `main()` reported a `KeyError` from `connect`, `resolve_peer`
    or `store.load` as a template problem and sent the operator to config.yaml.
    """
    raw = config.model_dump()
    raw["texts"]["notify"] = "{title} {nonexistent}"

    store = main.StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)

    monkeypatch.setattr(main, "resolve_peer", _fake_resolve_peer)
    monkeypatch.setattr(main, "BotApiClient", _FakeBotApi)

    code = await main._detect(
        cast(TelegramClient, None), store, main.Config.model_validate(raw), logger
    )

    assert code == 1

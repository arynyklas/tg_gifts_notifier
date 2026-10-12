"""`_run`, `--save-only` and `main()` driven end to end against a fake client.

The data file on disk is what the next start trusts, so the assertions are
about its bytes and contents, not about which helpers were called.
"""

from argparse import Namespace
import asyncio
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, tzinfo
import json
import logging
from pathlib import Path
import signal
import sys
from types import SimpleNamespace
from typing import Self, cast

import pytest
from telethon import TelegramClient, errors
from telethon.tl import functions, types
import yaml

from src import constants, main
from src.config import Config
from src.models import StarGiftData
from src.store import StarGiftsStore
from src.userbot import AuthorizationLostError, probe_upgradable

GIFT_LIMITED_ID = 20
GIFT_REGULAR_ID = 10
LEGACY_MESSAGE_ID = 555
ANNOUNCED_MESSAGE_ID = 777
DUMP_STAMP = "2026-10-11_18-32-26"


def _star_gift(gift_id: int, *, limited: bool) -> types.StarGift:
    return types.StarGift(
        id=gift_id,
        sticker=types.Document(
            id=gift_id,
            access_hash=gift_id * 7,
            file_reference=b"ref",
            date=datetime(2026, 1, 1, tzinfo=UTC),
            mime_type="application/x-tgsticker",
            size=1024,
            dc_id=2,
            attributes=[types.DocumentAttributeFilename(file_name=f"{gift_id}.tgs")],
        ),
        stars=gift_id * 10,
        convert_stars=gift_id * 5,
        limited=limited,
        sold_out=False,
        availability_remains=40 if limited else None,
        availability_total=100 if limited else None,
        first_sale_date=datetime(2026, 1, 2, tzinfo=UTC),
        last_sale_date=None,
    )


class FakeClient:
    """`TelegramClient` as `_run`, the catalog fetch and the probes use it."""

    def __init__(self) -> None:
        self.catalog: list[types.TypeStarGift] = [
            _star_gift(GIFT_LIMITED_ID, limited=True),
            _star_gift(GIFT_REGULAR_ID, limited=False),
        ]
        self.authorized = True
        self.connect_error: BaseException | None = None
        self.upgradable: set[int] = set()
        self.requests: list[object] = []
        self.probes: list[int] = []
        self.on_probe: Callable[[], None] | None = None
        # Not `disconnected`: Telethon exposes a future under that name.
        self.closed = False

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error

    def is_connected(self) -> bool:
        return not self.closed

    async def is_user_authorized(self) -> bool:
        return self.authorized

    async def disconnect(self) -> None:
        self.closed = True

    async def __call__(self, request: object) -> object:
        self.requests.append(request)

        if isinstance(request, functions.payments.GetStarGiftsRequest):
            return types.payments.StarGifts(
                hash=0, gifts=self.catalog, chats=[], users=[]
            )

        if isinstance(request, functions.payments.GetStarGiftUpgradePreviewRequest):
            self.probes.append(request.gift_id)

            if self.on_probe is not None:
                self.on_probe()

            if request.gift_id in self.upgradable:
                return types.payments.StarGiftUpgradePreview(
                    sample_attributes=[], prices=[], next_prices=[]
                )

            raise errors.BadRequestError(
                request=request, message="STARGIFT_UPGRADE_UNAVAILABLE"
            )

        raise AssertionError(f"unexpected request {request!r}")


class GaveUpClient(FakeClient):
    """Telethon after it gave up reconnecting, with the catalog request queued.

    Telethon never fails a request queued during its retries; only the next
    `connect()` starts a send loop that sends it, so the request parks until then.
    """

    def __init__(self) -> None:
        super().__init__()
        self.connects = 0
        self.gave_up = False
        self._sent = asyncio.Event()

    @property
    def disconnected(self) -> asyncio.Future[None]:
        future = asyncio.get_running_loop().create_future()
        future.set_exception(ConnectionError("Connection to Telegram failed 5 time(s)"))

        return future

    async def connect(self) -> None:
        await super().connect()

        self.connects += 1
        self.closed = False

        if self.gave_up:
            self._sent.set()

    async def __call__(self, request: object) -> object:
        if (
            isinstance(request, functions.payments.GetStarGiftsRequest)
            and not self.gave_up
        ):
            self.gave_up = True
            self.closed = True

            await self._sent.wait()

        return await super().__call__(request)


class FakeNotifier:
    """Records announcements the way the real one does: through the store."""

    def __init__(
        self, store: StarGiftsStore, announced: list[int], done: asyncio.Event
    ) -> None:
        self._store = store
        self._announced = announced
        self._done = done

    def validate_texts(self) -> None:
        pass

    async def announce_new_gift(self, gift: StarGiftData, sticker: object) -> bool:
        self._announced.append(gift.id)

        await self._store.set_message_ids(gift.id, message_id=ANNOUNCED_MESSAGE_ID)

        self._done.set()

        return True


class FakeBotApi:
    def __init__(self, cfg: object, logger: object) -> None:
        pass

    async def aclose(self) -> None:
        pass


class _FrozenDatetime(datetime):
    """Pins the dump file name so a test can block exactly that path."""

    @classmethod
    def now(cls, tz: tzinfo | None = None) -> Self:
        return cls(2026, 10, 11, 18, 32, 26, tzinfo=tz)


def _write_legacy(path: Path, *, upgradable: bool) -> bytes:
    """Write a v1 file: no `version`, dead sticker keys, legacy upgradability."""
    path.write_text(
        json.dumps(
            {
                "star_gifts": [
                    {
                        "id": GIFT_LIMITED_ID,
                        "number": 2,
                        "sticker_file_id": "CAACAgIAAxkBAA",
                        "sticker_file_name": f"{GIFT_LIMITED_ID}.tgs",
                        "price": 200,
                        "convert_price": 100,
                        "available_amount": 50,
                        "total_amount": 100,
                        "require_premium": False,
                        "user_limited": None,
                        "is_limited": True,
                        "first_appearance_timestamp": 1700000000,
                        "message_id": LEGACY_MESSAGE_ID,
                        "last_sale_timestamp": None,
                        "is_upgradable": upgradable,
                    },
                    {
                        "id": GIFT_REGULAR_ID,
                        "number": 1,
                        "sticker_file_id": "CAACAgIAAxkBAB",
                        "sticker_file_name": f"{GIFT_REGULAR_ID}.tgs",
                        "price": 100,
                        "convert_price": 50,
                        "available_amount": 0,
                        "total_amount": 0,
                        "is_limited": False,
                        "first_appearance_timestamp": 1600000000,
                        "message_id": None,
                        "is_upgradable": False,
                    },
                ]
            },
            indent=4,
        ),
        encoding="utf-8",
    )

    return path.read_bytes()


def _on_disk(path: Path) -> tuple[int, dict[int, dict]]:
    raw = json.loads(path.read_text(encoding="utf-8"))

    return raw.get("version", 1), {gift["id"]: gift for gift in raw["star_gifts"]}


def _failing_probes(monkeypatch: pytest.MonkeyPatch, error: BaseException) -> None:
    """Fail the regular gift's probe the way `is_gift_upgradable` surfaces it.

    Patched one level up from the client so a flood wait is not slept out.
    """

    async def probe(client: object, gift_id: int, logger: object) -> bool:
        if gift_id == GIFT_REGULAR_ID:
            raise error

        return True

    monkeypatch.setattr("src.userbot.is_gift_upgradable", probe)


# A flood wait and a dropped connection are startup failures (exit 1), a revoked
# session is an authorization failure (exit 2).
PROBE_FAILURES = [
    pytest.param(errors.FloodWaitError(request=None, capture=300), 1, id="flood"),
    pytest.param(ConnectionError("Cannot send requests"), 1, id="connection"),
    pytest.param(errors.AuthKeyUnregisteredError(request=None), 2, id="auth"),
]


@pytest.fixture
def data_path(tmp_path) -> Path:
    return tmp_path / "star_gifts.json"


@pytest.fixture
def cfg(config, data_path) -> Config:
    raw = config.model_dump()
    raw["storage"]["data_filepath"] = data_path

    return Config.model_validate(raw)


@pytest.fixture
def client(monkeypatch) -> FakeClient:
    fake = FakeClient()

    monkeypatch.setattr(main, "build_client", lambda cfg, logger, **_: fake)

    return fake


@pytest.fixture
def detected(monkeypatch) -> list[dict[int, bool]]:
    """Stand in for the detection phase; records the upgradability it got."""
    runs: list[dict[int, bool]] = []

    async def detect(client, store, cfg, logger) -> int:
        runs.append({gift.id: gift.is_upgradable for gift in store.snapshot()})

        return 0

    monkeypatch.setattr(main, "_detect", detect)

    return runs


@pytest.fixture
def default_sigint() -> Iterator[None]:
    """`asyncio.run` takes over Ctrl+C only from the default handler."""
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)

    yield

    signal.signal(signal.SIGINT, previous)


async def _start(cfg: Config, logger, *, save_only: bool = False) -> int:
    return await asyncio.wait_for(
        main._run(Namespace(save_only=save_only), cfg, logger), timeout=5
    )


async def test_an_unauthorized_session_exits_two_and_keeps_the_legacy_file(
    cfg, logger, data_path, client, detected
):
    """A failed first start used to persist the migration with nothing marked.

    The next start then skipped the baseline and announced every gift that was
    already upgradable.
    """
    legacy = _write_legacy(data_path, upgradable=False)
    client.authorized = False

    assert await _start(cfg, logger) == 2

    assert data_path.read_bytes() == legacy
    assert client.probes == []
    assert detected == []
    assert client.closed is True


async def test_migration_keeps_the_upgradability_legacy_recorded(
    cfg, logger, data_path, client, detected
):
    """Legacy tracked upgrades, so its flags are trusted rather than re-probed.

    A gift that became upgradable while neither version ran must stay
    unmarked, or its upgrade is never announced.
    """
    _write_legacy(data_path, upgradable=True)
    client.upgradable = {GIFT_LIMITED_ID, GIFT_REGULAR_ID}

    assert await _start(cfg, logger) == 0

    assert client.probes == []
    assert detected == [{GIFT_REGULAR_ID: False, GIFT_LIMITED_ID: True}]

    version, gifts = _on_disk(data_path)

    assert version == constants.SCHEMA_VERSION
    assert gifts[GIFT_LIMITED_ID]["is_upgradable"] is True
    assert gifts[GIFT_LIMITED_ID]["message_id"] == LEGACY_MESSAGE_ID
    assert gifts[GIFT_REGULAR_ID]["is_upgradable"] is False
    assert "sticker_file_id" not in gifts[GIFT_LIMITED_ID]


async def test_migration_baselines_a_legacy_file_that_marks_nothing_upgradable(
    cfg, logger, data_path, client, detected
):
    """A legacy run without upgrade checks left every gift unmarked.

    Without the baseline the first upgrades cycle announces all of them.
    """
    _write_legacy(data_path, upgradable=False)
    client.upgradable = {GIFT_LIMITED_ID}

    assert await _start(cfg, logger) == 0

    assert sorted(client.probes) == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]
    assert detected == [{GIFT_REGULAR_ID: False, GIFT_LIMITED_ID: True}]

    version, gifts = _on_disk(data_path)

    assert version == constants.SCHEMA_VERSION
    assert gifts[GIFT_LIMITED_ID]["is_upgradable"] is True
    assert gifts[GIFT_LIMITED_ID]["message_id"] == LEGACY_MESSAGE_ID
    assert gifts[GIFT_REGULAR_ID]["is_upgradable"] is False


@pytest.mark.parametrize(("error", "code"), PROBE_FAILURES)
async def test_a_failed_migration_baseline_keeps_the_legacy_file(
    cfg, logger, data_path, client, detected, monkeypatch, error, code
):
    """A failed probe is not "not upgradable", and a partial baseline is none.

    Persisting either made the next start announce the unprobed gifts.
    """
    legacy = _write_legacy(data_path, upgradable=False)

    _failing_probes(monkeypatch, error)

    assert await _start(cfg, logger) == code

    assert data_path.read_bytes() == legacy
    assert detected == []


async def test_save_only_writes_the_catalog_with_its_upgradability(
    cfg, logger, data_path, tmp_path, client, detected, monkeypatch
):
    monkeypatch.setattr(main, "datetime", _FrozenDatetime)

    _write_legacy(data_path, upgradable=True)
    client.upgradable = {GIFT_LIMITED_ID}

    assert await _start(cfg, logger, save_only=True) == 0

    # Each gift is probed once: save-only re-baselines everything, so the
    # migration baseline is skipped.
    assert sorted(client.probes) == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]
    assert detected == []

    version, gifts = _on_disk(data_path)

    assert version == constants.SCHEMA_VERSION
    assert sorted(gifts) == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]
    assert gifts[GIFT_LIMITED_ID]["is_upgradable"] is True
    assert gifts[GIFT_REGULAR_ID]["is_upgradable"] is False
    assert all(gift["message_id"] is None for gift in gifts.values())

    _, dumped = _on_disk(tmp_path / f"star_gifts_dump_{DUMP_STAMP}.json")

    assert dumped[GIFT_LIMITED_ID]["message_id"] == LEGACY_MESSAGE_ID


@pytest.mark.parametrize(("error", "code"), PROBE_FAILURES)
async def test_a_failed_save_only_baseline_keeps_the_data_file(
    cfg, logger, data_path, client, monkeypatch, error, code
):
    """Replacing first and probing second wrote the catalog with nothing marked."""
    legacy = _write_legacy(data_path, upgradable=True)

    _failing_probes(monkeypatch, error)

    assert await _start(cfg, logger, save_only=True) == code

    assert data_path.read_bytes() == legacy


async def test_a_failed_dump_aborts_save_only_before_fetching(
    cfg, logger, data_path, tmp_path, client, monkeypatch
):
    """The dump is the only other copy of the posted message ids."""
    monkeypatch.setattr(main, "datetime", _FrozenDatetime)

    # A directory where the dump goes makes its `os.replace` fail.
    (tmp_path / f"star_gifts_dump_{DUMP_STAMP}.json").mkdir()

    legacy = _write_legacy(data_path, upgradable=True)

    assert await _start(cfg, logger, save_only=True) == 1

    assert data_path.read_bytes() == legacy
    assert client.requests == []


async def test_a_failed_save_only_write_exits_one_and_keeps_the_data_file(
    cfg, logger, data_path, client, monkeypatch
):
    """Exit 0 here would report a baseline that never reached the disk."""
    legacy = _write_legacy(data_path, upgradable=True)
    original = StarGiftsStore._write_atomic

    # Only the data file fails: the dump must succeed to reach the final write.
    def failing(self: StarGiftsStore, payload: str) -> None:
        if self._path == data_path:
            raise OSError("disk is full")

        original(self, payload)

    monkeypatch.setattr(StarGiftsStore, "_write_atomic", failing)

    assert await _start(cfg, logger, save_only=True) == 1

    assert data_path.read_bytes() == legacy


@pytest.mark.parametrize(
    ("phase", "error", "code"),
    [
        pytest.param("connect", ConnectionError("Connection failed"), 1, id="offline"),
        pytest.param(
            "detect", errors.FloodWaitError(request=None, capture=300), 1, id="flood"
        ),
        pytest.param(
            "detect", errors.AuthKeyUnregisteredError(request=None), 2, id="auth"
        ),
        pytest.param("detect", AuthorizationLostError("revoked"), 2, id="lost"),
    ],
)
async def test_startup_failures_map_to_their_exit_codes(
    cfg, logger, client, monkeypatch, phase, error, code
):
    """They escaped `main()` as tracebacks and never reached the log file."""

    async def detect(client, store, cfg, logger) -> int:
        raise error

    if phase == "connect":
        client.connect_error = error

    else:
        monkeypatch.setattr(main, "_detect", detect)

    assert await _start(cfg, logger) == code

    assert client.closed is True


class _RecordingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


async def test_an_unexpected_startup_error_is_logged_with_its_traceback(
    cfg, logger, tmp_path, client, monkeypatch
):
    """Only RuntimeError was caught, so anything else bypassed the log file."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(cfg.model_dump(mode="json")), encoding="utf-8"
    )
    handler = _RecordingHandler()
    logger.addHandler(handler)

    async def resolve_peer(client, chat_id: int, logger) -> SimpleNamespace:
        raise ValueError("unexpected peer")

    monkeypatch.setattr(main, "resolve_peer", resolve_peer)
    monkeypatch.setattr(main, "get_logger", lambda name, cfg: logger)
    monkeypatch.setattr(sys, "argv", ["src.main", "--config", str(config_path)])

    assert await asyncio.wait_for(main.main(), timeout=5) == 1

    critical = [r for r in handler.records if r.levelno == logging.CRITICAL]

    assert [r.getMessage() for r in critical] == ["Startup failed"]
    assert critical[0].exc_info is not None
    assert isinstance(critical[0].exc_info[1], ValueError)
    assert client.closed is True


def test_ctrl_c_during_the_save_only_baseline_exits_zero_and_keeps_the_data_file(
    cfg, logger, data_path, tmp_path, client, monkeypatch, default_sigint
):
    """The interrupt used to flush a catalog with nothing marked upgradable.

    It also escaped as a KeyboardInterrupt traceback with exit 0xC000013A.
    """
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(cfg.model_dump(mode="json")), encoding="utf-8"
    )
    legacy = _write_legacy(data_path, upgradable=True)
    interrupted: list[bool] = []

    def interrupt() -> None:
        if not interrupted:
            interrupted.append(True)
            signal.raise_signal(signal.SIGINT)

    client.on_probe = interrupt
    client.upgradable = {GIFT_LIMITED_ID, GIFT_REGULAR_ID}

    monkeypatch.setattr(main, "get_logger", lambda name, cfg: logger)
    monkeypatch.setattr(
        sys, "argv", ["src.main", "--config", str(config_path), "--save-only"]
    )

    try:
        code = main._entrypoint()

    except KeyboardInterrupt:
        pytest.fail("Ctrl+C escaped as a KeyboardInterrupt")

    assert interrupted == [True]
    assert code == 0
    assert data_path.read_bytes() == legacy
    assert client.closed is True


async def test_an_interrupted_announcement_is_resumed_and_flushed_on_shutdown(
    cfg, logger, data_path, client, monkeypatch
):
    """Crash resume needs the seeding in `_detect` and the flush in `_run`.

    Without the seeding the gift is never announced; without the flush its
    message id is lost and the next start announces it a second time.
    """
    gifts = [
        StarGiftData(
            id=GIFT_LIMITED_ID,
            number=2,
            price=200,
            convert_price=100,
            available_amount=40,
            total_amount=100,
            is_limited=True,
            announce_pending=True,
        ),
        StarGiftData(
            id=GIFT_REGULAR_ID,
            number=1,
            price=100,
            convert_price=50,
            available_amount=0,
            total_amount=0,
            is_limited=False,
            message_id=LEGACY_MESSAGE_ID,
        ),
    ]
    data_path.write_text(
        json.dumps(
            {
                "version": constants.SCHEMA_VERSION,
                "star_gifts": [gift.model_dump() for gift in gifts],
            }
        ),
        encoding="utf-8",
    )

    announced: list[int] = []
    done = asyncio.Event()
    stops: list[asyncio.Event] = []

    def notifier(*, store: StarGiftsStore, **_: object) -> FakeNotifier:
        return FakeNotifier(store, announced, done)

    async def resolve_peer(client, chat_id: int, logger) -> SimpleNamespace:
        return SimpleNamespace(chat_id=chat_id)

    async def poll_once_then_idle(self) -> None:
        await self.poll_once()
        await asyncio.Event().wait()

    async def idle(self) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(main, "Notifier", notifier)
    monkeypatch.setattr(main, "BotApiClient", FakeBotApi)
    monkeypatch.setattr(main, "resolve_peer", resolve_peer)
    monkeypatch.setattr(main, "_install_signal_handlers", lambda s, _: stops.append(s))
    monkeypatch.setattr(main.Detector, "poll_loop", poll_once_then_idle)
    monkeypatch.setattr(main.Detector, "updates_loop", idle)
    monkeypatch.setattr(main.Detector, "upgrades_loop", idle)
    monkeypatch.setattr(main.Detector, "connection_loop", idle)
    # Only the shutdown flush may write, so its absence is observable.
    monkeypatch.setattr(main.StarGiftsStore, "run_saver", idle)

    run = asyncio.create_task(main._run(Namespace(save_only=False), cfg, logger))

    try:
        await asyncio.wait_for(done.wait(), timeout=5)

        stops[0].set()

        assert await asyncio.wait_for(run, timeout=5) == 0

    finally:
        run.cancel()

    assert announced == [GIFT_LIMITED_ID]

    _, on_disk = _on_disk(data_path)

    assert on_disk[GIFT_LIMITED_ID]["message_id"] == ANNOUNCED_MESSAGE_ID
    assert on_disk[GIFT_LIMITED_ID]["announce_pending"] is False


async def test_detection_recovers_a_poll_stuck_in_a_failed_reconnect(
    cfg, logger, monkeypatch
):
    """A request queued while Telethon retried is never answered once it gives up.

    The poll loop waiting on it never gets back to `ensure_connected`, so only the
    connection loop `_detect` runs beside it can reconnect and release it.
    """
    client = GaveUpClient()
    announced: list[int] = []
    done = asyncio.Event()
    stops: list[asyncio.Event] = []

    def notifier(*, store: StarGiftsStore, **_: object) -> FakeNotifier:
        return FakeNotifier(store, announced, done)

    async def resolve_peer(client, chat_id: int, logger) -> SimpleNamespace:
        return SimpleNamespace(chat_id=chat_id)

    async def idle(self) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(main, "build_client", lambda cfg, logger, **_: client)
    monkeypatch.setattr(main, "Notifier", notifier)
    monkeypatch.setattr(main, "BotApiClient", FakeBotApi)
    monkeypatch.setattr(main, "resolve_peer", resolve_peer)
    monkeypatch.setattr(main, "_install_signal_handlers", lambda s, _: stops.append(s))
    monkeypatch.setattr(main.Detector, "updates_loop", idle)
    monkeypatch.setattr(main.Detector, "upgrades_loop", idle)
    monkeypatch.setattr(main.StarGiftsStore, "run_saver", idle)
    monkeypatch.setattr(constants, "CONNECTION_CHECK_INTERVAL", 0.0)

    run = asyncio.create_task(main._run(Namespace(save_only=False), cfg, logger))

    try:
        await asyncio.wait_for(done.wait(), timeout=5)

        stops[0].set()

        assert await asyncio.wait_for(run, timeout=5) == 0

    finally:
        run.cancel()

    # The startup connect, then the connection loop's reconnect.
    assert client.connects == 2
    assert announced


async def test_a_config_error_never_prints_a_secret(
    example_config_dict, tmp_path, monkeypatch, capsys
):
    """`extra="forbid"` and type errors put the offending value in the message."""
    token = "123456:SECRET-IN-A-MISTYPED-KEY"
    api_hash = "SECRET-IN-A-WRONG-TYPE"
    raw = example_config_dict
    raw["telegram"]["bot_tokenz"] = token
    raw["telegram"]["api_hash"] = [api_hash]
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw), encoding="utf-8")

    monkeypatch.setattr(sys, "argv", ["src.main", "--config", str(config_path)])

    assert await main.main() == 1

    out, err = capsys.readouterr()

    assert "telegram -> bot_tokenz" in err
    assert "telegram -> api_hash" in err
    assert token not in out + err
    assert api_hash not in out + err


async def test_strict_probing_never_records_a_failed_probe(logger, monkeypatch):
    """The baseline must not store a gift it could not probe as not upgradable."""
    _failing_probes(monkeypatch, ConnectionError("Cannot send requests"))

    ids = [GIFT_REGULAR_ID, GIFT_LIMITED_ID]
    client = cast(TelegramClient, None)

    lenient = await probe_upgradable(client, ids, asyncio.Semaphore(2), logger)

    assert lenient == [GIFT_LIMITED_ID]

    with pytest.raises(ConnectionError):
        await probe_upgradable(client, ids, asyncio.Semaphore(2), logger, strict=True)


@pytest.mark.parametrize("strict", [True, False], ids=["strict", "lenient"])
@pytest.mark.parametrize(
    "earlier",
    [
        pytest.param(ConnectionError("Cannot send requests"), id="connection"),
        pytest.param(errors.FloodWaitError(request=None, capture=300), id="flood"),
    ],
)
async def test_probing_reports_a_lost_session_over_an_earlier_failure(
    logger, monkeypatch, earlier, strict
):
    """An auth error must win, or a revoked session exits 1 instead of 2.

    A flood wait on an earlier gift was raised first, in either mode.
    """

    async def probe(client: object, gift_id: int, logger: object) -> bool:
        if gift_id == GIFT_REGULAR_ID:
            raise earlier

        raise errors.AuthKeyUnregisteredError(request=None)

    monkeypatch.setattr("src.userbot.is_gift_upgradable", probe)

    with pytest.raises(errors.AuthKeyUnregisteredError):
        await probe_upgradable(
            cast(TelegramClient, None),
            [GIFT_REGULAR_ID, GIFT_LIMITED_ID],
            asyncio.Semaphore(2),
            logger,
            strict=strict,
        )


# Both orders, so one of them puts the flood wait first in the probe list
# whatever order the store hands its upgrade candidates out in.
@pytest.mark.parametrize("flooded", [GIFT_LIMITED_ID, GIFT_REGULAR_ID])
async def test_a_lost_session_behind_a_flood_wait_exits_two(
    cfg, logger, data_path, client, detected, monkeypatch, flooded
):
    """A revoked session must exit 2 even when another probe hit a flood wait.

    The flood wait was raised instead, and the run exited 1 as if a retry later
    could succeed.
    """
    legacy = _write_legacy(data_path, upgradable=False)

    async def probe(client: object, gift_id: int, logger: object) -> bool:
        if gift_id == flooded:
            raise errors.FloodWaitError(request=None, capture=300)

        raise errors.AuthKeyUnregisteredError(request=None)

    monkeypatch.setattr("src.userbot.is_gift_upgradable", probe)

    assert await _start(cfg, logger) == 2

    assert data_path.read_bytes() == legacy
    assert detected == []

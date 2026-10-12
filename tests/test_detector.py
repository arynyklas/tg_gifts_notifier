import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import replace
from datetime import UTC, datetime
import gc
import inspect
from io import BytesIO
from time import monotonic
from typing import Any, cast

import pytest
from telethon import TelegramClient, errors
from telethon.crypto import AuthKey
from telethon.errors.common import AuthKeyNotFound
from telethon.network.mtprotosender import MTProtoSender
from telethon.sessions import MemorySession
from telethon.tl import functions, types

from src import constants
from src.bot_api import BotApiClient, BotApiError
from src.config import Config
from src.detector import Detector, baseline_upgradability
from src.main import supervise
from src.models import StarGiftData
from src.notifier import Notifier
from src.store import StarGiftsStore
from src.userbot import (
    AuthorizationLostError,
    ensure_connected,
    fetch_star_gifts,
    is_gift_upgradable,
    probe_upgradable,
)

GIFT_LIMITED_ID = 200
GIFT_REGULAR_ID = 100
GIFT_SCARCE_ID = 300


def _document(doc_id: int) -> types.Document:
    return types.Document(
        id=doc_id,
        access_hash=doc_id * 7,
        file_reference=b"ref",
        date=datetime(2026, 1, 1, tzinfo=UTC),
        mime_type="application/x-tgsticker",
        size=1024,
        dc_id=2,
        attributes=[types.DocumentAttributeFilename(file_name=f"{doc_id}.tgs")],
    )


def _star_gift(
    gift_id: int,
    *,
    limited: bool,
    remains: int | None = None,
    total: int | None = None,
    upgrade_stars: int | None = None,
    sale_dates: tuple[datetime, datetime] | None = None,
) -> types.StarGift:
    """Build a gift the way Telegram sends it.

    `first_sale_date` and `last_sale_date` share TL `flags.1` with `sold_out`,
    so Telegram sends them for sold-out gifts only; `sale_dates` (first, last)
    marks the gift sold out.
    """
    first_sale, last_sale = sale_dates or (None, None)

    return types.StarGift(
        id=gift_id,
        sticker=_document(gift_id),
        stars=gift_id * 10,
        convert_stars=gift_id * 5,
        limited=limited,
        sold_out=sale_dates is not None,
        availability_remains=remains,
        availability_total=total,
        first_sale_date=first_sale,
        last_sale_date=last_sale,
        upgrade_stars=upgrade_stars,
    )


class FakeMessage:
    def __init__(self, message_id: int) -> None:
        self.id = message_id


class FakeClient:
    """Minimal stand-in for `TelegramClient` used by the detector hot path."""

    def __init__(self, payloads: list[list[types.TypeStarGift]]) -> None:
        self._payloads = payloads
        self.sent_stickers: list[dict[str, object]] = []
        self.downloads: list[int] = []
        self.requests: list[object] = []
        self.upgradable: set[int] = set()
        self.upgrade_errors: dict[int, BaseException] = {}
        self.upgrade_probes: list[int] = []
        self.poll_error: BaseException | None = None
        self.connected = True
        self.connects = 0
        self.connect_error: BaseException | None = None
        self.connect_gate: asyncio.Event | None = None
        self.disconnected: asyncio.Future[None] | None = None
        # Telethon keeps `is_connected()` true while it retries a dropped link
        # and queues what is sent meanwhile; only a new connection sends it.
        self.reconnecting = False
        self.queued: list[asyncio.Future[None]] = []
        self.fetch_gate: asyncio.Event | None = None
        self._next_message_id = 1000
        # `send_gift_sticker` revives dead per-DC file senders and bounds the
        # download by the flood-sleep threshold; neither exists on this fake.
        self._borrowed_senders: dict[int, object] = {}
        self.flood_sleep_threshold = 0

    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        if self.connect_error is not None:
            error, self.connect_error = self.connect_error, None

            raise error

        self.connects += 1

        if self.connect_gate is not None:
            await self.connect_gate.wait()

        self.connected = True

        # The new send loop drains what the old one left queued.
        for request in self.queued:
            request.set_result(None)

        self.queued.clear()

    async def disconnect(self) -> None:
        # Telethon's sender skips a connection it already gave up on, so the
        # reason stays readable from `disconnected`.
        self.connected = False

    def drop(self, reason: BaseException) -> None:
        """Disconnect for good, the way Telethon's sender does once it gives up.

        Like Telethon, this fails nothing still queued: those requests wait for
        the next `connect()`.
        """
        disconnected = asyncio.get_running_loop().create_future()
        disconnected.set_exception(reason)

        self.connected = False
        self.reconnecting = False
        self.disconnected = disconnected

    async def __call__(self, request: object) -> object:
        if not self.connected:
            raise ConnectionError("Cannot send requests while disconnected")

        if self.reconnecting:
            queued = asyncio.get_running_loop().create_future()
            self.queued.append(queued)

            await queued

        self.requests.append(request)

        if isinstance(request, functions.payments.GetStarGiftsRequest):
            if self.fetch_gate is not None:
                await self.fetch_gate.wait()

            if self.poll_error is not None:
                # One-shot, so a loop that stopped sleeping between failures
                # still reaches a cycle that ends the test instead of spinning
                # without ever yielding to the timeout.
                error, self.poll_error = self.poll_error, None

                raise error

            if not self._payloads:
                return types.payments.StarGiftsNotModified()

            gifts = self._payloads.pop(0)

            return types.payments.StarGifts(
                hash=0,
                gifts=gifts,
                chats=[],
                users=[],
            )

        if isinstance(request, functions.payments.GetStarGiftUpgradePreviewRequest):
            self.upgrade_probes.append(request.gift_id)

            error = self.upgrade_errors.get(request.gift_id)

            if error is not None:
                raise error

            if request.gift_id in self.upgradable:
                return types.payments.StarGiftUpgradePreview(
                    sample_attributes=[],
                    prices=[],
                    next_prices=[],
                )

            raise errors.BadRequestError(
                request=request,
                message="STARGIFT_UPGRADE_UNAVAILABLE",
            )

        raise AssertionError(f"unexpected request {request!r}")

    async def download_media(
        self, document: types.Document, file: type[bytes]
    ) -> bytes:
        assert file is bytes

        self.downloads.append(document.id)

        return f"tgs-bytes-of-{document.id}".encode()

    async def send_file(
        self,
        entity: object,
        file: BytesIO,
        *,
        mime_type: str,
        attributes: list[types.TypeDocumentAttribute],
        force_document: bool,
    ) -> FakeMessage:
        self.sent_stickers.append(
            {
                "entity": entity,
                "payload": file.read(),
                "name": file.name,
                "mime_type": mime_type,
                "attributes": attributes,
                "force_document": force_document,
            }
        )
        self._next_message_id += 1

        return FakeMessage(self._next_message_id)


class FakeBotApi:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.edited: list[dict[str, Any]] = []
        self.fail_send_times = 0
        self.fail_edit_times = 0
        self._next_message_id = 5000

    async def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
    ) -> int:
        if self.fail_send_times > 0:
            self.fail_send_times -= 1

            raise BotApiError(500, "internal server error")

        self.sent.append(
            {
                "chat_id": chat_id,
                "text": text,
                "reply_to_message_id": reply_to_message_id,
            }
        )
        self._next_message_id += 1

        return self._next_message_id

    async def edit_message_text(self, chat_id: int, message_id: int, text: str) -> None:
        if self.fail_edit_times > 0:
            self.fail_edit_times -= 1

            raise BotApiError(500, "internal server error")

        self.edited.append({"chat_id": chat_id, "message_id": message_id, "text": text})


class _StopLoopError(Exception):
    """Raised by a fake sleep to end a detector loop at that sleep."""


class _AsyncioWithSleep:
    """A module's view of `asyncio`, with only `sleep` replaced.

    Patching `asyncio.sleep` itself would replace it for every awaiting library
    in the process (see tests/test_main.py), so the module reference is swapped.
    """

    def __init__(self, sleep: Callable[[float], Awaitable[None]]) -> None:
        self.sleep = sleep

    def __getattr__(self, name: str) -> object:
        return getattr(asyncio, name)


def _build(
    config: Config,
    logger,
    tmp_path,
    payloads: list[list[types.TypeStarGift]],
    *,
    check_upgrades: bool = False,
) -> tuple[Detector, FakeClient, FakeBotApi, StarGiftsStore]:
    client = FakeClient(payloads)
    bot_api = FakeBotApi()
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)
    notifier = Notifier(
        client=cast(TelegramClient, client),
        bot_api=cast(BotApiClient, bot_api),
        store=store,
        cfg=config,
        notify_peer=cast(types.TypeInputPeer, types.InputPeerEmpty()),
        upgrades_peer=cast(types.TypeInputPeer, types.InputPeerEmpty()),
        logger=logger,
    )
    detector = Detector(
        client=cast(TelegramClient, client),
        notifier=notifier,
        store=store,
        cfg=config,
        logger=logger,
        check_upgrades=check_upgrades,
    )

    return detector, client, bot_api, store


@pytest.fixture
def first_payload() -> list[types.TypeStarGift]:
    return [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
    ]


@pytest.fixture
def loop_sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Record the detector loops' sleeps and end the loop at the first one."""
    slept: list[float] = []

    async def sleep(delay: float) -> None:
        slept.append(delay)

        raise _StopLoopError

    monkeypatch.setattr("src.detector.asyncio", _AsyncioWithSleep(sleep))

    return slept


async def test_stickers_are_reuploaded_the_way_production_posts_them(
    config, logger, tmp_path, first_payload
):
    """Gift stickers are custom emoji documents: sending them by reference fails.

    The bytes are downloaded and re-uploaded instead, and the resulting message
    must match what the pyrogram deployment posted in `@gifts_detector`: an
    `application/x-tgsticker` upload carrying only a filename attribute.
    """
    detector, client, _, _ = _build(config, logger, tmp_path, [first_payload])

    assert await detector.poll_once() is True

    assert client.downloads == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]

    for gift_id, upload in zip(
        (GIFT_REGULAR_ID, GIFT_LIMITED_ID), client.sent_stickers, strict=True
    ):
        attributes = cast(list[types.TypeDocumentAttribute], upload["attributes"])

        assert upload["payload"] == f"tgs-bytes-of-{gift_id}".encode()
        assert upload["mime_type"] == "application/x-tgsticker"
        assert upload["force_document"] is False
        assert upload["name"] == f"{gift_id}.tgs"
        assert [type(attr) for attr in attributes] == [types.DocumentAttributeFilename]
        assert cast(types.DocumentAttributeFilename, attributes[0]).file_name == (
            f"{gift_id}.tgs"
        )


async def test_a_hung_sticker_download_fails_the_announcement_for_a_retry(
    config, logger, tmp_path, monkeypatch: pytest.MonkeyPatch
):
    """A download queued on a per-DC file sender that then gives up is never
    answered, and `Detector.connection_loop` only watches the main client.

    Unbounded, the announcing loop hung forever; bounded, the `TimeoutError`
    (an `OSError`) fails the announcement and the sticker is posted on a retry.
    """
    detector, client, bot_api, _ = _build(config, logger, tmp_path, [])
    never = asyncio.Event()

    async def hung_download(document: types.Document, file: type[bytes]) -> bytes:
        await never.wait()

        raise AssertionError("unreachable")

    monkeypatch.setattr(client, "download_media", hung_download)
    monkeypatch.setattr(constants, "STICKER_DOWNLOAD_TIMEOUT", 0.05)
    notifier = detector._notifier
    gift = StarGiftData(
        id=GIFT_REGULAR_ID,
        number=1,
        price=1000,
        convert_price=500,
        available_amount=0,
        total_amount=0,
        is_limited=False,
    )

    announced = await asyncio.wait_for(
        notifier.announce_new_gift(gift, _document(GIFT_REGULAR_ID)), timeout=5
    )

    assert announced is False
    assert gift.sticker_message_id is None
    assert client.sent_stickers == []
    assert bot_api.sent == []


async def test_first_poll_announces_every_new_gift(
    config, logger, tmp_path, first_payload
):
    detector, client, bot_api, store = _build(config, logger, tmp_path, [first_payload])

    assert await detector.poll_once() is True

    assert len(client.sent_stickers) == 2
    assert len(bot_api.sent) == 2
    assert [gift.id for gift in store.snapshot()] == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]

    for gift in store.snapshot():
        assert gift.sticker_message_id is not None
        assert gift.message_id is not None

    # Each text replies to the sticker posted for that same gift.
    for gift in store.snapshot():
        sent = next(
            item for item in bot_api.sent if f"<code>{gift.id}</code>" in item["text"]
        )

        assert sent["reply_to_message_id"] == gift.sticker_message_id


async def test_new_gifts_are_announced_by_ascending_total_amount(
    config, logger, tmp_path
):
    # `fetch_star_gifts` yields the catalog by id, so the totals must disagree
    # with the id order or the sort is never exercised.
    payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
        _star_gift(GIFT_SCARCE_ID, limited=True, remains=5, total=10),
    ]
    detector, _, bot_api, _ = _build(config, logger, tmp_path, [payload])

    await detector.poll_once()

    # The unlimited gift has total_amount 0, so it goes first.
    expected = [GIFT_REGULAR_ID, GIFT_SCARCE_ID, GIFT_LIMITED_ID]

    for sent, gift_id in zip(bot_api.sent, expected, strict=True):
        assert f"<code>{gift_id}</code>" in sent["text"]


async def test_stock_drop_produces_exactly_one_edit(
    config, logger, tmp_path, first_payload
):
    second_payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=25, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
    ]
    detector, _, bot_api, store = _build(
        config, logger, tmp_path, [first_payload, second_payload]
    )

    await detector.poll_once()
    await detector.poll_once()

    limited = store.get(GIFT_LIMITED_ID)

    assert limited is not None
    assert limited.available_amount == 25

    assert await detector.drain_updates_once() == 1
    assert len(bot_api.edited) == 1
    assert bot_api.edited[0]["message_id"] == limited.message_id
    assert "25" in bot_api.edited[0]["text"]


async def test_repeated_queueing_coalesces_into_one_edit(
    config, logger, tmp_path, first_payload
):
    payloads: list[list[types.TypeStarGift]] = [
        first_payload,
        [
            _star_gift(GIFT_LIMITED_ID, limited=True, remains=30, total=100),
            _star_gift(GIFT_REGULAR_ID, limited=False),
        ],
        [
            _star_gift(GIFT_LIMITED_ID, limited=True, remains=20, total=100),
            _star_gift(GIFT_REGULAR_ID, limited=False),
        ],
    ]
    detector, _, bot_api, store = _build(config, logger, tmp_path, payloads)

    await detector.poll_once()
    await detector.poll_once()
    await detector.poll_once()

    assert await detector.drain_updates_once() == 1
    assert len(bot_api.edited) == 1
    assert "20" in bot_api.edited[0]["text"]

    limited = store.get(GIFT_LIMITED_ID)

    assert limited is not None
    assert limited.available_amount == 20


async def test_a_drained_drop_is_not_edited_again(config, logger, tmp_path):
    """Each drain takes its batch over; the next one edits only later drops.

    Copying the pending set instead of swapping it out re-edited every gift that
    ever dropped on every later drain, and the set grew without bound.
    """
    payloads: list[list[types.TypeStarGift]] = [
        [
            _star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100),
            _star_gift(GIFT_SCARCE_ID, limited=True, remains=5, total=10),
        ],
        [
            _star_gift(GIFT_LIMITED_ID, limited=True, remains=25, total=100),
            _star_gift(GIFT_SCARCE_ID, limited=True, remains=5, total=10),
        ],
        [
            _star_gift(GIFT_LIMITED_ID, limited=True, remains=25, total=100),
            _star_gift(GIFT_SCARCE_ID, limited=True, remains=4, total=10),
        ],
    ]
    detector, _, bot_api, store = _build(config, logger, tmp_path, payloads)

    await detector.poll_once()
    await detector.poll_once()

    assert await asyncio.wait_for(detector.drain_updates_once(), timeout=5) == 1

    await detector.poll_once()

    assert await asyncio.wait_for(detector.drain_updates_once(), timeout=5) == 1

    limited = store.get(GIFT_LIMITED_ID)
    scarce = store.get(GIFT_SCARCE_ID)

    assert limited is not None
    assert scarce is not None
    assert [edit["message_id"] for edit in bot_api.edited] == [
        limited.message_id,
        scarce.message_id,
    ]


async def test_the_sold_out_duration_runs_from_telegrams_first_sale_date(
    config, logger, tmp_path, monkeypatch
):
    """Telegram sends `first_sale_date` only once a gift has sold out.

    Until then the parser can only record when the gift was first seen, which
    is late for a gift first seen after downtime or mid-sale at a migration, so
    the final edit must measure from Telegram's date, as legacy did.
    """
    first_sale = datetime(2026, 1, 2, tzinfo=UTC)
    first_seen = datetime(2026, 1, 2, 6, tzinfo=UTC)
    last_sale = datetime(2026, 1, 2, 6, 12, tzinfo=UTC)
    payloads: list[list[types.TypeStarGift]] = [
        [_star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100)],
        [
            _star_gift(
                GIFT_LIMITED_ID,
                limited=True,
                remains=0,
                total=100,
                sale_dates=(first_sale, last_sale),
            )
        ],
    ]
    detector, _, bot_api, _ = _build(config, logger, tmp_path, payloads)

    monkeypatch.setattr(
        "src.utils.current_timestamp", lambda: int(first_seen.timestamp())
    )

    await detector.poll_once()
    await detector.poll_once()

    assert await asyncio.wait_for(detector.drain_updates_once(), timeout=5) == 1

    # Measured from the first-seen time it would read "12 minutes".
    assert "sold out in 6 hours and 12 minutes" in bot_api.edited[0]["text"]


async def test_failed_text_send_is_retried_without_a_second_sticker(
    config, logger, tmp_path, first_payload, monkeypatch
):
    second_payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
    ]
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload, second_payload]
    )

    bot_api.fail_send_times = 2  # both gifts fail their text send

    await detector.poll_once()

    assert len(client.sent_stickers) == 2
    assert bot_api.sent == []

    for gift in store.snapshot():
        assert gift.sticker_message_id is not None
        assert gift.message_id is None

    # The failed announcements are armed for a later cycle, not the same one.
    now = monotonic()
    monkeypatch.setattr("src.detector.monotonic", lambda: now + 5.0)

    await detector.poll_once()

    assert len(client.sent_stickers) == 2  # no duplicate stickers
    assert len(bot_api.sent) == 2

    for gift in store.snapshot():
        assert gift.message_id is not None

    # Retried texts still reply to the sticker posted in the first cycle.
    assert all(sent["reply_to_message_id"] is not None for sent in bot_api.sent)


async def test_unmodified_payload_is_a_no_op(config, logger, tmp_path, first_payload):
    detector, client, bot_api, _ = _build(config, logger, tmp_path, [first_payload])

    assert await detector.poll_once() is True
    assert await detector.poll_once() is False
    assert len(client.sent_stickers) == 2
    assert len(bot_api.sent) == 2


async def test_upgrade_is_announced_once_and_probe_errors_are_skipped(
    config, logger, tmp_path, first_payload
):
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    await detector.poll_once()

    client.upgradable = {GIFT_LIMITED_ID}
    client.upgrade_errors = {GIFT_REGULAR_ID: ConnectionError("dropped")}

    assert await detector.check_upgrades_once() == 1

    limited = store.get(GIFT_LIMITED_ID)
    regular = store.get(GIFT_REGULAR_ID)

    assert limited is not None
    assert regular is not None
    assert limited.is_upgradable is True
    assert regular.is_upgradable is False  # probe error must not change state

    upgrade_text = bot_api.sent[-1]

    assert upgrade_text["chat_id"] == config.upgrades.chat_id
    assert (
        upgrade_text["text"] == f"Gift is upgradable! (<code>{GIFT_LIMITED_ID}</code>)"
    )
    assert len(client.sent_stickers) == 3  # two announcements plus the upgrade

    # The upgradable gift is marked, and the one whose probe raised got the same
    # cooldown as a negative answer, so the very next cycle asks Telegram nothing.
    client.upgrade_probes.clear()

    assert await detector.check_upgrades_once() == 0
    assert client.upgrade_probes == []


async def test_upgrade_is_not_marked_when_the_notification_fails(
    config, logger, tmp_path, first_payload
):
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    await detector.poll_once()

    client.upgradable = {GIFT_LIMITED_ID, GIFT_REGULAR_ID}
    bot_api.fail_send_times = 1

    assert await detector.check_upgrades_once() == 1

    failed = store.get(GIFT_REGULAR_ID)
    delivered = store.get(GIFT_LIMITED_ID)

    assert failed is not None
    assert delivered is not None
    assert failed.is_upgradable is False  # announcement failed, retried next cycle
    assert delivered.is_upgradable is True


async def test_unchanged_polls_leave_nothing_to_save(
    config, logger, tmp_path, first_payload
):
    """An identical catalog must not schedule a write.

    The catalog is polled every second and the payload is nearly always the
    same; marking the store dirty per poll rewrote the whole file every debounce
    window for the process lifetime.
    """
    same_payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
    ]
    detector, _, _, store = _build(
        config, logger, tmp_path, [first_payload, same_payload]
    )

    await detector.poll_once()
    await store.flush()

    assert store.is_dirty is False

    assert await detector.poll_once() is True  # a full payload was processed
    assert store.is_dirty is False  # but nothing in it changed


async def test_stock_change_still_schedules_a_save(
    config, logger, tmp_path, first_payload
):
    sold_payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=39, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
    ]
    detector, _, _, store = _build(
        config, logger, tmp_path, [first_payload, sold_payload]
    )

    await detector.poll_once()
    await store.flush()

    await detector.poll_once()

    assert store.is_dirty is True


async def test_baselined_gifts_are_still_probed_for_upgrades(
    config, logger, tmp_path, first_payload
):
    """`--save-only` leaves every gift without a `message_id`.

    Requiring one to probe upgradability silenced the upgrades chat for the whole
    baselined catalog, so probing must depend only on `is_upgradable`.
    """
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    # Exactly what `--save-only` does: store the catalog, notify nothing.
    parsed = await fetch_star_gifts(cast(TelegramClient, client), logger)

    assert parsed is not None

    await store.replace_all(gift.data for gift in parsed.values())

    assert all(gift.message_id is None for gift in store.snapshot())

    client.upgradable = {GIFT_LIMITED_ID}

    assert await detector.check_upgrades_once() == 1
    assert sorted(client.upgrade_probes) == sorted([GIFT_LIMITED_ID, GIFT_REGULAR_ID])

    limited = store.get(GIFT_LIMITED_ID)

    assert limited is not None
    assert limited.is_upgradable is True
    assert bot_api.sent[-1]["chat_id"] == config.upgrades.chat_id


async def test_baseline_records_upgradability_without_announcing(
    config, logger, tmp_path, first_payload
):
    """`--save-only` must baseline upgradability, not just message state.

    Otherwise the first upgrades cycle announces every gift that is already
    upgradable; against the live catalog that was 118 of 149 gifts.
    """
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    parsed = await fetch_star_gifts(cast(TelegramClient, client), logger)

    assert parsed is not None

    await store.replace_all(gift.data for gift in parsed.values())

    client.upgradable = {GIFT_LIMITED_ID}

    upgradable = await baseline_upgradability(
        cast(TelegramClient, client),
        list(parsed),
        config,
        logger,
    )

    assert upgradable == [GIFT_LIMITED_ID]

    # The baseline only reports; recording is the caller's job.
    for gift_id in upgradable:
        await store.mark_upgradable(gift_id)

    limited = store.get(GIFT_LIMITED_ID)
    regular = store.get(GIFT_REGULAR_ID)

    assert limited is not None
    assert regular is not None
    assert limited.is_upgradable is True
    assert regular.is_upgradable is False

    # Nothing was announced, and the next cycle has nothing left to say.
    assert bot_api.sent == []
    assert client.sent_stickers == []
    assert await detector.check_upgrades_once() == 0


async def test_a_failed_upgrade_text_never_posts_a_second_sticker(
    config, logger, tmp_path, first_payload
):
    """The upgrade sticker id must be persisted before its text is sent.

    Keeping it in a local meant a failed text left the gift a candidate, and
    every following cycle posted another sticker.
    """
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    await detector.poll_once()

    stickers_after_announcements = len(client.sent_stickers)

    client.upgradable = {GIFT_LIMITED_ID}
    bot_api.fail_send_times = 1

    assert await detector.check_upgrades_once() == 0

    limited = store.get(GIFT_LIMITED_ID)

    assert limited is not None
    assert limited.is_upgradable is False
    assert limited.upgrade_sticker_message_id is not None
    assert len(client.sent_stickers) == stickers_after_announcements + 1

    posted = limited.upgrade_sticker_message_id

    assert await detector.check_upgrades_once() == 1

    # The retry posts the text only, replying to the sticker from the first try.
    assert len(client.sent_stickers) == stickers_after_announcements + 1
    assert limited.upgrade_sticker_message_id == posted
    assert bot_api.sent[-1]["reply_to_message_id"] == posted
    assert limited.is_upgradable is True


async def test_a_gift_without_an_upgrade_price_waits_out_the_probe_cooldown(
    config, logger, tmp_path, first_payload, monkeypatch
):
    """Probing every not-upgradable gift every cycle is what earned a FLOOD_WAIT.

    A gift whose payload advertises no upgrade price is probed once, then not
    again until `NEGATIVE_PROBE_COOLDOWN` has passed.
    """
    detector, client, _, _ = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    now = monotonic()
    monkeypatch.setattr("src.detector.monotonic", lambda: now)

    await detector.poll_once()

    assert await detector.check_upgrades_once() == 0
    assert sorted(client.upgrade_probes) == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]

    client.upgrade_probes.clear()

    assert await detector.check_upgrades_once() == 0
    assert client.upgrade_probes == []

    monkeypatch.setattr(
        "src.detector.monotonic",
        lambda: now + constants.NEGATIVE_PROBE_COOLDOWN + 1.0,
    )

    assert await detector.check_upgrades_once() == 0
    assert sorted(client.upgrade_probes) == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]


async def test_a_never_probed_gift_is_due_right_after_boot(
    config, logger, tmp_path, first_payload, monkeypatch
):
    """`monotonic()` has no fixed origin; on Linux it counts from boot.

    Measuring a never-probed gift against 0.0 kept every gift without an upgrade
    price out of the upgrades cycle for `NEGATIVE_PROBE_COOLDOWN` after boot.
    """
    detector, client, _, _ = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    monkeypatch.setattr("src.detector.monotonic", lambda: 1.0)

    await detector.poll_once()

    client.upgradable = {GIFT_LIMITED_ID}

    assert await detector.check_upgrades_once() == 1
    assert sorted(client.upgrade_probes) == [GIFT_REGULAR_ID, GIFT_LIMITED_ID]


async def test_a_gift_with_an_upgrade_price_is_probed_every_cycle(
    config, logger, tmp_path
):
    payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False, upgrade_stars=25),
    ]
    detector, client, _, _ = _build(
        config, logger, tmp_path, [payload], check_upgrades=True
    )

    await detector.poll_once()

    assert await detector.check_upgrades_once() == 0

    client.upgrade_probes.clear()

    assert await detector.check_upgrades_once() == 0
    assert client.upgrade_probes == [GIFT_REGULAR_ID]


async def test_an_interrupted_announcement_is_resumed_on_the_next_start(
    config, logger, tmp_path
):
    """A crash between `add()` and the text send must not lose the gift.

    `_announce_new` only looks at gifts absent from the store, so without the
    seeding the notification is lost forever and the gift can never be edited.
    """
    payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=40, total=100)
    ]
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [payload], check_upgrades=False
    )

    interrupted = StarGiftData(
        id=GIFT_LIMITED_ID,
        number=1,
        price=10,
        convert_price=5,
        available_amount=40,
        total_amount=100,
        is_limited=True,
        announce_pending=True,
    )

    await store.replace_all([interrupted])

    assert detector.seed_pending_announcements() == 1

    await detector.poll_once()

    stored = store.get(GIFT_LIMITED_ID)

    assert stored is not None
    assert stored.message_id is not None
    assert stored.announce_pending is False
    assert len(client.sent_stickers) == 1
    assert len(bot_api.sent) == 1


async def _cancelled_by_its_owner() -> None:
    """Await a future its owner cancels, as Telethon's `disconnect()` does."""
    doomed = asyncio.get_running_loop().create_future()

    asyncio.get_running_loop().call_soon(doomed.cancel)

    await doomed


async def _unexpected_error() -> None:
    raise RuntimeError("an error no handler expects")


@pytest.mark.parametrize("crash", [_cancelled_by_its_owner, _unexpected_error])
async def test_an_announcement_that_crashed_the_poll_loop_is_retried_on_restart(
    config, logger, tmp_path, first_payload, monkeypatch, crash
):
    """The gift is stored before it is announced, and retries live in memory.

    A crash skipped scheduling the retry, and after the supervise restart the
    gift was no longer new, so its announcement waited for the next process
    start. Telegram returns the full catalog every poll (`hash=0`).
    """
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload, first_payload]
    )
    crashes = [crash]
    download_media = client.download_media

    async def crashing_download(document: types.Document, file: type[bytes]) -> bytes:
        if crashes:
            await crashes.pop()()

        return await download_media(document, file)

    monkeypatch.setattr(client, "download_media", crashing_download)

    async def restart_delay(delay: float) -> None:
        await asyncio.sleep(0)

    polled_after_restart = asyncio.Event()

    async def poll_interval(delay: float) -> None:
        # The first cycle crashes before its sleep, so this is the restart's.
        polled_after_restart.set()

        await asyncio.Event().wait()  # parked until the test cancels it

    monkeypatch.setattr("src.main.asyncio", _AsyncioWithSleep(restart_delay))
    monkeypatch.setattr("src.detector.asyncio", _AsyncioWithSleep(poll_interval))

    poll = asyncio.create_task(
        supervise("poll", detector.poll_loop, logger, asyncio.Event())
    )

    try:
        await asyncio.wait_for(polled_after_restart.wait(), timeout=5)

    finally:
        poll.cancel()
        done, _ = await asyncio.wait({poll}, timeout=5)

    assert poll in done
    assert crashes == []

    for gift_id in (GIFT_LIMITED_ID, GIFT_REGULAR_ID):
        gift = store.get(gift_id)

        assert gift is not None
        assert gift.message_id is not None
        assert gift.announce_pending is False

    assert len(bot_api.sent) == 2


async def test_a_baselined_catalog_announces_nothing_on_start(
    config, logger, tmp_path, first_payload
):
    """`--save-only` leaves `announce_pending` false for the whole catalog."""
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=False
    )

    parsed = await fetch_star_gifts(cast(TelegramClient, client), logger)

    assert parsed is not None

    await store.replace_all(gift.data for gift in parsed.values())

    assert detector.seed_pending_announcements() == 0

    await detector.poll_once()

    assert client.sent_stickers == []
    assert bot_api.sent == []


async def test_a_failed_stock_edit_is_re_queued(
    config, logger, tmp_path, first_payload
):
    """Dropping the edit leaves a stale stock number until the gift's next drop.

    For the final drop before sell-out that next drop never comes.
    """
    second_payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=25, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
    ]
    detector, _, bot_api, store = _build(
        config, logger, tmp_path, [first_payload, second_payload]
    )

    await detector.poll_once()
    await detector.poll_once()

    bot_api.fail_edit_times = 1

    assert await detector.drain_updates_once() == 0
    assert bot_api.edited == []

    limited = store.get(GIFT_LIMITED_ID)

    assert limited is not None

    assert await detector.drain_updates_once() == 1
    assert bot_api.edited[0]["message_id"] == limited.message_id
    assert "25" in bot_api.edited[0]["text"]


async def test_a_revoked_session_stops_the_poll_loop(config, logger, tmp_path):
    """`UnauthorizedError` is an `RPCError`, so the generic handler retried it.

    The loop then ran forever with notifications dead and exited 0 on Ctrl+C.
    """
    detector, client, _, _ = _build(config, logger, tmp_path, [])

    client.poll_error = errors.AuthKeyUnregisteredError(request=None)

    # Bounded: without the fatal clause the loop retries forever, and the
    # timeout has to be the failure rather than a hung suite.
    with pytest.raises(AuthorizationLostError):
        await asyncio.wait_for(detector.poll_loop(), timeout=2.0)


async def test_a_dropped_userbot_is_reconnected_by_the_next_poll(
    config, logger, tmp_path, first_payload, loop_sleeps
):
    """Telethon stops reconnecting after `connection_retries` failed attempts.

    Every request then raises `ConnectionError` and nothing in Telethon connects
    again, so a network outage of a few minutes silenced the notifier for good.
    """
    detector, client, bot_api, _ = _build(config, logger, tmp_path, [first_payload])

    client.drop(ConnectionError("Connection to Telegram failed 5 time(s)"))

    # The loop ends at the fake sleep after its first cycle.
    with pytest.raises(_StopLoopError):
        await asyncio.wait_for(detector.poll_loop(), timeout=5)

    assert client.connects == 1
    assert len(bot_api.sent) == 2  # the poll after the reconnect went through


async def test_a_forgotten_auth_key_stops_the_poll_loop(
    config, logger, tmp_path, loop_sleeps
):
    """Telethon reports it only through `client.disconnected`.

    A request in flight at that moment gets `AuthKeyNotFound`; with none in
    flight every later request is a plain `ConnectionError`, which the loop
    retried forever instead of exiting for a new login.
    """
    detector, client, _, _ = _build(config, logger, tmp_path, [])

    client.drop(AuthKeyNotFound())

    with pytest.raises(AuthorizationLostError):
        await asyncio.wait_for(detector.poll_loop(), timeout=5)

    assert client.connects == 0


async def test_the_connection_loop_frees_a_poll_queued_during_a_failed_reconnect(
    config, logger, tmp_path, first_payload, monkeypatch
):
    """`is_connected()` stays true while Telethon retries a dropped connection.

    A request sent then is queued, and when Telethon gives up it fails only the
    requests already sent: the queued one is never answered. The poll loop waited
    on it forever and never got back to `ensure_connected`, so the notifier went
    silent for good even after the network came back.
    """
    detector, client, bot_api, _ = _build(config, logger, tmp_path, [first_payload])

    async def sleep(delay: float) -> None:
        if delay == constants.CONNECTION_CHECK_INTERVAL:
            await asyncio.sleep(0)  # the watchdog checks on every pass

            return

        raise _StopLoopError  # the poll loop ends after its first cycle

    monkeypatch.setattr("src.detector.asyncio", _AsyncioWithSleep(sleep))

    client.reconnecting = True
    poll = asyncio.create_task(detector.poll_loop())

    await asyncio.wait_for(_until_parked([poll]), timeout=5)

    assert len(client.queued) == 1

    client.drop(ConnectionError("Connection to Telegram failed 5 time(s)"))

    for _ in range(10):
        await asyncio.sleep(0)

    # Stuck: nothing in the poll loop can reconnect from here.
    assert not poll.done()
    assert client.connects == 0

    watchdog = asyncio.create_task(detector.connection_loop())

    try:
        with pytest.raises(_StopLoopError):
            await asyncio.wait_for(poll, timeout=5)

    finally:
        watchdog.cancel()
        done, _ = await asyncio.wait({watchdog}, timeout=5)

    assert watchdog in done
    assert client.connects == 1
    assert len(bot_api.sent) == 2  # the stuck poll went through after all


async def test_a_failed_reconnect_keeps_the_connection_loop_running(
    config, logger, tmp_path, loop_sleeps
):
    detector, client, _, _ = _build(config, logger, tmp_path, [])

    client.drop(ConnectionError("Connection to Telegram failed 5 time(s)"))
    client.connect_error = ConnectionError("Network is unreachable")

    # The loop ends at the fake sleep after its first check.
    with pytest.raises(_StopLoopError):
        await asyncio.wait_for(detector.connection_loop(), timeout=5)

    assert loop_sleeps == [constants.CONNECTION_CHECK_INTERVAL]


async def test_a_revoked_session_stops_the_connection_loop(
    config, logger, tmp_path, loop_sleeps
):
    """An auth error is an `RPCError`, which this loop otherwise retries forever."""
    detector, client, _, _ = _build(config, logger, tmp_path, [])

    client.drop(ConnectionError("Connection to Telegram failed 5 time(s)"))
    client.connect_error = errors.AuthKeyUnregisteredError(request=None)

    with pytest.raises(AuthorizationLostError):
        await asyncio.wait_for(detector.connection_loop(), timeout=5)

    assert loop_sleeps == []


async def test_two_loops_finding_the_client_dropped_reconnect_it_once(logger):
    """The second caller waits on the lock while the first reconnects.

    Without the re-check under the lock it then reconnects a healthy client:
    against Telethon it waits on the fresh connection's `disconnected` until the
    next drop, stalling its loop.
    """
    client = FakeClient([])
    client.drop(ConnectionError("Connection to Telegram failed 5 time(s)"))
    client.connect_gate = asyncio.Event()
    lock = asyncio.Lock()
    callers = [
        asyncio.create_task(
            ensure_connected(cast(TelegramClient, client), lock, logger)
        )
        for _ in range(2)
    ]

    async def until_one_is_connecting() -> None:
        while client.connects == 0:  # noqa: ASYNC110 - the fake counts, signals nothing
            await asyncio.sleep(0)

        await _until_parked(callers)

    await asyncio.wait_for(until_one_is_connecting(), timeout=5)

    client.connect_gate.set()

    await asyncio.wait_for(asyncio.gather(*callers), timeout=5)

    assert client.connects == 1


async def test_the_reason_is_read_once_telethons_teardown_records_it(logger):
    """`is_connected()` turns false before the teardown sets `disconnected`.

    Reading the reason at once raised `InvalidStateError`, and an auth key
    Telegram forgot was then never reported as a lost session.
    """
    client = FakeClient([])
    teardown = asyncio.get_running_loop().create_future()
    client.connected = False
    client.disconnected = teardown
    caller = asyncio.create_task(
        ensure_connected(cast(TelegramClient, client), asyncio.Lock(), logger)
    )

    await asyncio.wait_for(_until_parked([caller]), timeout=5)

    teardown.set_exception(AuthKeyNotFound())

    with pytest.raises(AuthorizationLostError):
        await asyncio.wait_for(caller, timeout=5)

    assert client.connects == 0


async def test_a_cancel_while_awaiting_teardown_leaves_no_error_unread(logger):
    """A shutdown can land while `ensure_connected` waits for the teardown.

    The reason recorded afterwards was never read, and asyncio logged "Future
    exception was never retrieved" for the shield, or for Telethon's own future
    when the shield was merely cancelled.
    """
    loop = asyncio.get_running_loop()
    unread: list[str] = []
    previous_handler = loop.get_exception_handler()

    loop.set_exception_handler(lambda _, context: unread.append(context["message"]))

    try:
        client = FakeClient([])
        teardown = loop.create_future()  # stands in for the sender's own future
        client.connected = False
        client.disconnected = asyncio.shield(teardown)
        caller = asyncio.create_task(
            ensure_connected(cast(TelegramClient, client), asyncio.Lock(), logger)
        )

        await asyncio.wait_for(_until_parked([caller]), timeout=5)

        caller.cancel()
        done, _ = await asyncio.wait({caller}, timeout=5)

        assert caller in done
        assert caller.cancelled()

        teardown.set_exception(ConnectionError("Connection to Telegram failed"))

        for _ in range(3):  # the shield's and the consumer's callbacks
            await asyncio.sleep(0)

        # Unread futures report it only when collected.
        del client, teardown, caller, done
        gc.collect()

    finally:
        loop.set_exception_handler(previous_handler)

    assert unread == []


class FakeSender:
    """The part of Telethon's `MTProtoSender` a real `TelegramClient` uses here.

    It answers what an authorized account asks on connect, so the client loads
    its update state and starts its background loops just as in production.
    """

    # Telethon wraps every request in these; the answer is the inner query's.
    _WRAPPERS = (
        functions.InvokeWithLayerRequest,
        functions.InvokeWithoutUpdatesRequest,
        functions.InitConnectionRequest,
    )

    def __init__(self) -> None:
        self.auth_key = AuthKey(None)
        self._connection: object | None = None
        self._user_connected = False
        self._disconnected_future: asyncio.Future[None] | None = None

    @property
    def _disconnected(self) -> asyncio.Future[None]:
        # Telethon's starts out resolved: a sender never connected is disconnected.
        if self._disconnected_future is None:
            self._disconnected_future = asyncio.get_running_loop().create_future()
            self._disconnected_future.set_result(None)

        return self._disconnected_future

    @property
    def disconnected(self) -> asyncio.Future[None]:
        return asyncio.shield(self._disconnected)

    def is_connected(self) -> bool:
        return self._user_connected

    async def connect(self, connection: object) -> bool:
        if self._user_connected:
            return False

        self._connection = connection
        self._user_connected = True

        if self._disconnected.done():
            self._disconnected_future = asyncio.get_running_loop().create_future()

        return True

    async def disconnect(self) -> None:
        self._close(None)

    def give_up(self, error: BaseException) -> None:
        """End the connection the way `_disconnect(error)` does after the retries."""
        self._close(error)

    def _close(self, error: BaseException | None) -> None:
        if self._connection is None:
            return  # Telethon: "Not disconnecting (already have no connection)"

        self._user_connected = False
        self._connection = None

        if error is None:
            self._disconnected.set_result(None)
        else:
            self._disconnected.set_exception(error)

    def send(self, request: object, ordered: bool = False) -> asyncio.Future[object]:
        if not self._user_connected:
            raise ConnectionError("Cannot send requests while disconnected")

        future = asyncio.get_running_loop().create_future()
        future.set_result(self._answer(request))

        return future

    @classmethod
    def _answer(cls, request: object) -> object:
        while isinstance(request, cls._WRAPPERS):
            request = request.query

        now = datetime.now(UTC)

        if isinstance(request, functions.help.GetConfigRequest):
            return None  # `connect()` ignores the config

        if isinstance(request, functions.users.GetUsersRequest):
            return [types.User(id=1, access_hash=2, is_self=True)]

        if isinstance(request, functions.updates.GetStateRequest):
            return types.updates.State(pts=1, qts=1, date=now, seq=1, unread_count=0)

        if isinstance(request, functions.updates.GetDifferenceRequest):
            return types.updates.DifferenceEmpty(date=now, seq=1)

        raise AssertionError(f"unexpected request {request!r}")


def _live_tasks(coroutine_name: str) -> list[asyncio.Task[object]]:
    return [
        task
        for task in asyncio.all_tasks()
        if not task.done()
        and getattr(task.get_coro(), "__name__", None) == coroutine_name
    ]


async def _until_parked(tasks: list[asyncio.Task[object]]) -> None:
    """Wait until every task has started and is suspended at an await."""

    def parked(task: asyncio.Task[object]) -> bool:
        coro = task.get_coro()

        return (
            isinstance(coro, Coroutine)
            and inspect.getcoroutinestate(coro) == inspect.CORO_SUSPENDED
        )

    # Telethon's own loops signal nothing when they park, so this polls; the
    # caller bounds it with `wait_for`.
    while not all(parked(task) for task in tasks):  # noqa: ASYNC110
        await asyncio.sleep(0)


async def test_a_reconnect_leaves_one_telethon_update_loop(logger):
    """The sender giving up does not stop the client's update loop.

    A bare `connect()` started a second one. At the next updates deadline both
    fetched the difference, the loser crashed, and Telethon disconnected the
    client, cancelling the catalog poll in flight and ending the poll loop.
    """
    client = TelegramClient(MemorySession(), 1, "0" * 32, receive_updates=False)
    sender = FakeSender()
    client._sender = cast(MTProtoSender, sender)

    try:
        await asyncio.wait_for(client.connect(), timeout=5)

        # In production the loops have long been waiting when the sender gives up.
        loops = _live_tasks("_update_loop") + _live_tasks("_keepalive_loop")

        await asyncio.wait_for(_until_parked(loops), timeout=5)

        sender.give_up(ConnectionError("Connection to Telegram failed 5 time(s)"))

        await asyncio.wait_for(
            ensure_connected(client, asyncio.Lock(), logger), timeout=5
        )

        assert client.is_connected()
        assert len(_live_tasks("_update_loop")) == 1
        assert len(_live_tasks("_keepalive_loop")) == 1

    finally:
        await asyncio.wait_for(client.disconnect(), timeout=5)

    assert _live_tasks("_update_loop") == []
    assert _live_tasks("_keepalive_loop") == []


async def test_a_revoked_session_stops_the_upgrades_loop(
    config, logger, tmp_path, first_payload, loop_sleeps
):
    """An auth error is an `RPCError`, which this loop otherwise retries forever."""
    detector, client, _, _ = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    await detector.poll_once()  # the upgrades loop waits for the first catalog

    client.upgrade_errors = {GIFT_LIMITED_ID: errors.SessionRevokedError(request=None)}

    with pytest.raises(AuthorizationLostError):
        await asyncio.wait_for(detector.upgrades_loop(), timeout=5)


async def test_a_revoked_session_stops_the_updates_loop(
    config, logger, tmp_path, first_payload, monkeypatch, loop_sleeps
):
    """An auth error is an `RPCError`, which this loop otherwise retries forever."""
    second_payload: list[types.TypeStarGift] = [
        _star_gift(GIFT_LIMITED_ID, limited=True, remains=25, total=100),
        _star_gift(GIFT_REGULAR_ID, limited=False),
    ]
    detector, _, _, _ = _build(
        config, logger, tmp_path, [first_payload, second_payload]
    )

    await detector.poll_once()
    await detector.poll_once()  # queues one stock edit

    async def edit_gift_text(self: Notifier, gift: StarGiftData) -> bool:
        raise errors.SessionRevokedError(request=None)

    monkeypatch.setattr(Notifier, "edit_gift_text", edit_gift_text)

    with pytest.raises(AuthorizationLostError):
        await asyncio.wait_for(detector.updates_loop(), timeout=5)


async def test_the_poll_loop_sleeps_out_a_flood_wait(
    config, logger, tmp_path, loop_sleeps
):
    detector, client, _, _ = _build(config, logger, tmp_path, [])

    client.poll_error = errors.FloodWaitError(request=None, capture=7)

    with pytest.raises(_StopLoopError):
        await asyncio.wait_for(detector.poll_loop(), timeout=5)

    assert loop_sleeps == [8]


async def test_the_upgrades_loop_sleeps_out_a_flood_wait(
    config, logger, tmp_path, first_payload, monkeypatch, loop_sleeps
):
    """`probe_upgradable` re-raises a hard flood wait for the loop to sleep out.

    Without the sleep the whole candidate set is probed again on the next tick.
    """
    detector, _, _, _ = _build(
        config, logger, tmp_path, [first_payload], check_upgrades=True
    )

    await detector.poll_once()  # the upgrades loop waits for the first catalog

    floods = [errors.FloodWaitError(request=None, capture=7)]

    async def probe(client, gift_ids: list[int], slots, logger) -> list[int]:
        # One-shot, so a loop that stopped sleeping still reaches a cycle that
        # ends the test instead of spinning without ever yielding.
        if floods:
            raise floods.pop()

        return []

    monkeypatch.setattr("src.detector.probe_upgradable", probe)

    with pytest.raises(_StopLoopError):
        await asyncio.wait_for(detector.upgrades_loop(), timeout=5)

    assert loop_sleeps == [8]


async def test_the_upgrades_loop_waits_for_the_first_catalog_poll(
    config, logger, tmp_path, first_payload, loop_sleeps
):
    """Stickers come only from a catalog poll, and both loops start together.

    An upgrade found before the first poll was posted without its sticker and
    marked upgradable, so it never got one. The gate must also stay shut while
    that poll's fetch is in flight, since the stickers arrive only with it.
    """
    detector, client, bot_api, store = _build(
        config, logger, tmp_path, [first_payload, first_payload], check_upgrades=True
    )

    # A stored gift that became upgradable while the notifier was down.
    parsed = await fetch_star_gifts(cast(TelegramClient, client), logger)

    assert parsed is not None

    await store.replace_all(gift.data for gift in parsed.values())

    client.upgradable = {GIFT_LIMITED_ID}
    # A real fetch is a network round trip; the gate holds the poll inside it.
    client.fetch_gate = asyncio.Event()

    upgrades = asyncio.create_task(detector.upgrades_loop())
    poll = asyncio.create_task(detector.poll_once())

    # An ungated loop probes within two of these yields: nothing it awaits is slow.
    for _ in range(10):
        await asyncio.sleep(0)

    assert client.upgrade_probes == []
    assert bot_api.sent == []

    client.fetch_gate.set()

    assert await asyncio.wait_for(poll, timeout=5) is True

    # The loop ends at the fake sleep after its first cycle.
    with pytest.raises(_StopLoopError):
        await asyncio.wait_for(upgrades, timeout=5)

    limited = store.get(GIFT_LIMITED_ID)

    assert limited is not None
    assert limited.is_upgradable is True
    assert limited.upgrade_sticker_message_id is not None
    assert bot_api.sent[-1]["reply_to_message_id"] == limited.upgrade_sticker_message_id


async def test_a_hard_flood_wait_propagates_out_of_the_probe_batch(
    config, logger, tmp_path, monkeypatch
):
    """Absorbing it per gift meant `upgrades_loop` never slept the wait out.

    The whole candidate set was then re-probed 2s later.
    """
    flood = errors.FloodWaitError(request=None, capture=7)

    async def probe(client, gift_id: int, logger) -> bool:
        if gift_id == GIFT_REGULAR_ID:
            raise flood

        return True

    monkeypatch.setattr("src.userbot.is_gift_upgradable", probe)

    with pytest.raises(errors.FloodWaitError) as excinfo:
        await probe_upgradable(
            cast(TelegramClient, None),
            [GIFT_LIMITED_ID, GIFT_REGULAR_ID],
            asyncio.Semaphore(2),
            logger,
        )

    assert excinfo.value.seconds == 7


async def test_an_auth_error_propagates_out_of_the_probe_batch(
    config, logger, tmp_path, monkeypatch
):
    async def probe(client, gift_id: int, logger) -> bool:
        raise errors.SessionRevokedError(request=None)

    monkeypatch.setattr("src.userbot.is_gift_upgradable", probe)

    with pytest.raises(errors.SessionRevokedError):
        await probe_upgradable(
            cast(TelegramClient, None),
            [GIFT_LIMITED_ID],
            asyncio.Semaphore(1),
            logger,
        )


async def test_only_a_bad_request_means_a_gift_is_not_upgradable(logger):
    """Every other error must reach the caller, an auth error above all.

    Treating any `RPCError` as "not upgradable" swallowed a revoked session.
    """
    client = FakeClient([])
    telegram = cast(TelegramClient, client)

    assert await is_gift_upgradable(telegram, GIFT_REGULAR_ID, logger) is False

    client.upgrade_errors = {GIFT_REGULAR_ID: errors.SessionRevokedError(request=None)}

    with pytest.raises(errors.SessionRevokedError):
        await is_gift_upgradable(telegram, GIFT_REGULAR_ID, logger)


async def test_a_probe_sleeps_out_one_flood_wait_and_retries(logger, monkeypatch):
    client = FakeClient([])
    client.upgradable = {GIFT_LIMITED_ID}
    client.upgrade_errors = {
        GIFT_LIMITED_ID: errors.FloodWaitError(request=None, capture=7)
    }
    slept: list[float] = []

    async def sleep(delay: float) -> None:
        slept.append(delay)
        client.upgrade_errors.clear()  # the wait is over

    monkeypatch.setattr("src.userbot.asyncio", _AsyncioWithSleep(sleep))

    telegram = cast(TelegramClient, client)

    assert await is_gift_upgradable(telegram, GIFT_LIMITED_ID, logger) is True
    assert slept == [8]
    assert client.upgrade_probes == [GIFT_LIMITED_ID, GIFT_LIMITED_ID]


async def test_a_second_flood_wait_in_one_probe_is_raised(logger, monkeypatch):
    """The batch re-raises it, so `upgrades_loop` sleeps it out once for all."""
    client = FakeClient([])
    client.upgrade_errors = {
        GIFT_LIMITED_ID: errors.FloodWaitError(request=None, capture=7)
    }
    slept: list[float] = []

    async def sleep(delay: float) -> None:
        slept.append(delay)

    monkeypatch.setattr("src.userbot.asyncio", _AsyncioWithSleep(sleep))

    with pytest.raises(errors.FloodWaitError):
        await is_gift_upgradable(cast(TelegramClient, client), GIFT_LIMITED_ID, logger)

    assert slept == [8]  # only the first wait is slept out here
    assert client.upgrade_probes == [GIFT_LIMITED_ID, GIFT_LIMITED_ID]


async def test_announcement_retries_back_off_and_stop_at_the_configured_ceiling(
    config, logger, tmp_path, first_payload, monkeypatch
):
    """Retries fire at 1s, 2s, 4s — not a tick earlier — and stop doubling."""
    detector, _, bot_api, store = _build(
        replace(
            config, detector=replace(config.detector, announce_retry_max_delay=4.0)
        ),
        logger,
        tmp_path,
        [first_payload],
    )

    clock = monotonic()
    monkeypatch.setattr("src.detector.monotonic", lambda: clock)

    bot_api.fail_send_times = 2  # both gifts fail their first text send

    await detector.poll_once()

    assert bot_api.sent == []
    assert sorted(detector._retry_delay.values()) == [1.0, 1.0]

    # One tick short of the delay is too early.
    clock += 0.999
    bot_api.fail_send_times = 2

    await detector._retry_announcements()

    assert bot_api.sent == []
    assert sorted(detector._retry_delay.values()) == [1.0, 1.0]

    for expected in (2.0, 4.0, 4.0):
        clock += 1000.0
        bot_api.fail_send_times = 2

        await detector._retry_announcements()

        assert sorted(detector._retry_delay.values()) == [expected, expected]

    clock += 1000.0

    await detector._retry_announcements()

    assert len(bot_api.sent) == 2
    assert detector._retry_at == {}
    assert detector._retry_delay == {}
    assert all(gift.message_id is not None for gift in store.snapshot())

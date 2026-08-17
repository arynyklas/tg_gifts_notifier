import asyncio
from datetime import UTC, datetime
from io import BytesIO
from time import monotonic
from typing import Any, cast

import pytest
from telethon import TelegramClient, errors
from telethon.tl import functions, types

from src import constants
from src.bot_api import BotApiClient, BotApiError
from src.config import Config
from src.detector import Detector, baseline_upgradability
from src.models import StarGiftData
from src.notifier import Notifier
from src.store import StarGiftsStore
from src.userbot import AuthorizationLostError, fetch_star_gifts, probe_upgradable

GIFT_LIMITED_ID = 200
GIFT_REGULAR_ID = 100


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
) -> types.StarGift:
    return types.StarGift(
        id=gift_id,
        sticker=_document(gift_id),
        stars=gift_id * 10,
        convert_stars=gift_id * 5,
        limited=limited,
        sold_out=False,
        availability_remains=remains,
        availability_total=total,
        first_sale_date=datetime(2026, 1, 2, tzinfo=UTC),
        last_sale_date=None,
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
        self._next_message_id = 1000

    async def __call__(self, request: object) -> object:
        self.requests.append(request)

        if isinstance(request, functions.payments.GetStarGiftsRequest):
            if self.poll_error is not None:
                raise self.poll_error

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

    async def edit_message_text(self, chat_id: int, message_id: int, text: str) -> bool:
        if self.fail_edit_times > 0:
            self.fail_edit_times -= 1

            raise BotApiError(500, "internal server error")

        self.edited.append({"chat_id": chat_id, "message_id": message_id, "text": text})

        return True


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
    config, logger, tmp_path, first_payload
):
    detector, _, bot_api, _ = _build(config, logger, tmp_path, [first_payload])

    await detector.poll_once()

    # The unlimited gift has total_amount 0, so it goes first.
    assert f"<code>{GIFT_REGULAR_ID}</code>" in bot_api.sent[0]["text"]
    assert f"<code>{GIFT_LIMITED_ID}</code>" in bot_api.sent[1]["text"]


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

    assert upgrade_text["chat_id"] == config.notifications.upgrades_chat_id
    assert (
        upgrade_text["text"] == f"Gift is upgradable! (<code>{GIFT_LIMITED_ID}</code>)"
    )
    assert len(client.sent_stickers) == 3  # two announcements plus the upgrade

    # The upgradable gift is marked, and the one whose probe came back negative
    # is in its cooldown, so the very next cycle has nothing to ask Telegram.
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
    assert bot_api.sent[-1]["chat_id"] == config.notifications.upgrades_chat_id


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

    assert (
        await baseline_upgradability(
            cast(TelegramClient, client), store, config, logger
        )
        == 1
    )

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


async def test_a_gift_without_an_upgrade_price_is_probed_once_an_hour(
    config, logger, tmp_path, first_payload, monkeypatch
):
    """Probing every not-upgradable gift every 2s is what earns a FLOOD_WAIT.

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


async def test_announcement_retries_back_off_and_stop_at_the_configured_ceiling(
    config, logger, tmp_path, first_payload, monkeypatch
):
    """Retries fire at 1s, 2s, 4s — not a tick earlier — and stop doubling."""
    raw = config.model_dump()
    raw["detector"]["announce_retry_max_delay"] = 4.0

    detector, _, bot_api, store = _build(
        Config.model_validate(raw), logger, tmp_path, [first_payload]
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

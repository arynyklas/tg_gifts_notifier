import asyncio
from collections.abc import Sequence
from logging import Logger
from time import monotonic

from telethon import TelegramClient, errors
from telethon.tl import types

from src import constants
from src.config import Config
from src.notifier import Notifier
from src.store import StarGiftsStore
from src.userbot import (
    AUTH_ERRORS,
    AuthorizationLostError,
    ParsedGift,
    ensure_connected,
    fetch_star_gifts,
    probe_upgradable,
)


class Detector:
    def __init__(
        self,
        client: TelegramClient,
        notifier: Notifier,
        store: StarGiftsStore,
        cfg: Config,
        logger: Logger,
        *,
        check_upgrades: bool,
    ) -> None:
        self._client = client
        self._notifier = notifier
        self._store = store
        self._detector_cfg = cfg.detector
        self._upgrades_cfg = cfg.upgrades
        self._logger = logger
        self._check_upgrades = check_upgrades

        self._stickers: dict[int, types.Document] = {}
        self._pending_updates: dict[int, int] = {}
        self._updates_event = asyncio.Event()
        self._retry_at: dict[int, float] = {}
        self._retry_delay: dict[int, float] = {}
        self._probed_at: dict[int, float] = {}
        self._upgrade_slots = asyncio.Semaphore(cfg.upgrades.check_concurrency)
        # One reconnect at a time, whichever loop notices the dropped client.
        self._connect_lock = asyncio.Lock()
        self._catalog_ready = asyncio.Event()

    def sticker_for(self, gift_id: int) -> types.Document | None:
        return self._stickers.get(gift_id)

    async def poll_loop(self) -> None:
        # The retry schedule lives only in memory, so a supervise restart after
        # an announcement crashed the loop would otherwise never retry that gift.
        self.seed_pending_announcements()

        while True:
            try:
                await ensure_connected(self._client, self._connect_lock, self._logger)
                await self.poll_once()

            except AUTH_ERRORS as ex:
                raise AuthorizationLostError(str(ex)) from ex

            except errors.FloodWaitError as ex:
                self._logger.warning("FLOOD_WAIT %ds while polling gifts", ex.seconds)

                await asyncio.sleep(ex.seconds + 1)

                continue

            except (errors.RPCError, OSError):  # ConnectionError is an OSError
                self._logger.exception("Poll cycle failed")

            await asyncio.sleep(self._detector_cfg.check_interval)

    async def updates_loop(self) -> None:
        while True:
            try:
                await self.drain_updates_once()

            except AUTH_ERRORS as ex:
                raise AuthorizationLostError(str(ex)) from ex

            except (errors.RPCError, OSError):
                self._logger.exception("Update batch failed")

            await asyncio.sleep(self._detector_cfg.updates_batch_delay)

    async def connection_loop(self) -> None:
        """Reconnect independently of the loops that send requests.

        `is_connected()` stays true while Telethon retries a dropped connection,
        so a request sent then is queued, and once Telethon gives up that queue
        is never answered or failed. The loop that sent it never gets back to
        `ensure_connected`; a reconnect from here drains the queue and frees it.
        """
        while True:
            try:
                await ensure_connected(self._client, self._connect_lock, self._logger)

            except AUTH_ERRORS as ex:
                raise AuthorizationLostError(str(ex)) from ex

            except (errors.RPCError, OSError) as ex:
                self._logger.warning("Userbot reconnect failed: %r", ex)

            await asyncio.sleep(constants.CONNECTION_CHECK_INTERVAL)

    async def upgrades_loop(self) -> None:
        # Stickers come only from a catalog poll, and both loops start together:
        # an upgrade found before the first poll would be posted without its
        # sticker and marked upgradable, so it would never get one.
        await self._catalog_ready.wait()

        while True:
            try:
                await ensure_connected(self._client, self._connect_lock, self._logger)
                await self.check_upgrades_once()

            except AUTH_ERRORS as ex:
                raise AuthorizationLostError(str(ex)) from ex

            except errors.FloodWaitError as ex:
                self._logger.warning(
                    "FLOOD_WAIT %ds while checking upgrades", ex.seconds
                )

                await asyncio.sleep(ex.seconds + 1)

                continue

            except (errors.RPCError, OSError):
                self._logger.exception("Upgrades cycle failed")

            await asyncio.sleep(self._upgrades_cfg.check_interval)

    async def poll_once(self) -> bool:
        """One catalog poll. Returns whether a fresh payload was processed."""
        parsed = await fetch_star_gifts(self._client, self._logger)

        if parsed is None:
            self._logger.debug("Star gifts data not modified")

            return False

        # Sticker references are refreshed every cycle, never persisted.
        self._stickers = {
            gift_id: gift.sticker
            for gift_id, gift in parsed.items()
            if gift.sticker is not None
        }

        self._catalog_ready.set()

        await self._announce_new(parsed)
        await self._retry_announcements()
        await self._apply_live(parsed)

        return True

    async def drain_updates_once(self) -> int:
        """Edit one message per gift whose stock dropped. Returns edits made."""
        await self._updates_event.wait()
        self._updates_event.clear()

        pending, self._pending_updates = self._pending_updates, {}

        items = [
            (gift, previous)
            for gift_id, previous in pending.items()
            if (gift := self._store.get(gift_id)) is not None
        ]
        items.sort(key=lambda item: item[0].first_appearance_timestamp or 0)

        edited = 0

        for gift, previous in items:
            if not await self._notifier.edit_gift_text(gift):
                # A transient failure must not drop the stock number: the batch
                # is re-armed with the oldest known previous value.
                self._pending_updates.setdefault(gift.id, previous)
                self._updates_event.set()

                continue

            edited += 1

            self._logger.debug(
                "Gift %d available %d -> %d (message %s)",
                gift.id,
                previous,
                gift.available_amount,
                gift.message_id,
            )

        return edited

    async def check_upgrades_once(self) -> int:
        """Probe every gift not yet known to be upgradable. Returns announcements.

        Gifts that were never announced are probed too: the upgrade notification
        goes to its own chat and does not reply to the original message, and
        after a `--save-only` baseline nothing in the file has a `message_id`.
        """
        if not self._check_upgrades:
            return 0

        now = monotonic()
        # `monotonic()` has no fixed origin (on Linux it counts from boot), so a
        # never-probed gift is due now rather than measured against 0.0.
        candidates = [
            gift
            for gift in self._store.upgrade_candidates()
            if gift.upgrade_stars is not None
            or gift.id not in self._probed_at
            or now - self._probed_at[gift.id] >= constants.NEGATIVE_PROBE_COOLDOWN
        ]

        if not candidates:
            self._logger.debug("No upgrade candidates due")

            return 0

        upgradable = await probe_upgradable(
            self._client,
            [gift.id for gift in candidates],
            self._upgrade_slots,
            self._logger,
        )
        upgradable_ids = set(upgradable)

        for gift in candidates:
            if gift.id not in upgradable_ids:
                self._probed_at[gift.id] = now

        announced = 0

        for gift_id in upgradable:
            gift = self._store.get(gift_id)

            if gift is None:
                self._logger.warning(
                    "Stored gift %d vanished before its upgrade notify", gift_id
                )

                continue

            if not await self._notifier.announce_upgrade(
                gift, self.sticker_for(gift_id)
            ):
                continue

            await self._store.mark_upgradable(gift_id)

            announced += 1

        return announced

    async def _announce_new(self, parsed: dict[int, ParsedGift]) -> None:
        new_gifts = [
            gift for gift_id, gift in parsed.items() if gift_id not in self._store
        ]

        if not new_gifts:
            return

        new_gifts.sort(key=lambda gift: gift.data.total_amount)

        self._logger.info(
            "Found %d new gifts: %s",
            len(new_gifts),
            [gift.data.id for gift in new_gifts],
        )

        for gift in new_gifts:
            stored = await self._store.add(gift.data)

            if await self._notifier.announce_new_gift(stored, gift.sticker):
                self._clear_retry(stored.id)

            else:
                self._schedule_retry(stored.id)

    async def _retry_announcements(self) -> None:
        if not self._retry_at:
            return

        now = monotonic()
        # Driven by the pending set, not by the catalog: the store holds every
        # gift ever seen, and all but a handful are already announced.
        due = [
            gift
            for gift_id, retry_at in self._retry_at.items()
            if retry_at <= now
            and (gift := self._store.get(gift_id)) is not None
            and gift.message_id is None
        ]

        for gift in due:
            if await self._notifier.announce_new_gift(gift, self.sticker_for(gift.id)):
                self._clear_retry(gift.id)

            else:
                self._schedule_retry(gift.id)

    async def _apply_live(self, parsed: dict[int, ParsedGift]) -> None:
        drops = await self._store.apply_live_many(gift.data for gift in parsed.values())

        for stored, previous_available in drops:
            if stored.message_id is None:
                continue

            self._pending_updates.setdefault(stored.id, previous_available)
            self._updates_event.set()

    def _schedule_retry(self, gift_id: int) -> None:
        delay = min(
            max(self._retry_delay.get(gift_id, 0.0) * 2, 1.0),
            self._detector_cfg.announce_retry_max_delay,
        )

        self._retry_at[gift_id] = monotonic() + delay
        self._retry_delay[gift_id] = delay

    def seed_pending_announcements(self) -> int:
        """Re-arm announcements interrupted by a crash. Returns how many."""
        now = monotonic()

        for gift in self._store.gifts():
            if gift.announce_pending and gift.message_id is None:
                self._retry_at[gift.id] = now
                self._retry_delay[gift.id] = 0.0

        return len(self._retry_at)

    def _clear_retry(self, gift_id: int) -> None:
        self._retry_at.pop(gift_id, None)
        self._retry_delay.pop(gift_id, None)


async def baseline_upgradability(
    client: TelegramClient,
    gift_ids: Sequence[int],
    cfg: Config,
    logger: Logger,
) -> list[int]:
    """Return which of `gift_ids` are already upgradable, announcing nothing.

    A baseline (a `--save-only` run, or a legacy file whose run never checked
    upgrades) knows nothing about upgradability, and the first upgrades cycle
    would announce every already-upgradable gift — 118 of 149 when this was
    written. Every probe must succeed: a gift whose probe failed would be stored
    as not upgradable and announced the same way, so any failure raises and the
    caller persists nothing.
    """
    if not gift_ids:
        return []

    return await probe_upgradable(
        client,
        gift_ids,
        asyncio.Semaphore(cfg.upgrades.check_concurrency),
        logger,
        strict=True,
    )

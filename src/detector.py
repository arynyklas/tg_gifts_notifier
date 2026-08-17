import asyncio
from logging import Logger
from time import monotonic

from telethon import TelegramClient, errors
from telethon.tl import types

from src.config import Config
from src.notifier import Notifier
from src.store import StarGiftsStore
from src.userbot import ParsedGift, fetch_star_gifts, is_gift_upgradable


class Detector:
    def __init__(
        self,
        client: TelegramClient,
        notifier: Notifier,
        store: StarGiftsStore,
        cfg: Config,
        logger: Logger,
        *,
        track_updates: bool,
        check_upgrades: bool,
    ) -> None:
        self._client = client
        self._notifier = notifier
        self._store = store
        self._cfg = cfg
        self._detector_cfg = cfg.detector
        self._logger = logger
        self._track_updates = track_updates
        self._check_upgrades = check_upgrades

        self._hash = 0
        self._stickers: dict[int, types.Document] = {}
        self._pending_updates: dict[int, int] = {}
        self._updates_event = asyncio.Event()
        self._retry_at: dict[int, float] = {}
        self._retry_delay: dict[int, float] = {}

    def sticker_for(self, gift_id: int) -> types.Document | None:
        return self._stickers.get(gift_id)

    async def poll_loop(self) -> None:
        while True:
            try:
                await self.poll_once()

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

            except (errors.RPCError, OSError):
                self._logger.exception("Update batch failed")

            await asyncio.sleep(self._detector_cfg.updates_batch_delay)

    async def upgrades_loop(self) -> None:
        while True:
            try:
                await self.check_upgrades_once()

            except errors.FloodWaitError as ex:
                self._logger.warning(
                    "FLOOD_WAIT %ds while checking upgrades", ex.seconds
                )

                await asyncio.sleep(ex.seconds + 1)

                continue

            except (errors.RPCError, OSError):
                self._logger.exception("Upgrades cycle failed")

            await asyncio.sleep(self._detector_cfg.check_upgrades_interval)

    async def poll_once(self) -> bool:
        """One catalog poll. Returns whether a fresh payload was processed."""
        gifts_hash, parsed = await fetch_star_gifts(
            self._client, self._hash, self._logger
        )

        if parsed is None:
            self._logger.debug("Star gifts data not modified")

            return False

        # Sticker references are refreshed every cycle, never persisted.
        self._stickers = {
            gift_id: gift.sticker
            for gift_id, gift in parsed.items()
            if gift.sticker is not None
        }

        if not self._track_updates:
            # Full payloads are required to observe stock changes, so the hash
            # is only cached when nothing tracks updates.
            self._hash = gifts_hash

        await self._announce_new(parsed)
        await self._retry_announcements()
        await self._apply_live(parsed)

        self._store.mark_dirty()

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
        """Probe every non-upgradable gift. Returns announced upgrades."""
        if not self._check_upgrades:
            return 0

        candidates = [
            gift
            for gift in self._store.snapshot()
            if not gift.is_upgradable and gift.message_id is not None
        ]

        if not candidates:
            self._logger.debug("No non-upgradable gifts to check")

            return 0

        semaphore = asyncio.Semaphore(self._detector_cfg.upgrades_check_concurrency)

        async def probe(gift_id: int) -> tuple[int, bool]:
            async with semaphore:
                return gift_id, await is_gift_upgradable(
                    self._client, gift_id, self._logger
                )

        results = await asyncio.gather(
            *(probe(gift.id) for gift in candidates),
            return_exceptions=True,
        )

        upgradable: list[int] = []

        for candidate, result in zip(candidates, results, strict=True):
            if isinstance(result, BaseException):
                self._logger.error(
                    "Failed to check upgradability of gift %d",
                    candidate.id,
                    exc_info=result,
                )

                continue

            gift_id, is_upgradable = result

            if is_upgradable:
                upgradable.append(gift_id)

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
            await self._store.add(gift.data)

            if await self._notifier.announce_new_gift(gift.data, gift.sticker):
                self._clear_retry(gift.data.id)

            else:
                self._schedule_retry(gift.data.id)

    async def _retry_announcements(self) -> None:
        if not self._retry_at:
            return

        now = monotonic()
        due = [
            gift
            for gift in self._store.snapshot()
            if gift.message_id is None
            and self._retry_at.get(gift.id, float("inf")) <= now
        ]

        for gift in due:
            if await self._notifier.announce_new_gift(gift, self.sticker_for(gift.id)):
                self._clear_retry(gift.id)

            else:
                self._schedule_retry(gift.id)

    async def _apply_live(self, parsed: dict[int, ParsedGift]) -> None:
        for gift_id, gift in parsed.items():
            result = await self._store.apply_live(gift.data)

            if result is None or not self._track_updates:
                continue

            stored, previous_available = result

            if (
                stored.available_amount >= previous_available
                or stored.message_id is None
            ):
                continue

            self._pending_updates.setdefault(gift_id, previous_available)
            self._updates_event.set()

    def _schedule_retry(self, gift_id: int) -> None:
        delay = min(
            max(self._retry_delay.get(gift_id, 0.0) * 2, 1.0),
            self._detector_cfg.announce_retry_max_delay,
        )

        self._retry_at[gift_id] = monotonic() + delay
        self._retry_delay[gift_id] = delay

    def _clear_retry(self, gift_id: int) -> None:
        self._retry_at.pop(gift_id, None)
        self._retry_delay.pop(gift_id, None)

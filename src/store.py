import asyncio
from collections.abc import Iterable, ValuesView
import json
from logging import Logger
import os
from pathlib import Path
from typing import Self

from src import constants
from src.models import StarGiftData, StarGiftsFile


def _merge_live(stored: StarGiftData, live: StarGiftData) -> bool:
    """Copy the volatile fields of `live` onto `stored`; report a real change.

    Compared field by field rather than through a dump or a tuple so that the
    unchanged case — nearly every poll — allocates nothing.
    """
    if (
        stored.number == live.number
        and stored.price == live.price
        and stored.convert_price == live.convert_price
        and stored.available_amount == live.available_amount
        and stored.total_amount == live.total_amount
        and stored.require_premium == live.require_premium
        and stored.user_limited == live.user_limited
        and stored.is_limited == live.is_limited
        and stored.last_sale_timestamp == live.last_sale_timestamp
        and stored.upgrade_stars == live.upgrade_stars
    ):
        return False

    stored.number = live.number
    stored.price = live.price
    stored.convert_price = live.convert_price
    stored.available_amount = live.available_amount
    stored.total_amount = live.total_amount
    stored.require_premium = live.require_premium
    stored.user_limited = live.user_limited
    stored.is_limited = live.is_limited
    stored.last_sale_timestamp = live.last_sale_timestamp
    stored.upgrade_stars = live.upgrade_stars

    return True


class StarGiftsStore:
    """Sole owner of the persisted gift state.

    Every mutation goes through this class; nothing else may mutate a
    `StarGiftData` instance that lives in the store.
    """

    def __init__(self, path: Path, save_debounce: float, logger: Logger) -> None:
        self._path = path
        self._debounce = save_debounce
        self._logger = logger
        self._gifts: dict[int, StarGiftData] = {}
        self._lock = asyncio.Lock()
        self._dirty = asyncio.Event()
        self.loaded_version = constants.SCHEMA_VERSION

    @classmethod
    def load(
        cls, path: Path, save_debounce: float, logger: Logger
    ) -> tuple[Self, bool]:
        """Load `path`, returning the store and whether the file needs migrating."""
        store = cls(path, save_debounce, logger)

        try:
            raw = path.read_text(encoding=constants.ENCODING)

        except FileNotFoundError:
            logger.info("No gift data at %s, starting empty", path)

            return store, False

        parsed = StarGiftsFile.model_validate(json.loads(raw))

        store._gifts = {gift.id: gift for gift in parsed.star_gifts}
        store.loaded_version = parsed.version

        return store, parsed.version < constants.SCHEMA_VERSION

    def __contains__(self, gift_id: int) -> bool:
        return gift_id in self._gifts

    def __len__(self) -> int:
        return len(self._gifts)

    def get(self, gift_id: int) -> StarGiftData | None:
        return self._gifts.get(gift_id)

    def snapshot(self) -> list[StarGiftData]:
        """Gifts ordered by id, for persistence and reporting."""
        return sorted(self._gifts.values(), key=lambda gift: gift.id)

    def gifts(self) -> ValuesView[StarGiftData]:
        """Unordered live view, for scans that do not care about order."""
        return self._gifts.values()

    def upgrade_candidates(self) -> list[StarGiftData]:
        """Gifts not yet known to be upgradable, cheapest signal first."""
        return [gift for gift in self._gifts.values() if not gift.is_upgradable]

    @property
    def is_dirty(self) -> bool:
        return self._dirty.is_set()

    async def add(self, gift: StarGiftData) -> StarGiftData:
        """Insert a gift that is about to be announced; returns the stored one.

        `add` is called only where an announcement is intended, so it is also
        where `announce_pending` is armed. The stored instance is returned so no
        caller depends on the store adopting its argument by reference.
        """
        async with self._lock:
            gift.announce_pending = True
            self._gifts[gift.id] = gift

        self.mark_dirty()

        return gift

    async def apply_live_many(
        self, live: Iterable[StarGiftData]
    ) -> list[tuple[StarGiftData, int]]:
        """Merge the volatile fields of a whole payload under a single lock.

        Returns `(stored, previous_available)` for every known gift whose stock
        dropped. Unknown gifts are ignored, and `message_id`,
        `sticker_message_id`, `is_upgradable` and `first_appearance_timestamp`
        are never touched, so a concurrent notification cannot be clobbered.

        The store is marked dirty only when a field really changed: the catalog
        is polled every second and is usually identical, so marking it
        unconditionally rewrote the whole file every debounce window forever.
        """
        drops: list[tuple[StarGiftData, int]] = []
        changed = False

        async with self._lock:
            for item in live:
                stored = self._gifts.get(item.id)

                if stored is None:
                    continue

                previous_available = stored.available_amount

                if not _merge_live(stored, item):
                    continue

                changed = True

                if stored.available_amount < previous_available:
                    drops.append((stored, previous_available))

        if changed:
            self.mark_dirty()

        return drops

    async def set_message_ids(
        self,
        gift_id: int,
        *,
        sticker_message_id: int | None = None,
        message_id: int | None = None,
        upgrade_sticker_message_id: int | None = None,
    ) -> None:
        async with self._lock:
            stored = self._gifts.get(gift_id)

            if stored is None:
                self._logger.warning(
                    "Cannot set message ids of unknown gift %d", gift_id
                )

                return

            if sticker_message_id is not None:
                stored.sticker_message_id = sticker_message_id

            if message_id is not None:
                stored.message_id = message_id
                stored.announce_pending = False

            if upgrade_sticker_message_id is not None:
                stored.upgrade_sticker_message_id = upgrade_sticker_message_id

        self.mark_dirty()

    async def mark_upgradable(self, gift_id: int) -> bool:
        async with self._lock:
            stored = self._gifts.get(gift_id)

            if stored is None or stored.is_upgradable:
                return False

            stored.is_upgradable = True

        self.mark_dirty()

        return True

    async def replace_all(self, gifts: Iterable[StarGiftData]) -> None:
        async with self._lock:
            self._gifts = {gift.id: gift for gift in gifts}

        self.mark_dirty()

    def mark_dirty(self) -> None:
        self._dirty.set()

    async def run_saver(self) -> None:
        while True:
            await self._dirty.wait()
            await asyncio.sleep(self._debounce)
            # Cleared before writing: a mutation landing mid-write schedules
            # another pass instead of being dropped.
            self._dirty.clear()

            try:
                await self._write()

            except OSError:
                # Re-arm so the restarted saver and the shutdown flush retry.
                self.mark_dirty()

                raise

    async def flush(self) -> None:
        """Write pending changes, if any."""
        if not self._dirty.is_set():
            self._logger.debug("Nothing to save to %s", self._path)

            return

        self._dirty.clear()

        try:
            await self._write()

        except OSError:
            self.mark_dirty()
            self._logger.exception("Could not save gift data to %s", self._path)

    async def _write(self) -> None:
        async with self._lock:
            gifts = self.snapshot()
            payload = StarGiftsFile(
                version=constants.SCHEMA_VERSION,
                star_gifts=gifts,
            ).model_dump_json(indent=4)
            amount = len(gifts)

        await asyncio.to_thread(self._write_atomic, payload)

        self._logger.debug("Saved %d gifts to %s", amount, self._path)

    def _write_atomic(self, payload: str) -> None:
        parent = self._path.parent

        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)

        tmp = self._path.with_name(f"{self._path.name}.{os.getpid()}.{id(payload)}.tmp")

        try:
            tmp.write_text(payload, encoding=constants.ENCODING)

            os.replace(tmp, self._path)

        finally:
            tmp.unlink(missing_ok=True)

import asyncio
from collections.abc import Iterable
import json
from logging import Logger
import os
from pathlib import Path
from typing import Self

from src import constants
from src.models import StarGiftData, StarGiftsFile


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
        return sorted(self._gifts.values(), key=lambda gift: gift.id)

    async def add(self, gift: StarGiftData) -> None:
        async with self._lock:
            self._gifts[gift.id] = gift

        self.mark_dirty()

    async def apply_live(self, live: StarGiftData) -> tuple[StarGiftData, int] | None:
        """Merge volatile fields of `live` into the stored gift.

        Returns the stored gift and its previous `available_amount`, or `None`
        when the gift is unknown. Never touches `message_id`,
        `sticker_message_id`, `is_upgradable` or `first_appearance_timestamp`,
        so a concurrent upgrade notification cannot be clobbered.
        """
        async with self._lock:
            stored = self._gifts.get(live.id)

            if stored is None:
                return None

            previous_available = stored.available_amount

            stored.number = live.number
            stored.price = live.price
            stored.convert_price = live.convert_price
            stored.available_amount = live.available_amount
            stored.total_amount = live.total_amount
            stored.require_premium = live.require_premium
            stored.user_limited = live.user_limited
            stored.is_limited = live.is_limited
            stored.last_sale_timestamp = live.last_sale_timestamp

            return stored, previous_available

    async def set_message_ids(
        self,
        gift_id: int,
        *,
        sticker_message_id: int | None = None,
        message_id: int | None = None,
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

            await self._write()

    async def flush(self) -> None:
        self._dirty.clear()

        await self._write()

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

        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(payload, encoding=constants.ENCODING)

        os.replace(tmp, self._path)

import asyncio
from contextlib import suppress
import json
from pathlib import Path

import pytest

from src.constants import SCHEMA_VERSION
from src.models import StarGiftData
from src.store import StarGiftsStore


def write_legacy_data(path: Path) -> None:
    """Write a v1 data file: no `version`, gifts carrying dead sticker keys."""
    path.write_text(
        json.dumps(
            {
                "star_gifts": [
                    {
                        "id": 20,
                        "number": 2,
                        "sticker_file_id": "CAACAgIAAxkBAA",
                        "sticker_file_name": "20.tgs",
                        "price": 200,
                        "convert_price": 100,
                        "available_amount": 50,
                        "total_amount": 100,
                        "require_premium": False,
                        "user_limited": None,
                        "is_limited": True,
                        "first_appearance_timestamp": 1700000000,
                        "message_id": 555,
                        "last_sale_timestamp": None,
                        "is_upgradable": True,
                    },
                    {
                        "id": 10,
                        "number": 1,
                        "sticker_file_id": "CAACAgIAAxkBAB",
                        "sticker_file_name": "10.tgs",
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


def _gift(gift_id: int, **overrides) -> StarGiftData:
    fields = {
        "id": gift_id,
        "number": gift_id,
        "price": 100,
        "convert_price": 50,
        "available_amount": 10,
        "total_amount": 20,
        "is_limited": True,
    }
    fields.update(overrides)

    return StarGiftData(**fields)


def test_legacy_file_is_migrated_and_dead_keys_are_dropped(tmp_path, logger):
    path = tmp_path / "star_gifts.json"
    write_legacy_data(path)

    store, legacy = StarGiftsStore.load(path, 0.01, logger)

    assert legacy is True
    assert store.loaded_version == 1
    assert [gift.id for gift in store.snapshot()] == [10, 20]

    limited = store.get(20)

    assert limited is not None
    assert limited.message_id == 555
    assert limited.is_upgradable is True
    assert limited.sticker_message_id is None
    assert not hasattr(limited, "sticker_file_id")
    assert "sticker_file_id" not in limited.model_dump()


def test_missing_file_yields_empty_store(tmp_path, logger):
    store, legacy = StarGiftsStore.load(tmp_path / "absent.json", 0.01, logger)

    assert legacy is False
    assert store.snapshot() == []


def test_corrupt_file_aborts_load(tmp_path, logger):
    path = tmp_path / "star_gifts.json"
    path.write_text("{not json", encoding="utf-8")

    try:
        StarGiftsStore.load(path, 0.01, logger)

    except json.JSONDecodeError:
        return

    raise AssertionError("corrupt data must not be swallowed")


async def test_live_merge_preserves_message_state(tmp_path, logger):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.01, logger)

    await store.add(
        _gift(
            10,
            available_amount=10,
            message_id=42,
            sticker_message_id=41,
            is_upgradable=True,
            first_appearance_timestamp=1000,
        )
    )

    drops = await store.apply_live_many(
        [
            _gift(
                10,
                available_amount=7,
                last_sale_timestamp=2000,
                first_appearance_timestamp=9999,
            )
        ]
    )

    assert len(drops) == 1

    stored, previous_available = drops[0]

    assert previous_available == 10
    assert stored.available_amount == 7
    assert stored.last_sale_timestamp == 2000
    assert stored.message_id == 42
    assert stored.sticker_message_id == 41
    assert stored.is_upgradable is True
    assert stored.first_appearance_timestamp == 1000


async def test_live_merge_ignores_unknown_gifts(tmp_path, logger):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.01, logger)

    assert await store.apply_live_many([_gift(10)]) == []
    assert store.is_dirty is False


async def test_identical_payload_leaves_the_store_clean(tmp_path, logger):
    """The catalog is polled every second and is usually unchanged.

    Marking the store dirty regardless rewrote the whole file every debounce
    window forever, so an unchanged payload must leave no work behind.
    """
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.01, logger)
    live = _gift(10, available_amount=5, last_sale_timestamp=2000)

    await store.add(live.model_copy())

    await store.flush()

    assert store.is_dirty is False

    assert await store.apply_live_many([live]) == []
    assert store.is_dirty is False

    # A single changed field is still caught.
    assert await store.apply_live_many([_gift(10, available_amount=4)]) != []
    assert store.is_dirty is True


async def test_stock_rise_is_not_reported_as_a_drop(tmp_path, logger):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.01, logger)

    await store.add(_gift(10, available_amount=5))

    assert await store.apply_live_many([_gift(10, available_amount=9)]) == []
    # The rise is persisted even though it is not an edit trigger.
    assert store.is_dirty is True

    gift = store.get(10)

    assert gift is not None
    assert gift.available_amount == 9


async def test_flush_without_changes_writes_nothing(tmp_path, logger, monkeypatch):
    path = tmp_path / "star_gifts.json"
    store = StarGiftsStore(path, 0.01, logger)

    await store.add(_gift(10))
    await store.flush()

    writes = 0
    original = store._write_atomic

    def counting(payload: str) -> None:
        nonlocal writes

        writes += 1

        original(payload)

    monkeypatch.setattr(store, "_write_atomic", counting)

    await store.flush()

    assert writes == 0


async def test_saver_debounces_into_a_single_atomic_write(
    tmp_path, logger, monkeypatch
):
    """Marks arriving inside the window must coalesce into one write.

    With a near-zero debounce this passed even with the `sleep` deleted, so the
    marks are spread across a window that is an order of magnitude shorter.
    """
    path = tmp_path / "star_gifts.json"
    store = StarGiftsStore(path, 0.05, logger)

    await store.add(_gift(20))
    await store.add(_gift(10))

    writes = 0
    original = store._write_atomic

    def counting_write(payload: str) -> None:
        nonlocal writes
        writes += 1

        original(payload)

    monkeypatch.setattr(store, "_write_atomic", counting_write)

    saver = asyncio.create_task(store.run_saver())

    for _ in range(3):
        store.mark_dirty()

        await asyncio.sleep(0.01)

    await asyncio.sleep(0.1)

    assert writes == 1

    store.mark_dirty()

    await asyncio.sleep(0.1)

    assert writes == 2

    saver.cancel()

    with suppress(asyncio.CancelledError):
        await saver

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["version"] == SCHEMA_VERSION
    assert [gift["id"] for gift in payload["star_gifts"]] == [10, 20]
    assert not list(tmp_path.glob("*.tmp"))


async def test_a_failed_save_is_retried_instead_of_being_lost(
    tmp_path, logger, monkeypatch
):
    """A write that raises must leave the store dirty.

    `run_saver` clears the flag before writing, so without re-arming it the
    restarted saver blocks forever and the shutdown flush reports "Nothing to
    save", discarding every `message_id` written since the last good write.
    """
    path = tmp_path / "star_gifts.json"
    store = StarGiftsStore(path, 0.0, logger)
    original = store._write_atomic

    def failing(payload: str) -> None:
        raise OSError("disk is full")

    monkeypatch.setattr(store, "_write_atomic", failing)

    await store.add(_gift(10, message_id=77))

    saver = asyncio.create_task(store.run_saver())

    await asyncio.sleep(0.05)

    assert saver.done()

    with pytest.raises(OSError, match="disk is full"):
        await saver

    assert store.is_dirty is True
    assert not path.exists()

    monkeypatch.setattr(store, "_write_atomic", original)

    await store.flush()

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["star_gifts"][0]["message_id"] == 77


async def test_flush_swallows_a_write_failure_but_stays_dirty(
    tmp_path, logger, monkeypatch
):
    """`flush` runs from a `finally`, where raising would replace the exit code."""
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)

    await store.add(_gift(10))

    def failing(payload: str) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(store, "_write_atomic", failing)

    await store.flush()  # must not raise

    assert store.is_dirty is True


async def test_add_arms_the_announcement_and_returns_the_stored_gift(tmp_path, logger):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)
    gift = _gift(10)
    stored = await store.add(gift)

    assert stored is store.get(10)
    assert stored.announce_pending is True

    await store.set_message_ids(10, sticker_message_id=5)

    assert stored.announce_pending is True  # the text has not landed yet

    await store.set_message_ids(10, message_id=6)

    assert stored.announce_pending is False


async def test_replace_all_leaves_nothing_pending(tmp_path, logger):
    """`--save-only` must not arm an announcement for the whole catalog."""
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)

    await store.replace_all([_gift(10), _gift(20)])

    assert [gift.announce_pending for gift in store.snapshot()] == [False, False]


async def test_upgrade_candidates_skips_gifts_already_known_upgradable(
    tmp_path, logger
):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)

    await store.replace_all([_gift(10), _gift(20, is_upgradable=True)])

    assert [gift.id for gift in store.upgrade_candidates()] == [10]


async def test_upgrade_stars_is_merged_from_the_live_payload(tmp_path, logger):
    """The cooldown filter reads `upgrade_stars`, so it must track the catalog."""
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)

    await store.add(_gift(10, upgrade_stars=None))
    await store.flush()

    assert await store.apply_live_many([_gift(10, upgrade_stars=25)]) == []

    gift = store.get(10)

    assert gift is not None
    assert gift.upgrade_stars == 25
    assert store.is_dirty is True


async def test_upgrade_sticker_id_is_persisted_separately(tmp_path, logger):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)

    await store.add(_gift(10, sticker_message_id=1))
    await store.set_message_ids(10, upgrade_sticker_message_id=9)

    gift = store.get(10)

    assert gift is not None
    assert gift.sticker_message_id == 1
    assert gift.upgrade_sticker_message_id == 9


async def test_two_writers_do_not_collide_on_the_temp_file(
    tmp_path, logger, monkeypatch
):
    """A cancelled saver's `to_thread` write can still be running during flush.

    `to_thread` is not cancellable, so both writers can be inside
    `_write_atomic` at once; with one shared temp name the loser of the
    `os.replace` race raises `FileNotFoundError` out of shutdown.
    """
    path = tmp_path / "star_gifts.json"
    first = StarGiftsStore(path, 0.0, logger)
    second = StarGiftsStore(path, 0.0, logger)

    await first.add(_gift(10))
    await second.add(_gift(20))

    real_write_text = Path.write_text
    interleaved = False

    def write_text_then_let_the_other_writer_finish(self, data, *args, **kwargs):
        nonlocal interleaved

        result = real_write_text(self, data, *args, **kwargs)

        if not interleaved:
            interleaved = True

            second._write_atomic('{"star_gifts": []}')

        return result

    monkeypatch.setattr(Path, "write_text", write_text_then_let_the_other_writer_finish)

    first._write_atomic('{"star_gifts": [{"id": 10}]}')  # must not raise

    monkeypatch.undo()

    assert json.loads(path.read_text(encoding="utf-8"))["star_gifts"][0]["id"] == 10
    assert not list(tmp_path.glob("*.tmp"))


async def test_flush_writes_immediately(tmp_path, logger):
    path = tmp_path / "star_gifts.json"
    store = StarGiftsStore(path, 30.0, logger)

    await store.add(_gift(10))
    await store.flush()

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["star_gifts"][0]["id"] == 10
    assert "sticker_file_id" not in payload["star_gifts"][0]


async def test_mark_upgradable_is_idempotent(tmp_path, logger):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.01, logger)

    await store.add(_gift(10))

    assert await store.mark_upgradable(10) is True
    assert await store.mark_upgradable(10) is False
    assert await store.mark_upgradable(999) is False

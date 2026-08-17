import asyncio
from contextlib import suppress
import json
from pathlib import Path

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


async def test_apply_live_preserves_message_state(tmp_path, logger):
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

    result = await store.apply_live(
        _gift(
            10,
            available_amount=7,
            last_sale_timestamp=2000,
            first_appearance_timestamp=9999,
        )
    )

    assert result is not None

    stored, previous_available = result

    assert previous_available == 10
    assert stored.available_amount == 7
    assert stored.last_sale_timestamp == 2000
    assert stored.message_id == 42
    assert stored.sticker_message_id == 41
    assert stored.is_upgradable is True
    assert stored.first_appearance_timestamp == 1000


async def test_apply_live_ignores_unknown_gift(tmp_path, logger):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.01, logger)

    assert await store.apply_live(_gift(10)) is None


async def test_saver_debounces_into_a_single_atomic_write(
    tmp_path, logger, monkeypatch
):
    path = tmp_path / "star_gifts.json"
    store = StarGiftsStore(path, 0.01, logger)

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

    store.mark_dirty()
    store.mark_dirty()
    store.mark_dirty()

    await asyncio.sleep(0.1)

    saver.cancel()

    with suppress(asyncio.CancelledError):
        await saver

    assert writes == 1

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["version"] == SCHEMA_VERSION
    assert [gift["id"] for gift in payload["star_gifts"]] == [10, 20]
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

from typing import cast

import pytest
from telethon import TelegramClient
from telethon.tl import types

from src.bot_api import BotApiClient
from src.config import Config
from src.models import StarGiftData
from src.notifier import Notifier
from src.store import StarGiftsStore


@pytest.fixture
def notifier(config: Config, logger) -> Notifier:
    return Notifier(
        client=cast(TelegramClient, None),
        bot_api=cast(BotApiClient, None),
        store=cast(StarGiftsStore, None),
        cfg=config,
        notify_peer=cast(types.TypeInputPeer, None),
        upgrades_peer=None,
        logger=logger,
    )


def _gift(**overrides) -> StarGiftData:
    fields = {
        "id": 1234,
        "number": 7,
        "price": 1500,
        "convert_price": 750,
        "available_amount": 0,
        "total_amount": 0,
        "is_limited": False,
    }
    fields.update(overrides)

    return StarGiftData(**fields)


def test_unlimited_gift_has_no_amount_blocks(notifier: Notifier):
    text = notifier.render_gift_text(_gift())

    assert "{" not in text
    assert "Total amount" not in text
    assert "Available amount" not in text
    assert "№ 7 (<code>1234</code>)" in text
    assert "💎 Price: 1,500 ⭐️" in text
    assert "♻️ Convert price: 750 ⭐️" in text
    assert "❄️ A new gift has appeared" in text


def test_limited_gift_reports_stock_and_percentage(notifier: Notifier):
    text = notifier.render_gift_text(
        _gift(is_limited=True, available_amount=5000, total_amount=10000)
    )

    assert "{" not in text
    assert "🔥 A new limited gift has appeared" in text
    assert "🎯 Total amount: 10,000" in text
    assert "❓ Available amount: 5,000 (50%," in text


def test_sold_out_gift_reports_duration(notifier: Notifier):
    text = notifier.render_gift_text(
        _gift(
            is_limited=True,
            available_amount=0,
            total_amount=10000,
            first_appearance_timestamp=1_700_000_000,
            last_sale_timestamp=1_700_090_061,
        )
    )

    assert "{" not in text
    assert "⏰ Completely sold out in 1 day, 1 hour, 1 minute and 1 second" in text
    assert "❓ Available amount: 0 (0%," in text


def test_premium_and_user_limited_blocks_are_joined(notifier: Notifier):
    text = notifier.render_gift_text(_gift(require_premium=True, user_limited=3))

    assert "{" not in text
    assert "✨ <b>Premium only</b> | <b>3 per user</b>" in text


def test_only_premium_has_no_separator(notifier: Notifier):
    text = notifier.render_gift_text(_gift(require_premium=True))

    assert "✨ <b>Premium only</b>\n" in text
    assert "per user" not in text


def test_validate_texts_accepts_the_example_config(notifier: Notifier):
    notifier.validate_texts()


def test_validate_texts_rejects_unknown_placeholder(config: Config, logger):
    raw = config.model_dump()
    raw["texts"]["notify"] = "{title} {nonexistent}"

    broken = Notifier(
        client=cast(TelegramClient, None),
        bot_api=cast(BotApiClient, None),
        store=cast(StarGiftsStore, None),
        cfg=Config.model_validate(raw),
        notify_peer=cast(types.TypeInputPeer, None),
        upgrades_peer=None,
        logger=logger,
    )

    with pytest.raises(KeyError):
        broken.validate_texts()

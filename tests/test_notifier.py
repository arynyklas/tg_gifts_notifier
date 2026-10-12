from dataclasses import replace
from typing import cast

import pytest
from telethon import TelegramClient, errors
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
    broken = Notifier(
        client=cast(TelegramClient, None),
        bot_api=cast(BotApiClient, None),
        store=cast(StarGiftsStore, None),
        cfg=replace(
            config, texts=replace(config.texts, new_gift="{title} {nonexistent}")
        ),
        notify_peer=cast(types.TypeInputPeer, None),
        upgrades_peer=None,
        logger=logger,
    )

    with pytest.raises(KeyError):
        broken.validate_texts()


@pytest.fixture
def _frozen_clock(monkeypatch):
    """The rendered timestamp is the only non-deterministic part of the text."""
    monkeypatch.setattr(
        "src.notifier.utils.get_current_datetime", lambda _: "01-01-2026 00:00:00"
    )


@pytest.mark.usefixtures("_frozen_clock")
def test_unlimited_text_is_byte_identical(notifier: Notifier):
    """Byte-identity with the pre-rewrite templates is a stated requirement.

    Substring assertions survive collapsing a blank line, which changes every
    message the channel has ever posted.
    """
    assert notifier.render_gift_text(_gift()) == (
        "❄️ A new gift has appeared\n"
        "\n"
        "№ 7 (<code>1234</code>)\n"
        "\n"
        "💎 Price: 1,500 ⭐️\n"
        "♻️ Convert price: 750 ⭐️\n"
        "\n"
    )


@pytest.mark.usefixtures("_frozen_clock")
def test_limited_text_with_an_exact_ratio_is_byte_identical(notifier: Notifier):
    text = notifier.render_gift_text(
        _gift(is_limited=True, available_amount=5000, total_amount=10000)
    )

    assert text == (
        "🔥 A new limited gift has appeared\n"
        "\n"
        "№ 7 (<code>1234</code>)\n"
        "\n"
        "🎯 Total amount: 10,000\n"
        "❓ Available amount: 5,000 (50%, updated at 01-01-2026 00:00:00 UTC)\n"
        "\n"
        "💎 Price: 1,500 ⭐️\n"
        "♻️ Convert price: 750 ⭐️\n"
        "\n"
    )


@pytest.mark.usefixtures("_frozen_clock")
def test_limited_text_with_a_rounded_ratio_marks_it_approximate(notifier: Notifier):
    """The only shape that renders the `~`: a percentage that does not survive
    rounding to one significant digit.
    """
    text = notifier.render_gift_text(
        _gift(is_limited=True, available_amount=4750, total_amount=10000)
    )

    assert text == (
        "🔥 A new limited gift has appeared\n"
        "\n"
        "№ 7 (<code>1234</code>)\n"
        "\n"
        "🎯 Total amount: 10,000\n"
        "❓ Available amount: 4,750 (~50%, updated at 01-01-2026 00:00:00 UTC)\n"
        "\n"
        "💎 Price: 1,500 ⭐️\n"
        "♻️ Convert price: 750 ⭐️\n"
        "\n"
    )


@pytest.mark.usefixtures("_frozen_clock")
def test_sold_out_text_is_byte_identical(notifier: Notifier):
    text = notifier.render_gift_text(
        _gift(
            is_limited=True,
            available_amount=0,
            total_amount=10000,
            first_appearance_timestamp=1_700_000_000,
            last_sale_timestamp=1_700_090_061,
        )
    )

    assert text == (
        "🔥 A new limited gift has appeared\n"
        "\n"
        "№ 7 (<code>1234</code>)\n"
        "\n"
        "🎯 Total amount: 10,000\n"
        "❓ Available amount: 0 (0%, updated at 01-01-2026 00:00:00 UTC)\n"
        "\n"
        "⏰ Completely sold out in 1 day, 1 hour, 1 minute and 1 second\n"
        "\n"
        "💎 Price: 1,500 ⭐️\n"
        "♻️ Convert price: 750 ⭐️\n"
        "\n"
    )


@pytest.mark.usefixtures("_frozen_clock")
def test_premium_and_user_limited_text_is_byte_identical(notifier: Notifier):
    text = notifier.render_gift_text(_gift(require_premium=True, user_limited=3))

    assert text == (
        "❄️ A new gift has appeared\n"
        "\n"
        "№ 7 (<code>1234</code>)\n"
        "\n"
        "💎 Price: 1,500 ⭐️\n"
        "♻️ Convert price: 750 ⭐️\n"
        "\n"
        "✨ <b>Premium only</b> | <b>3 per user</b>\n"
        "\n"
    )


def test_upgrade_text_is_byte_identical(config: Config):
    assert config.upgrades.text.format(id=1234) == (
        "Gift is upgradable! (<code>1234</code>)"
    )


class _RevokedBotApi:
    async def send_message(
        self,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
    ) -> int:
        raise errors.SessionRevokedError(request=None)


def _revoked_notifier(config: Config, store: StarGiftsStore, logger) -> Notifier:
    return Notifier(
        client=cast(TelegramClient, None),
        bot_api=cast(BotApiClient, _RevokedBotApi()),
        store=store,
        cfg=config,
        notify_peer=cast(types.TypeInputPeer, types.InputPeerEmpty()),
        upgrades_peer=cast(types.TypeInputPeer, types.InputPeerEmpty()),
        logger=logger,
    )


async def test_a_revoked_session_propagates_out_of_the_new_gift_announcement(
    config: Config, logger, tmp_path
):
    """`UnauthorizedError` is an `RPCError`, which this path swallows.

    Returning False would retry the announcement forever against a session that
    can never work again.
    """
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)
    gift = await store.add(_gift(sticker_message_id=1))

    with pytest.raises(errors.SessionRevokedError):
        await _revoked_notifier(config, store, logger).announce_new_gift(gift, None)


async def test_a_revoked_session_propagates_out_of_the_upgrade_announcement(
    config: Config, logger, tmp_path
):
    store = StarGiftsStore(tmp_path / "star_gifts.json", 0.0, logger)
    gift = await store.add(_gift(upgrade_sticker_message_id=1))

    with pytest.raises(errors.SessionRevokedError):
        await _revoked_notifier(config, store, logger).announce_upgrade(gift, None)

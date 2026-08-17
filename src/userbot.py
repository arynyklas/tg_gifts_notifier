import asyncio
from dataclasses import dataclass
from datetime import datetime
from logging import Logger

from telethon import TelegramClient, errors
from telethon.tl import functions, types

from src import utils
from src.config import TelegramConfig
from src.models import StarGiftData


@dataclass(frozen=True, slots=True)
class ParsedGift:
    data: StarGiftData
    sticker: types.Document | None


def build_client(cfg: TelegramConfig, logger: Logger) -> TelegramClient:
    cfg.session_path.parent.mkdir(parents=True, exist_ok=True)

    return TelegramClient(
        str(cfg.session_path),
        cfg.api_id,
        cfg.api_hash,
        flood_sleep_threshold=cfg.flood_sleep_threshold,
        receive_updates=False,  # the notifier never handles updates
        base_logger=logger.name,
    )


async def resolve_peer(
    client: TelegramClient, chat_id: int, logger: Logger
) -> types.TypeInputPeer:
    try:
        return await client.get_input_entity(chat_id)

    except ValueError:
        logger.warning("Peer %d not in session cache, priming dialogs", chat_id)

        await client.get_dialogs(limit=None)

        try:
            return await client.get_input_entity(chat_id)

        except ValueError as ex:
            raise RuntimeError(
                f"Userbot cannot resolve chat {chat_id}; join the chat first"
            ) from ex


def _timestamp(value: datetime | None) -> int | None:
    return int(value.timestamp()) if value is not None else None


def _parse_gift(raw: types.StarGift, number: int, logger: Logger) -> ParsedGift:
    sticker = raw.sticker if isinstance(raw.sticker, types.Document) else None

    if sticker is None:
        logger.warning("Gift %d has no usable sticker document", raw.id)

    return ParsedGift(
        data=StarGiftData(
            id=raw.id,
            number=number,
            price=raw.stars,
            convert_price=raw.convert_stars,
            available_amount=raw.availability_remains or 0,
            total_amount=raw.availability_total or 0,
            require_premium=bool(raw.require_premium),
            user_limited=(raw.per_user_total or 0) if raw.limited_per_user else None,
            is_limited=bool(raw.limited),
            first_appearance_timestamp=(
                _timestamp(raw.first_sale_date) or utils.current_timestamp()
            ),
            last_sale_timestamp=_timestamp(raw.last_sale_date),
        ),
        sticker=sticker,
    )


async def fetch_star_gifts(
    client: TelegramClient,
    gifts_hash: int,
    logger: Logger,
) -> tuple[int, dict[int, ParsedGift] | None]:
    """Fetch the gift catalog; `None` gifts mean the payload was not modified."""
    result = await client(functions.payments.GetStarGiftsRequest(hash=gifts_hash))

    if isinstance(result, types.payments.StarGiftsNotModified):
        return gifts_hash, None

    catalog = [gift for gift in result.gifts if isinstance(gift, types.StarGift)]

    parsed = {
        raw.id: _parse_gift(raw, number, logger)
        for number, raw in enumerate(sorted(catalog, key=lambda gift: gift.id), 1)
    }

    return result.hash, parsed


async def is_gift_upgradable(
    client: TelegramClient, gift_id: int, logger: Logger
) -> bool:
    """Probe `payments.getStarGiftUpgradePreview`.

    Only a `BadRequestError` means "not upgradable"; a flood wait is retried
    once and anything else propagates so the caller can leave the gift alone.
    """
    for attempt in (0, 1):
        try:
            await client(
                functions.payments.GetStarGiftUpgradePreviewRequest(gift_id=gift_id)
            )

            return True

        except errors.FloodWaitError as ex:
            if attempt:
                raise

            logger.warning(
                "FLOOD_WAIT %ds checking upgrade for gift %d", ex.seconds, gift_id
            )

            await asyncio.sleep(ex.seconds + 1)

        except errors.BadRequestError:
            return False

    return False

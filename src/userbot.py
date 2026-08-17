import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from logging import Logger
from typing import cast

from telethon import TelegramClient, errors
from telethon.errors import common
from telethon.tl import functions, types

from src import constants, utils
from src.config import TelegramConfig
from src.models import StarGiftData


class AuthorizationLostError(Exception):
    """The session can no longer be used; the process must stop."""


# UnauthorizedError/AuthKeyError are RPCError subclasses, so every generic
# `except RPCError` retry loop would otherwise swallow a revoked session.
AUTH_ERRORS: tuple[type[BaseException], ...] = (
    errors.UnauthorizedError,
    errors.AuthKeyError,
    common.AuthKeyNotFound,
)


@dataclass(frozen=True, slots=True)
class ParsedGift:
    data: StarGiftData
    sticker: types.Document | None


def build_client(
    cfg: TelegramConfig,
    logger: Logger,
    *,
    receive_updates: bool = False,
) -> TelegramClient:
    """Build the userbot client.

    The notifier never handles updates, so they are off by default: Telethon
    then wraps every request in `InvokeWithoutUpdatesRequest`. QR login is the
    exception — it completes only when the `UpdateLoginToken` update arrives,
    so that path must ask for updates explicitly.

    The session file is full account access with no 2FA prompt, so it and its
    directory are narrowed to the owner. `Path.chmod` is a documented no-op for
    the group and other bits on Windows, so no platform guard is needed.
    """
    cfg.session_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.session_path.parent.chmod(0o700)

    client = TelegramClient(
        str(cfg.session_path),
        cfg.api_id,
        cfg.api_hash,
        flood_sleep_threshold=cfg.flood_sleep_threshold,
        receive_updates=receive_updates,
        # Telethon's own loggers become children of this name, which
        # `get_logger` levels separately from ours.
        base_logger=f"{logger.name}.{constants.TELETHON_LOGGER_CHILD}",
    )

    if cfg.session_path.exists():
        cfg.session_path.chmod(0o600)

    return client


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


def _filename(document: types.Document) -> str:
    for attr in document.attributes:
        if isinstance(attr, types.DocumentAttributeFilename):
            return attr.file_name

    return f"{document.id}{constants.STICKER_SUFFIX}"


async def send_gift_sticker(
    client: TelegramClient,
    peer: types.TypeInputPeer,
    document: types.Document,
) -> types.Message:
    """Post a gift sticker by re-uploading its bytes.

    A gift's `sticker` is a *custom emoji* document (it carries
    `DocumentAttributeCustomEmoji`), and Telegram rejects those as message media
    with `DOCUMENT_INVALID`, so the document cannot simply be forwarded by
    reference. Downloading and re-uploading the bytes under the document's own
    mime type reproduces exactly what the pyrogram deployment posted: an
    `application/x-tgsticker` document with image-size and filename attributes.
    """
    # Telethon's hints cover neither documented case used here: a bare
    # `Document` as the source, and `file=bytes` asking for the bytes back.
    download = cast(
        "Callable[[types.Document, type[bytes]], Awaitable[bytes | None]]",
        client.download_media,
    )
    data = await download(document, bytes)

    if data is None:
        msg = f"Sticker document {document.id} has nothing to download"

        raise RuntimeError(msg)

    name = _filename(document)
    stream = BytesIO(data)
    stream.name = name

    return cast(
        "types.Message",
        await client.send_file(
            peer,
            stream,
            mime_type=document.mime_type,
            attributes=[types.DocumentAttributeFilename(name)],
            force_document=False,
        ),
    )


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
            upgrade_stars=raw.upgrade_stars,
        ),
        sticker=sticker,
    )


async def fetch_star_gifts(
    client: TelegramClient, logger: Logger
) -> dict[int, ParsedGift] | None:
    """Fetch the gift catalog; `None` means Telegram reported it unmodified.

    The hash is always `0` because stock changes are only observable in a full
    payload, so a cached hash would suppress exactly the updates this tool
    exists to report. `StarGiftsNotModified` is still part of the response
    union, so the branch stays.
    """
    result = await client(functions.payments.GetStarGiftsRequest(hash=0))

    if isinstance(result, types.payments.StarGiftsNotModified):
        return None

    catalog = [gift for gift in result.gifts if isinstance(gift, types.StarGift)]

    return {
        raw.id: _parse_gift(raw, number, logger)
        for number, raw in enumerate(sorted(catalog, key=lambda gift: gift.id), 1)
    }


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


async def probe_upgradable(
    client: TelegramClient,
    gift_ids: Sequence[int],
    slots: asyncio.Semaphore,
    logger: Logger,
) -> list[int]:
    """Probe many gifts concurrently; return the ids that are upgradable.

    A failed probe is logged and treated as "not upgradable" so one bad gift
    cannot sink the batch; `slots` bounds how many probes are in flight. An auth
    failure and a hard flood wait are re-raised instead: the first must stop the
    process, and absorbing the second re-probed the whole set on the next tick.
    """

    async def probe(gift_id: int) -> bool:
        async with slots:
            return await is_gift_upgradable(client, gift_id, logger)

    results = await asyncio.gather(
        *(probe(gift_id) for gift_id in gift_ids), return_exceptions=True
    )
    upgradable: list[int] = []

    for gift_id, result in zip(gift_ids, results, strict=True):
        if isinstance(result, (*AUTH_ERRORS, errors.FloodWaitError)):
            raise result

        if isinstance(result, BaseException):
            logger.error(
                "Failed to check upgradability of gift %d", gift_id, exc_info=result
            )

        elif result:
            upgradable.append(gift_id)

    return upgradable

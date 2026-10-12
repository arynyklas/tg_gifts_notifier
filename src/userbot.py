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
    session_path = cfg.session_path

    if not session_path.name.endswith(".session"):
        # `SQLiteSession` appends the suffix to a name not ending in it (the
        # same `endswith` test, so a name of exactly `.session` is kept);
        # passing it the normalized path keeps the file narrowed below the one
        # it actually opens.
        session_path = session_path.with_name(session_path.name + ".session")

    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.parent.chmod(0o700)

    client = TelegramClient(
        str(session_path),
        cfg.api_id,
        cfg.api_hash,
        flood_sleep_threshold=cfg.flood_sleep_threshold,
        receive_updates=receive_updates,
        # After its automatic retries of a server-side error Telethon otherwise
        # raises a bare `ValueError`, which escapes every `RPCError`/`OSError`
        # handler; this raises the last `RPCError` instead.
        raise_last_call_error=True,
        # Telethon's own loggers become children of this name, which
        # `get_logger` levels separately from ours.
        base_logger=f"{logger.name}.{constants.TELETHON_LOGGER_CHILD}",
    )

    if session_path.exists():
        session_path.chmod(0o600)

    return client


async def ensure_connected(
    client: TelegramClient, lock: asyncio.Lock, logger: Logger
) -> None:
    """Reconnect a client that Telethon has stopped reconnecting.

    After `connection_retries` failed attempts Telethon's sender disconnects for
    good: every later request raises `ConnectionError` and nothing in Telethon
    connects again. The request loops call this before each cycle, and
    `Detector.connection_loop` on a timer for loops stuck on a request; a failed
    `connect()` propagates to the caller's transient-error handling and is
    retried next cycle. `lock` keeps two callers from connecting at once.

    The client is fully disconnected before connecting again: the sender giving
    up leaves the client's update loop running, and every `connect()` starts
    another one.

    Telegram no longer knowing the auth key ends the session like a revoked
    one. Only a request in flight at that moment gets `AuthKeyNotFound`, every
    later one a plain `ConnectionError`, so the reason is read from
    `client.disconnected` instead.
    """
    if client.is_connected():
        return

    async with lock:
        if client.is_connected():
            return  # another loop reconnected while this one waited

        # `is_connected()` turns false before the teardown that records the
        # reason has finished, and a `connect()` during it races that teardown.
        disconnected = client.disconnected

        try:
            await asyncio.wait([disconnected])

        except asyncio.CancelledError:
            # `wait` leaves the shield attached, and once teardown records its
            # reason nobody reads it: asyncio logs "exception was never
            # retrieved". Cancelling the shield is not enough, it then detaches
            # and leaves Telethon's own future unread; consuming the result keeps
            # the shield attached, and it reads the inner future as it resolves.
            disconnected.add_done_callback(_consume_result)

            raise

        # Reading the exception also stops asyncio from logging it as unretrieved.
        reason = disconnected.exception()

        if isinstance(reason, common.AuthKeyNotFound):
            msg = "Telegram no longer knows this session's auth key"

            raise AuthorizationLostError(msg) from reason

        logger.warning("Userbot connection lost (%r), reconnecting", reason)

        # The sender giving up does not stop the client's update loop, and
        # `connect()` starts a second one. Both then fetch the difference at the
        # same deadline, the loser crashes, and Telethon disconnects itself,
        # cancelling every request in flight. Only a client-level disconnect
        # cancels the stale loops; the session it closes reopens on next use.
        await client.disconnect()
        await client.connect()


def _consume_result(future: asyncio.Future[None]) -> None:
    if not future.cancelled():
        future.exception()


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


def _revive_dead_file_senders(client: TelegramClient, logger: Logger) -> None:
    """Make Telethon reconnect cached per-DC file senders that gave up.

    Downloads of files on another DC hit `FILE_MIGRATE_X` and borrow a cached
    exported sender (`client/downloads.py:112-116`). `_borrow_exported_sender`
    reconnects a cached sender only when `state.need_connect()`
    (`client/telegrambaseclient.py:857-887`, `:66-67`) and never checks
    `sender.is_connected()`. The state resets only on idle cleanup, which needs
    zero outstanding borrows (`:61-64`, `:899-911`), but a failed download never
    returns its borrow (`downloads.py:565-601`, `:78-85`). So a sender that gave
    up reconnecting stays dead: every later download from that DC raises
    `ConnectionError` until a restart, or until the main connection is reset
    (`_disconnect_coro` clears the cache, `:724-743`).

    Telethon exposes no public hook, hence the private `_borrowed_senders` and
    `_ExportState._connected`. Clearing `_connected` puts the sender back on
    Telethon's own borrow path, which reconnects the SAME sender (keeping its
    auth key and imported authorization) and whose new send loop also sends
    anything still queued. A sender Telethon is still retrying
    (`is_connected()` True) is left alone.
    """
    for state, sender in client._borrowed_senders.values():
        if not state.need_connect() and not sender.is_connected():
            state._connected = False
            logger.warning("Reviving dead file sender for DC %d", sender.dc_id)


async def send_gift_sticker(
    client: TelegramClient,
    peer: types.TypeInputPeer,
    document: types.Document,
    logger: Logger,
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

    _revive_dead_file_senders(client, logger)

    # A download queued on a file sender that later gives up is never answered
    # (`network/mtprotosender.py:177-190`, `:330-338`), and the main-client
    # watchdog cannot release it. The bound exceeds `flood_sleep_threshold`
    # because Telethon sleeps flood waits below it inside the call. The
    # resulting `TimeoutError` is an `OSError`, so callers schedule a retry.
    async with asyncio.timeout(
        client.flood_sleep_threshold + constants.STICKER_DOWNLOAD_TIMEOUT
    ):
        data = await download(document, bytes)

    if data is None:
        msg = f"Sticker document {document.id} has nothing to download"

        raise RuntimeError(msg)

    name = _filename(document)
    stream = BytesIO(data)
    stream.name = name

    # The upload is deliberately unbounded: cutting short a slow but successful
    # sendMedia would post the sticker twice on retry, and main-sender hangs are
    # already released by `Detector.connection_loop`.
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
    *,
    strict: bool = False,
) -> list[int]:
    """Probe many gifts concurrently; return the ids that are upgradable.

    A failed probe is logged and treated as "not upgradable" so one bad gift
    cannot sink the batch; `slots` bounds how many probes are in flight. An auth
    failure and a hard flood wait are re-raised instead: the first must stop the
    process, and absorbing the second re-probed the whole set on the next tick.
    An auth failure wins over a flood wait wherever either sits in `gift_ids`:
    sleeping out the wait only delays the exit a revoked session needs.

    `strict` re-raises every other failure too, once all probes have settled and
    only when no auth failure or flood wait was found: a baseline that records a
    failed probe as "not upgradable" makes the next upgrades cycle announce that
    gift as a fresh upgrade.
    """

    async def probe(gift_id: int) -> bool:
        async with slots:
            return await is_gift_upgradable(client, gift_id, logger)

    results = await asyncio.gather(
        *(probe(gift_id) for gift_id in gift_ids), return_exceptions=True
    )
    upgradable: list[int] = []
    auth_error: BaseException | None = None
    flood: BaseException | None = None
    failure: BaseException | None = None

    for gift_id, result in zip(gift_ids, results, strict=True):
        if isinstance(result, AUTH_ERRORS):
            auth_error = auth_error or result

        elif isinstance(result, errors.FloodWaitError):
            flood = flood or result

        elif isinstance(result, BaseException):
            if strict:
                failure = failure or result

                continue

            logger.error(
                "Failed to check upgradability of gift %d", gift_id, exc_info=result
            )

        elif result:
            upgradable.append(gift_id)

    for error in (auth_error, flood, failure):
        if error is not None:
            raise error

    return upgradable

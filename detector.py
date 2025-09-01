from pyrogram import Client, types
from pyrogram.file_id import FileId
from httpx import AsyncClient, TimeoutException
from pytz import timezone as _timezone
from io import BytesIO
from itertools import cycle
from functools import partial

import math
import asyncio
import typing

from parse_data import get_all_star_gifts, check_is_star_gift_upgradable
from star_gifts_data import StarGiftData, StarGiftsData

import utils
import userbot_helpers
import constants
import config


timezone = _timezone(config.TIMEZONE)

USERBOT_SLEEP_THRESHOLD = 60
BATCH_STICKERS_DOWNLOAD = True


T = typing.TypeVar("T")
STAR_GIFT_RAW_T = dict[str, typing.Any]
UPDATE_GIFTS_QUEUE_T = asyncio.Queue[tuple[StarGiftData, StarGiftData]]

BASIC_REQUEST_DATA = {
    "parse_mode": "HTML",
    "disable_web_page_preview": True
}


BOTS_AMOUNT = len(config.BOT_TOKENS)

if BOTS_AMOUNT > 0:
    BOT_HTTP_CLIENT = AsyncClient(
        base_url = "https://api.telegram.org/",
        timeout = config.HTTP_REQUEST_TIMEOUT
    )

    BOT_TOKENS_CYCLE = cycle(config.BOT_TOKENS)


STAR_GIFTS_DATA = StarGiftsData.load(config.DATA_FILEPATH)

logger = utils.get_logger(
    name = config.SESSION_NAME,
    log_filepath = constants.LOG_FILEPATH,
    console_log_level = config.CONSOLE_LOG_LEVEL,
    file_log_level = config.FILE_LOG_LEVEL
)


@typing.overload
async def bot_send_request(
    method: str,
    data: dict[str, typing.Any] | None
) -> dict[str, typing.Any]: ...

@typing.overload
async def bot_send_request(
    method: typing.Literal["editMessageText"],
    data: dict[str, typing.Any]
) -> dict[str, typing.Any] | None: ...

async def bot_send_request(
    method: str,
    data: dict[str, typing.Any] | None = None
) -> dict[str, typing.Any] | None:
    logger.debug(f"Sending request {method} with data: {data}")

    retries = BOTS_AMOUNT
    response = None

    for bot_token in BOT_TOKENS_CYCLE:
        retries -= 1

        if retries < 0:
            logger.error(f"Exceeded bot token retries for method {method}")

            break

        try:
            response = (await BOT_HTTP_CLIENT.post(
                f"/bot{bot_token}/{method}",
                json = data
            )).json()

        except TimeoutException:
            logger.warning(f"Timeout exception while sending request {method} with data: {data}")

            continue

        except Exception as ex:
            logger.error(f"An error occurred while sending request {method}: {ex}")

            continue

        if response.get("ok"):
            return response.get("result")

        elif method == "editMessageText" and isinstance(response.get("description"), str) and "message is not modified" in response["description"]:
            return

        logger.warning(f"Telegram API error for method {method}: {response}")

    raise RuntimeError(f"Failed to send request to Telegram API after multiple retries. Last response: {response}")


async def detector(
    app: Client,
    new_gift_callback: typing.Callable[[StarGiftData, BytesIO | None], typing.Coroutine[None, None, typing.Any]] | None = None,
    update_gifts_queue: UPDATE_GIFTS_QUEUE_T | None = None,
    save_only: bool = False
) -> None:
    if new_gift_callback is None and update_gifts_queue is None:
        raise ValueError("At least one of new_gift_callback or update_gifts_queue must be provided")

    current_hash = 0

    while True:
        logger.debug("Checking for new gifts / updates...")

        if not app.is_connected:
            try:
                await app.start()

            except Exception as ex:
                logger.error(f"Failed to start Pyrogram client: {ex}")

                await asyncio.sleep(config.CHECK_INTERVAL)

                continue

        new_hash, all_star_gifts_dict = await get_all_star_gifts(app, current_hash)

        if save_only:
            if all_star_gifts_dict is None:
                logger.debug("No new gifts found, exiting save-only mode.")

                return

            logger.info(f"Gifts found, saving data to {STAR_GIFTS_DATA.DATA_FILEPATH}")

            STAR_GIFTS_DATA.star_gifts = [
                StarGiftData.model_validate(star_gift)
                for star_gift in all_star_gifts_dict.values()
            ]

            await star_gifts_data_saver()

            return

        if all_star_gifts_dict is None:
            logger.debug("Star gifts data not modified.")

            await asyncio.sleep(config.CHECK_INTERVAL)

            continue

        if not update_gifts_queue:
            current_hash = new_hash

        old_star_gifts_dict = {
            star_gift.id: star_gift
            for star_gift in STAR_GIFTS_DATA.star_gifts
        }

        new_star_gifts_found: list[StarGiftData] = []

        for star_gift_id, star_gift in all_star_gifts_dict.items():
            if star_gift_id not in old_star_gifts_dict:
                new_star_gifts_found.append(star_gift)

        if new_star_gifts_found and new_gift_callback:
            logger.info(f"""Found {len(new_star_gifts_found)} new gifts: [{", ".join(map(str, [g.id for g in new_star_gifts_found]))}]""")

            if BATCH_STICKERS_DOWNLOAD:
                logger.debug("Downloading all new gift stickers in batch...")

                sticker_file_id_objs = {
                    star_gift.id: FileId.decode(star_gift.sticker_file_id)
                    for star_gift in new_star_gifts_found
                }

                documents_data: dict[int, list[tuple[int, int, bytes]]] = {}

                for star_gift in new_star_gifts_found:
                    sticker_file_id_obj = sticker_file_id_objs.get(star_gift.id)

                    if not sticker_file_id_obj:
                        logger.warning(f"Invalid sticker file ID for new star gift {star_gift.id}, skipping download.")

                        continue

                    if sticker_file_id_obj.dc_id not in documents_data:
                        documents_data[sticker_file_id_obj.dc_id] = []

                    documents_data[sticker_file_id_obj.dc_id].append((
                        sticker_file_id_obj.media_id,
                        sticker_file_id_obj.access_hash,
                        sticker_file_id_obj.file_reference
                    ))

                downloaded_stickers_data = await userbot_helpers.download_documents(
                    client = app,
                    documents_data = documents_data,
                    logger = logger
                )

                downloaded_stickers_mapped = {
                    star_gift_id: downloaded_stickers_data[sticker_file_id_obj.media_id]
                    for star_gift_id, sticker_file_id_obj in sticker_file_id_objs.items()
                    if sticker_file_id_obj
                }

                logger.info(f"Batch download of {len(downloaded_stickers_mapped)} completed.")

            for star_gift in sorted(new_star_gifts_found, key=lambda sg: sg.total_amount):
                await new_gift_callback(
                    star_gift,
                    downloaded_stickers_mapped.get(star_gift.id) if BATCH_STICKERS_DOWNLOAD else None  # pyright: ignore[reportPossiblyUnboundVariable]
                )

                STAR_GIFTS_DATA.star_gifts.append(star_gift)

                await star_gifts_data_saver()

        elif new_star_gifts_found:
            STAR_GIFTS_DATA.star_gifts.extend(new_star_gifts_found)

            await star_gifts_data_saver()

        if update_gifts_queue:
            for star_gift_id, old_star_gift in old_star_gifts_dict.items():
                new_star_gift = all_star_gifts_dict.get(star_gift_id)

                if new_star_gift is None:
                    logger.warning(f"Star gift {star_gift_id} not found in new gifts, skipping for updating.")

                    continue

                if new_star_gift.available_amount < old_star_gift.available_amount:
                    new_star_gift.message_id = old_star_gift.message_id
                    new_star_gift.is_upgradable = old_star_gift.is_upgradable

                    update_gifts_queue.put_nowait((old_star_gift, new_star_gift))

        await star_gifts_data_saver()

        await asyncio.sleep(config.CHECK_INTERVAL)


def get_notify_text(star_gift: StarGiftData) -> str:
    is_limited = star_gift.is_limited

    available_percentage_str = constants.NULL_STR
    available_percentage_is_same = False

    if is_limited and star_gift.total_amount > 0:
        available_percentage_str, available_percentage_is_same = utils.pretty_float(
            math.ceil(star_gift.available_amount / star_gift.total_amount * 100 * 100) / 100,
            get_is_same = True
        )

    return config.NOTIFY_TEXT.format(
        title = config.NOTIFY_TEXT_TITLES[is_limited],
        number = star_gift.number,
        id = star_gift.id,
        total_amount = (
            config.NOTIFY_TEXT_TOTAL_AMOUNT.format(
                total_amount = utils.pretty_int(star_gift.total_amount)
            )
            if is_limited else
            constants.NULL_STR
        ),
        available_amount = (
            config.NOTIFY_TEXT_AVAILABLE_AMOUNT.format(
                available_amount = utils.pretty_int(star_gift.available_amount),
                same_str = (
                    constants.NULL_STR
                    if available_percentage_is_same else
                    "~"
                ),
                available_percentage = available_percentage_str,
                updated_datetime = utils.get_current_datetime(timezone)
            )
            if is_limited else
            constants.NULL_STR
        ),
        sold_out = (
            config.NOTIFY_TEXT_SOLD_OUT.format(
                sold_out = utils.format_seconds_to_human_readable(star_gift.last_sale_timestamp - star_gift.first_appearance_timestamp)
            )
            if star_gift.last_sale_timestamp and star_gift.first_appearance_timestamp else
            constants.NULL_STR
        ),
        price = utils.pretty_int(star_gift.price),
        convert_price = utils.pretty_int(star_gift.convert_price),
        require_premium_or_user_limited = (
            config.NOTIFY_TEXT_REQUIRE_PREMIUM_OR_USER_LIMITED.format(
                emoji = config.NOTIFY_TEXT_REQUIRE_PREMIUM_OR_USER_LIMITED_EMOJI,
                require_premium = (
                    config.NOTIFY_TEXT_REQUIRE_PREMIUM
                    if star_gift.require_premium else
                    constants.NULL_STR
                ),
                user_limited = (
                    config.NOTIFY_TEXT_USER_LIMITED.format(
                        user_limited = utils.pretty_int(star_gift.user_limited)
                    )
                    if star_gift.user_limited is not None else
                    constants.NULL_STR
                ),
                separator = (
                    config.NOTIFY_TEXT_REQUIRE_PREMIUM_AND_USER_LIMITED_SEPARATOR
                    if star_gift.require_premium and star_gift.user_limited is not None else
                    constants.NULL_STR
                )
            )
            if star_gift.require_premium or star_gift.user_limited is not None else
            constants.NULL_STR
        )
    )


async def process_new_gift(app: Client, star_gift: StarGiftData, sticker_binary: BytesIO | None) -> None:
    if not sticker_binary:
        sticker_binary = typing.cast(BytesIO, await app.download_media(  # pyright: ignore[reportUnknownMemberType]
            message = star_gift.sticker_file_id,
            in_memory = True
        ))

    sticker_binary.seek(0)
    sticker_binary.name = star_gift.sticker_file_name

    try:
        sticker_message = typing.cast(types.Message, await app.send_sticker(  # pyright: ignore[reportUnknownMemberType]
            chat_id = config.NOTIFY_CHAT_ID,
            sticker = sticker_binary
        ))

        await asyncio.sleep(config.NOTIFY_AFTER_STICKER_DELAY)

        response = await bot_send_request(
            "sendMessage",
            {
                "chat_id": config.NOTIFY_CHAT_ID,
                "text": get_notify_text(star_gift),
                "reply_to_message_id": sticker_message.id
            } | BASIC_REQUEST_DATA
        )

        if response and "message_id" in response:
            star_gift.message_id = response["message_id"]

            logger.info(f"Sent notification for new gift {star_gift.id}, message_id: {star_gift.message_id}")

        else:
            logger.warning(f"Failed to get message_id for new gift {star_gift.id} notification.")

    except Exception as ex:
        logger.exception(f"Error processing new gift {star_gift.id}", exc_info=ex)


async def process_update_gifts(update_gifts_queue: UPDATE_GIFTS_QUEUE_T) -> None:
    ignore_gift_ids: set[int] = set()

    while True:
        gifts_to_update: list[tuple[StarGiftData, StarGiftData]] = []

        while True:
            try:
                old_star_gift, new_star_gift = update_gifts_queue.get_nowait()
                gifts_to_update.append((old_star_gift, new_star_gift))
                update_gifts_queue.task_done()

            except asyncio.QueueEmpty:
                break

        if not gifts_to_update:
            await asyncio.sleep(0.1)

            continue

        gifts_to_update = sorted(gifts_to_update, key=lambda gift_pair: gift_pair[0].first_appearance_timestamp or 0)

        for old_star_gift, new_star_gift in gifts_to_update:
            if new_star_gift in ignore_gift_ids:
                continue

            elif new_star_gift.message_id is None:
                logger.warning(f"Cannot update star gift {new_star_gift.id}: message_id is None, ignoring.")

                ignore_gift_ids.add(new_star_gift.id)

                continue

            try:
                await bot_send_request(
                    "editMessageText",
                    {
                        "chat_id": config.NOTIFY_CHAT_ID,
                        "message_id": new_star_gift.message_id,
                        "text": get_notify_text(new_star_gift)
                    } | BASIC_REQUEST_DATA
                )

                logger.debug(f"Available amount of star gift {new_star_gift.id} updated from {old_star_gift.available_amount} to {new_star_gift.available_amount} (message #{new_star_gift.message_id}).")

                stored_star_gift_index = next((
                    i
                    for i, stored_gift in enumerate(STAR_GIFTS_DATA.star_gifts)
                    if stored_gift.id == new_star_gift.id
                ), None)

                if stored_star_gift_index is None:
                    logger.warning(f"Stored star gift {new_star_gift.id} not found for update.")

                    continue

                STAR_GIFTS_DATA.star_gifts[stored_star_gift_index] = new_star_gift

                await star_gifts_data_saver()

            except Exception as ex:
                logger.exception(f"Error updating gift message for {new_star_gift.id}", exc_info=ex)


star_gifts_data_saver_lock = asyncio.Lock()
last_star_gifts_data_saved_time = 0

async def star_gifts_data_saver() -> None:
    global STAR_GIFTS_DATA, last_star_gifts_data_saved_time

    async with star_gifts_data_saver_lock:
        current_time = utils.get_current_timestamp()

        if current_time - last_star_gifts_data_saved_time >= config.DATA_SAVER_DELAY:
            STAR_GIFTS_DATA.save()

            last_star_gifts_data_saved_time = current_time

            logger.debug("Saved star gifts data file.")

        else:
            logger.debug(f"Skipping data save. Next save in {config.DATA_SAVER_DELAY - (current_time - last_star_gifts_data_saved_time)} seconds.")


async def star_gifts_upgrades_checker(app: Client) -> None:
    while True:
        gifts_to_check = [
            star_gift
            for star_gift in STAR_GIFTS_DATA.star_gifts
            if not star_gift.is_upgradable
        ]

        if not gifts_to_check:
            logger.debug("No non-upgradable star gifts to check.")

            await asyncio.sleep(config.CHECK_UPGRADES_PER_CYCLE)

            continue

        upgradable_star_gifts: list[StarGiftData] = []

        for star_gift in gifts_to_check:
            logger.debug(f"Checking if star gift {star_gift.id} is upgradable...")

            if await check_is_star_gift_upgradable(
                app = app,
                star_gift_id = star_gift.id
            ):
                logger.info(f"Star gift {star_gift.id} is now upgradable.")

                upgradable_star_gifts.append(star_gift)

            else:
                logger.debug(f"Star gift {star_gift.id} is still not upgradable.")

        if upgradable_star_gifts:
            if BATCH_STICKERS_DOWNLOAD:
                logger.info("Downloading all upgradable gift stickers in batch...")

                sticker_file_id_objs = {
                    star_gift.id: FileId.decode(star_gift.sticker_file_id)
                    for star_gift in upgradable_star_gifts
                }

                documents_data: dict[int, list[tuple[int, int, bytes]]] = {}

                for star_gift in upgradable_star_gifts:
                    sticker_file_id_obj = sticker_file_id_objs.get(star_gift.id)

                    if not sticker_file_id_obj:
                        logger.warning(f"Invalid sticker file ID upgradable for star gift {star_gift.id}, skipping download.")

                        continue

                    if sticker_file_id_obj.dc_id not in documents_data:
                        documents_data[sticker_file_id_obj.dc_id] = []

                    documents_data[sticker_file_id_obj.dc_id].append((
                        sticker_file_id_obj.media_id,
                        sticker_file_id_obj.access_hash,
                        sticker_file_id_obj.file_reference
                    ))

                downloaded_stickers_data = await userbot_helpers.download_documents(
                    client = app,
                    documents_data = documents_data,
                    logger = logger
                )

                downloaded_stickers_mapped = {
                    star_gift_id: downloaded_stickers_data[sticker_file_id_obj.media_id]
                    for star_gift_id, sticker_file_id_obj in sticker_file_id_objs.items()
                    if sticker_file_id_obj
                }

                logger.info(f"Batch download of {len(downloaded_stickers_mapped)} completed.")

            for star_gift in upgradable_star_gifts:
                logger.info(f"""Sending upgrade notification for star gift {star_gift.id}.""")

                try:
                    sticker_binary = downloaded_stickers_mapped.get(star_gift.id) if BATCH_STICKERS_DOWNLOAD else None  # pyright: ignore[reportPossiblyUnboundVariable]

                    if not sticker_binary:
                        sticker_binary = typing.cast(BytesIO, await app.download_media(  # pyright: ignore[reportUnknownMemberType]
                            message = star_gift.sticker_file_id,
                            in_memory = True
                        ))

                    sticker_binary.seek(0)
                    sticker_binary.name = star_gift.sticker_file_name

                    sticker_message = typing.cast(types.Message, await app.send_sticker(  # pyright: ignore[reportUnknownMemberType]
                        chat_id = config.NOTIFY_UPGRADES_CHAT_ID,
                        sticker = sticker_binary
                    ))

                    await asyncio.sleep(config.NOTIFY_AFTER_STICKER_DELAY)

                    await bot_send_request(
                        "sendMessage",
                        {
                            "chat_id": config.NOTIFY_UPGRADES_CHAT_ID,
                            "text": config.NOTIFY_UPGRADES_TEXT.format(
                                id = star_gift.id
                            ),
                            "reply_to_message_id": sticker_message.id
                        } | BASIC_REQUEST_DATA
                    )

                    logger.info(f"Upgrade notification sent for gift {star_gift.id}.")

                    await asyncio.sleep(config.NOTIFY_AFTER_TEXT_DELAY)

                except Exception as ex:
                    logger.exception(f"Error sending upgrade notification for gift {star_gift.id}", exc_info=ex)

                stored_star_gift = next((
                    sg
                    for sg in STAR_GIFTS_DATA.star_gifts
                    if sg.id == star_gift.id
                ), None)

                if not stored_star_gift:
                    logger.warning(f"Stored star gift {star_gift.id} not found.")

                    return

                stored_star_gift.is_upgradable = True

                await star_gifts_data_saver()

        logger.debug("Star gifts upgrades one loop completed.")

        await asyncio.sleep(config.CHECK_UPGRADES_PER_CYCLE)


async def logger_wrapper(coro: typing.Awaitable[T]) -> T | None:
    try:
        return await coro

    except asyncio.CancelledError:
        logger.info(f"Task {getattr(coro, '__name__', coro)} was cancelled.")

        return None

    except Exception as ex:
        logger.exception(f"""Error in {getattr(coro, "__name__", coro)}""", exc_info=ex)

        return None


async def main(save_only: bool=False) -> None:
    global STAR_GIFTS_DATA

    logger.info("Starting gifts detector...")

    if save_only:
        logger.info("Save only mode enabled, skipping gift detection.")

        if STAR_GIFTS_DATA.star_gifts:
            star_gifts_data_filepath = STAR_GIFTS_DATA.DATA_FILEPATH.with_name(
                f"""star_gifts_dump_{utils.get_current_datetime(timezone).replace(":", "-")}.json"""
            )

            STAR_GIFTS_DATA.DATA_FILEPATH = star_gifts_data_filepath

            STAR_GIFTS_DATA.save()

            logger.info(f"Old star gifts dump saved to {star_gifts_data_filepath}.")

        STAR_GIFTS_DATA = StarGiftsData.load(config.DATA_FILEPATH, new=True)  # pyright: ignore[reportConstantRedefinition]
        STAR_GIFTS_DATA.save()

    app = Client(
        name = config.SESSION_NAME,
        api_id = config.API_ID,
        api_hash = config.API_HASH,
        sleep_threshold = USERBOT_SLEEP_THRESHOLD
    )

    try:
        await app.start()

    except Exception as ex:
        logger.critical(f"Failed to start Pyrogram client, exiting: {ex}")

        return

    logger.info("Pyrogram client started.")

    update_gifts_queue = (
        UPDATE_GIFTS_QUEUE_T()
        if BOTS_AMOUNT > 0 else
        None
    )

    tasks: list[asyncio.Task[typing.Any]] = []

    if update_gifts_queue and not save_only:
        tasks.append(asyncio.create_task(logger_wrapper(
            process_update_gifts(
                update_gifts_queue = update_gifts_queue
            )
        )))

        logger.info("Update gifts processing task started.")

    elif not save_only:
        logger.info("No bots available, skipping update gifts processing.")

    if config.NOTIFY_UPGRADES_CHAT_ID and not save_only:
        tasks.append(asyncio.create_task(logger_wrapper(
            star_gifts_upgrades_checker(app)
        )))

        logger.info("Star gifts upgrades checker task started.")

    elif not save_only:
        logger.info("Upgrades channel is not set, skipping star gifts upgrades checking.")

    tasks.append(asyncio.create_task(logger_wrapper(
        detector(
            app = app,
            new_gift_callback = partial(process_new_gift, app),
            update_gifts_queue = update_gifts_queue,
            save_only = save_only
        )
    )))

    logger.info("Main detector task started.")

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    import sys

    try:
        asyncio.run(main(
            save_only = "--save-only" in sys.argv or "-S" in sys.argv
        ))

    except KeyboardInterrupt:
        logger.info("Detector stopped by user (KeyboardInterrupt).")

    except Exception as ex:
        logger.critical(f"An unhandled exception occurred in main: {ex}")

    finally:
        logger.info("Saving star gifts data before exit...")

        STAR_GIFTS_DATA.save()

        logger.info("Star gifts data saved. Exiting.")

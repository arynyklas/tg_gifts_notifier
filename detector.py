from pyrogram import Client, types, filters
from httpx import AsyncClient, TimeoutException
import pyrogram
from pytz import timezone as _timezone
from io import BytesIO
from itertools import cycle, groupby
from bisect import bisect_left
from functools import partial
import json
import os

import math
import asyncio
import typing

from parse_data import get_all_star_gifts, check_is_star_gift_upgradable
from star_gifts_data import StarGiftData, StarGiftsData

import utils
import constants
import config

timezone = _timezone(config.TIMEZONE)

NULL_STR = ""

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
last_star_gifts_data_saved_time: int | None = None

# Файл для хранения известных подарков
KNOWN_GIFTS_FILE = "known_gifts.json"

logger = utils.get_logger(
    name = config.SESSION_NAME,
    log_filepath = constants.LOG_FILEPATH,
    console_log_level = config.CONSOLE_LOG_LEVEL,
    file_log_level = config.FILE_LOG_LEVEL
)

def load_known_gifts() -> set[int]:
    """Загружает список известных подарков из файла"""
    if os.path.exists(KNOWN_GIFTS_FILE):
        try:
            with open(KNOWN_GIFTS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return set(data.get('known_gift_ids', []))
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Ошибка при загрузке файла известных подарков: {e}")
            return set()
    return set()


def save_known_gifts(known_gift_ids: set[int]) -> None:
    """Сохраняет список известных подарков в файл"""
    try:
        with open(KNOWN_GIFTS_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                'known_gift_ids': list(known_gift_ids),
                'last_updated': utils.get_current_timestamp()
            }, f, indent=2, ensure_ascii=False)
        logger.debug(f"Сохранено {len(known_gift_ids)} известных подарков")
    except IOError as e:
        logger.error(f"Ошибка при сохранении файла известных подарков: {e}")

SUBSCRIBERS_FILE = "subscribers.json"

def load_subscribers() -> set[int]:
    if os.path.exists(SUBSCRIBERS_FILE):
        with open(SUBSCRIBERS_FILE, 'r', encoding='utf-8') as f:
            return set(json.load(f))
    return set()

def save_subscribers(subscribers: set[int]) -> None:
    with open(SUBSCRIBERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(subscribers), f, indent=2)

subscribers = load_subscribers()

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
            break

        try:
            response = (await BOT_HTTP_CLIENT.post(
                f"/bot{bot_token}/{method}",
                json = data
            )).json()

        except TimeoutException:
            logger.warning(f"Timeout exception while sending request {method} with data: {data}")

            continue

        if response.get("ok"):
            return response.get("result")

        elif method == "editMessageText" and isinstance(response.get("description"), str) and "message is not modified" in response["description"]:
            return

    raise RuntimeError(f"Failed to send request to Telegram API: {response}")


async def detector(
    app: Client,
    new_gift_callback: typing.Callable[[StarGiftData], typing.Coroutine[None, None, typing.Any]] | None = None,
    update_gifts_queue: UPDATE_GIFTS_QUEUE_T | None = None
) -> None:
    if new_gift_callback is None and update_gifts_queue is None:
        raise ValueError("At least one of new_gift_callback or update_gifts_queue must be provided")

    # Загружаем известные подарки
    known_gift_ids = load_known_gifts()
    is_first_run = len(known_gift_ids) == 0
    
    logger.info(f"Загружено {len(known_gift_ids)} известных подарков. Первый запуск: {is_first_run}")

    while True:
        logger.debug("Checking for new gifts / updates...")

        if not app.is_connected:
            await app.start()

        _, all_star_gifts_dict = await get_all_star_gifts(app)

        old_star_gifts_dict = {
            star_gift.id: star_gift
            for star_gift in STAR_GIFTS_DATA.star_gifts
        }

        # Текущие ID подарков из API
        current_gift_ids = set(all_star_gifts_dict.keys())
        
        if is_first_run:
            # При первом запуске сохраняем все подарки как известные
            logger.info(f"Первый запуск: найдено {len(current_gift_ids)} подарков, сохраняем как известные")
            known_gift_ids = current_gift_ids.copy()
            save_known_gifts(known_gift_ids)
            is_first_run = False
            
            # Также сохраняем все подарки в основное хранилище
            if current_gift_ids:
                new_gifts_for_storage = [
                    star_gift for star_gift in all_star_gifts_dict.values()
                    if star_gift.id not in old_star_gifts_dict
                ]
                if new_gifts_for_storage:
                    await star_gifts_data_saver(new_gifts_for_storage)
            
            logger.info("Первоначальная инициализация завершена, начинаем мониторинг новых подарков")
        else:
            # Обычная логика - ищем только реально новые подарки
            truly_new_gift_ids = current_gift_ids - known_gift_ids
            
            if truly_new_gift_ids:
                truly_new_gifts = {
                    gift_id: all_star_gifts_dict[gift_id]
                    for gift_id in truly_new_gift_ids
                }
                
                logger.info(f"Найдено {len(truly_new_gifts)} НОВЫХ подарков: [{', '.join(map(str, truly_new_gift_ids))}]")
                
                # Отправляем уведомления только о реально новых подарках
                if new_gift_callback:
                    for star_gift_id, star_gift in truly_new_gifts.items():
                        await new_gift_callback(star_gift)
                
                # Обновляем список известных подарков
                known_gift_ids.update(truly_new_gift_ids)
                save_known_gifts(known_gift_ids)
                
                # Сохраняем новые подарки в основное хранилище
                await star_gifts_data_saver(list(truly_new_gifts.values()))
            else:
                logger.debug(f"Новых подарков не найдено. Всего подарков: {len(current_gift_ids)}")

        # Обновляем существующие подарки
        if update_gifts_queue:
            for star_gift_id, old_star_gift in old_star_gifts_dict.items():
                new_star_gift = all_star_gifts_dict.get(star_gift_id)

                if new_star_gift is None:
                    logger.warning("Star gift not found in new gifts, skipping for updating", extra={"star_gift_id": str(star_gift_id)})
                    continue

                new_star_gift.message_id = old_star_gift.message_id

                if new_star_gift.available_amount < old_star_gift.available_amount:
                    update_gifts_queue.put_nowait((old_star_gift, new_star_gift))

        await asyncio.sleep(config.CHECK_INTERVAL)


def get_notify_text(star_gift: StarGiftData) -> str:
    is_limited = star_gift.is_limited

    available_percentage, available_percentage_is_same = (
        utils.pretty_float(
            math.ceil(star_gift.available_amount / star_gift.total_amount * 100 * 100) / 100,
            get_is_same = True
        )
        if is_limited else
        (
            NULL_STR,
            False
        )
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
            NULL_STR
        ),
        available_amount = (
            config.NOTIFY_TEXT_AVAILABLE_AMOUNT.format(
                available_amount = utils.pretty_int(star_gift.available_amount),
                same_str = (
                    NULL_STR
                    if available_percentage_is_same else
                    "~"
                ),
                available_percentage = available_percentage,
                updated_datetime = utils.get_current_datetime(timezone)
            )
            if is_limited else
            NULL_STR
        ),
        sold_out = (
            config.NOTIFY_TEXT_SOLD_OUT.format(
                sold_out = utils.format_seconds_to_human_readable(star_gift.last_sale_timestamp - star_gift.first_appearance_timestamp)
            )
            if star_gift.last_sale_timestamp and star_gift.first_appearance_timestamp else
            NULL_STR
        ),
        price = utils.pretty_int(star_gift.price),
        convert_price = utils.pretty_int(star_gift.convert_price)
    )


async def process_new_gift(app: Client, star_gift: StarGiftData) -> None:
    binary = typing.cast(BytesIO, await app.download_media(  # pyright: ignore[reportUnknownMemberType]
        message = star_gift.sticker_file_id,
        in_memory = True
    ))

    binary.name = star_gift.sticker_file_name

    sticker_message = typing.cast(types.Message, await app.send_sticker(  # pyright: ignore[reportUnknownMemberType]
        chat_id = config.NOTIFY_CHAT_ID,
        sticker = binary
    ))

    await asyncio.sleep(config.NOTIFY_AFTER_STICKER_DELAY)

    for chat_id in subscribers:
        binary.seek(0)  # чтобы отправка стикера не упала, тк BytesIO уже прочитан
        user_sticker_message = await app.send_sticker(
            chat_id = chat_id,
            sticker = binary
        )
    await asyncio.sleep(config.NOTIFY_AFTER_STICKER_DELAY)

    response = await bot_send_request(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": get_notify_text(star_gift),
                "reply_to_message_id": user_sticker_message.id
            } | BASIC_REQUEST_DATA
        )

    star_gift.message_id = response["message_id"]


async def process_update_gifts(update_gifts_queue: UPDATE_GIFTS_QUEUE_T) -> None:
    while True:
        new_star_gifts: list[StarGiftData] = []

        while True:
            try:
                _, new_star_gift = update_gifts_queue.get_nowait()

                new_star_gifts.append(new_star_gift)

                update_gifts_queue.task_done()

            except asyncio.QueueEmpty:
                break

        if not new_star_gifts:
            await asyncio.sleep(0.1)

            continue

        new_star_gifts.sort(
            key = lambda star_gift: star_gift.id
        )

        for new_star_gift in [
            min(
                gifts,
                key = lambda star_gift: star_gift.available_amount
            )
            for _, gifts in groupby(
                new_star_gifts,
                key = lambda star_gift: star_gift.id
            )
        ]:
            if new_star_gift.message_id is None:
                continue

            await bot_send_request(
                "editMessageText",
                {
                    "chat_id": config.NOTIFY_CHAT_ID,
                    "message_id": new_star_gift.message_id,
                    "text": get_notify_text(new_star_gift)
                } | BASIC_REQUEST_DATA
            )

            logger.debug(f"Star gift updated with {new_star_gift.available_amount} available amount", extra={"star_gift_id": str(new_star_gift.id)})

        await star_gifts_data_saver(new_star_gifts)


star_gifts_data_saver_lock = asyncio.Lock()

async def star_gifts_data_saver(star_gifts: StarGiftData | list[StarGiftData]) -> None:
    global STAR_GIFTS_DATA, last_star_gifts_data_saved_time

    async with star_gifts_data_saver_lock:
        if not isinstance(star_gifts, list):
            star_gifts = [star_gifts]

        updated_gifts_list = list(STAR_GIFTS_DATA.star_gifts)

        for star_gift in star_gifts:
            pos = bisect_left([
                gift.id
                for gift in updated_gifts_list
            ], star_gift.id)

            if pos < len(updated_gifts_list) and updated_gifts_list[pos].id == star_gift.id:
                updated_gifts_list[pos] = star_gift

            else:
                updated_gifts_list.insert(pos, star_gift)

        if last_star_gifts_data_saved_time is None or last_star_gifts_data_saved_time + config.DATA_SAVER_DELAY < utils.get_current_timestamp():
            STAR_GIFTS_DATA.save()

            last_star_gifts_data_saved_time = utils.get_current_timestamp()

            logger.debug("Saved star gifts data file")


async def star_gifts_upgrades_checker(app: Client) -> None:
    while True:
        for star_gift_id, star_gift in {
            star_gift.id: star_gift
            for star_gift in STAR_GIFTS_DATA.star_gifts
            if not star_gift.is_upgradable
        }.items():
            if await check_is_star_gift_upgradable(
                app = app,
                star_gift_id = star_gift_id
            ):
                logger.info(f"Star gift {star_gift_id} is upgradable")

                logger.debug(f"Sending upgrade notification for star gift {star_gift_id} (msg #{star_gift.message_id})")

                binary = typing.cast(BytesIO, await app.download_media(  # pyright: ignore[reportUnknownMemberType]
                    message = star_gift.sticker_file_id,
                    in_memory = True
                ))

                binary.name = star_gift.sticker_file_name

                sticker_message = typing.cast(types.Message, await app.send_sticker(  # pyright: ignore[reportUnknownMemberType]
                    chat_id = config.NOTIFY_UPGRADES_CHAT_ID,
                    sticker = binary
                ))

                await asyncio.sleep(config.NOTIFY_AFTER_STICKER_DELAY)

                await bot_send_request(
                    "sendMessage",
                    {
                        "chat_id": config.NOTIFY_UPGRADES_CHAT_ID,
                        "text": config.NOTIFY_UPGRADES_TEXT.format(
                            id = star_gift_id
                        ),
                        "reply_to_message_id": sticker_message.id
                    } | BASIC_REQUEST_DATA
                )

                star_gift.is_upgradable = True

                await star_gifts_data_saver(star_gift)

                await asyncio.sleep(config.NOTIFY_AFTER_TEXT_DELAY)

            else:
                logger.debug(f"Star gift {star_gift_id} is not upgradable")

        await asyncio.sleep(config.CHECK_UPGRADES_PER_CYCLE)


async def logger_wrapper(coro: typing.Awaitable[T]) -> T | None:
    try:
        return await coro
    except Exception as ex:
        logger.exception(f"""Error in {getattr(coro, "__name__", coro)}: {ex}""")


async def main() -> None:
    logger.info("Starting gifts detector...")

    app = Client(
        name = config.SESSION_NAME,
        api_id = config.API_ID,
        api_hash = config.API_HASH,
        sleep_threshold = 60
    )

    await app.start()

    # -------------------------

    async def start_handler(client: Client, message: types.Message):
        user_id = message.chat.id
        if user_id not in subscribers:
            subscribers.add(user_id)
            save_subscribers(subscribers)
        await message.reply_text("✅ Ты подписан на уведомления о новых подарках!")

    async def stop_handler(client: Client, message: types.Message):
        user_id = message.chat.id
        if user_id in subscribers:
            subscribers.remove(user_id)
            save_subscribers(subscribers)
        await message.reply_text("🚫 Ты отписан от уведомлений о новых подарках.")
    
    # Добавим handlers для /start и /stop
    app.add_handler(pyrogram.handlers.MessageHandler(start_handler, filters.command("start")))
    app.add_handler(pyrogram.handlers.MessageHandler(stop_handler, filters.command("stop")))

    # -------------------------

    update_gifts_queue = (
        UPDATE_GIFTS_QUEUE_T()
        if BOTS_AMOUNT > 0 else
        None
    )

    if update_gifts_queue:
        asyncio.create_task(logger_wrapper(
            process_update_gifts(
                update_gifts_queue = update_gifts_queue
            )
        ))

    else:
        logger.info("No bots available, skipping update gifts processing")

    if config.NOTIFY_UPGRADES_CHAT_ID:
        asyncio.create_task(logger_wrapper(
            star_gifts_upgrades_checker(app)
        ))

    else:
        logger.info("Upgrades channel is not set, skipping star gifts upgrades checking")

    await detector(
        app = app,
        new_gift_callback = partial(process_new_gift, app),
        update_gifts_queue = update_gifts_queue
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        STAR_GIFTS_DATA.save()
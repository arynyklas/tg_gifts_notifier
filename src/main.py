import argparse
import asyncio
from collections.abc import Callable, Coroutine
from logging import Logger
from pathlib import Path
import signal
import sys
import time
from typing import Any, cast
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from telethon import TelegramClient
import yaml

from src import constants, utils
from src.bot_api import BotApiClient
from src.config import Config
from src.detector import Detector
from src.logger import get_logger
from src.notifier import Notifier
from src.store import StarGiftsStore
from src.userbot import build_client, fetch_star_gifts, resolve_peer

_EXIT_OK = 0
_EXIT_CONFIG = 1
_EXIT_UNAUTHORIZED = 2
_HEALTHY_UPTIME = 60.0
_MAX_RESTART_DELAY = 60.0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.main", description=__doc__)

    parser.add_argument(
        "--config",
        type=Path,
        default=constants.DEFAULT_CONFIG_PATH,
        help="path to the YAML config file",
    )
    parser.add_argument(
        "--save-only",
        "-S",
        action="store_true",
        help="re-baseline the gift data file and exit, without notifying",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="interactively authorize the userbot session and exit",
    )

    return parser


async def supervise(
    name: str,
    factory: Callable[[], Coroutine[Any, Any, None]],
    logger: Logger,
) -> None:
    delay = 1.0

    while True:
        started = time.monotonic()

        try:
            await factory()

            logger.error("Task %s returned unexpectedly, restarting", name)

        except asyncio.CancelledError:
            logger.info("Task %s cancelled", name)

            raise

        except Exception:
            logger.exception("Task %s crashed, restarting in %.1fs", name, delay)

        delay = (
            1.0
            if time.monotonic() - started > _HEALTHY_UPTIME
            else min(delay * 2, _MAX_RESTART_DELAY)
        )

        await asyncio.sleep(delay)


def _install_signal_handlers(stop: asyncio.Event, logger: Logger) -> None:
    loop = asyncio.get_running_loop()

    def _handle(*_: object) -> None:
        logger.info("Shutdown signal received")
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle)

        except NotImplementedError:  # Windows
            signal.signal(sig, _handle)


async def _login(client: TelegramClient, logger: Logger) -> int:
    # `start()` returns a coroutine when awaited from async code; the stub says otherwise.
    await cast("Coroutine[Any, Any, None]", client.start())

    try:
        me = await client.get_me()

        logger.info(
            "Authorized as %s", getattr(me, "username", None) or getattr(me, "id", me)
        )

    finally:
        await client.disconnect()

    return _EXIT_OK


async def _save_only(
    client: TelegramClient,
    store: StarGiftsStore,
    cfg: Config,
    logger: Logger,
) -> int:
    if len(store):
        stamp = utils.get_current_datetime(
            ZoneInfo(cfg.notifications.timezone)
        ).replace(":", "-")
        dump_path = cfg.storage.data_filepath.with_name(f"star_gifts_dump_{stamp}.json")
        dump_store = StarGiftsStore(dump_path, cfg.storage.save_debounce, logger)

        await dump_store.replace_all(store.snapshot())
        await dump_store.flush()

        logger.info("Old gift data dumped to %s", dump_path)

    _, parsed = await fetch_star_gifts(client, 0, logger)

    if parsed is None:
        logger.error("Gift catalog came back unmodified for hash 0, nothing saved")

        return _EXIT_CONFIG

    await store.replace_all([gift.data for gift in parsed.values()])
    await store.flush()

    logger.info("Saved %d gifts to %s", len(parsed), cfg.storage.data_filepath)

    return _EXIT_OK


async def _detect(
    client: TelegramClient,
    store: StarGiftsStore,
    cfg: Config,
    logger: Logger,
) -> int:
    notify_peer = await resolve_peer(client, cfg.notifications.chat_id, logger)
    upgrades_peer = (
        await resolve_peer(client, cfg.notifications.upgrades_chat_id, logger)
        if cfg.notifications.upgrades_chat_id is not None
        else None
    )

    if upgrades_peer is None:
        logger.info("Upgrades chat is not set, skipping upgrade checks")

    bot_api = BotApiClient(cfg.telegram, logger)

    try:
        notifier = Notifier(
            client=client,
            bot_api=bot_api,
            store=store,
            cfg=cfg,
            notify_peer=notify_peer,
            upgrades_peer=upgrades_peer,
            logger=logger,
        )

        notifier.validate_texts()

        detector = Detector(
            client=client,
            notifier=notifier,
            store=store,
            cfg=cfg,
            logger=logger,
            track_updates=True,
            check_upgrades=upgrades_peer is not None,
        )

        stop = asyncio.Event()

        _install_signal_handlers(stop, logger)

        supervised: list[tuple[str, Callable[[], Coroutine[Any, Any, None]]]] = [
            ("saver", store.run_saver),
            ("poll", detector.poll_loop),
            ("updates", detector.updates_loop),
        ]

        if upgrades_peer is not None:
            supervised.append(("upgrades", detector.upgrades_loop))

        tasks = [
            asyncio.create_task(supervise(name, factory, logger), name=name)
            for name, factory in supervised
        ]

        logger.info("Started tasks: %s", [name for name, _ in supervised])

        await stop.wait()

        logger.info("Stopping tasks")

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        await bot_api.aclose()

    return _EXIT_OK


async def _run(args: argparse.Namespace, cfg: Config, logger: Logger) -> int:
    try:
        store, migrated = StarGiftsStore.load(
            cfg.storage.data_filepath,
            cfg.storage.save_debounce,
            logger,
        )

    except (OSError, ValueError) as ex:  # ValueError covers JSON and validation errors
        logger.critical("Cannot read gift data %s: %s", cfg.storage.data_filepath, ex)

        return _EXIT_CONFIG

    if migrated:
        logger.info(
            "Migrated %d gifts from schema v%d to v%d",
            len(store),
            store.loaded_version,
            constants.SCHEMA_VERSION,
        )

        store.mark_dirty()

    client = build_client(cfg.telegram, logger)

    await client.connect()

    try:
        if not await client.is_user_authorized():
            logger.critical(
                "Session is not authorized; run: python -m src.main --login"
            )

            return _EXIT_UNAUTHORIZED

        if args.save_only:
            return await _save_only(client, store, cfg, logger)

        return await _detect(client, store, cfg, logger)

    finally:
        await store.flush()
        await client.disconnect()


async def main() -> int:
    args = _build_parser().parse_args()

    try:
        cfg = Config.load(args.config)

    except FileNotFoundError:
        print(f"Config file not found: {args.config}", file=sys.stderr)

        return _EXIT_CONFIG

    except (OSError, yaml.YAMLError, ValidationError) as ex:
        print(f"Invalid config {args.config}: {ex}", file=sys.stderr)

        return _EXIT_CONFIG

    logger = get_logger(constants.LOGGER_NAME, cfg.logging)

    if args.login:
        return await _login(build_client(cfg.telegram, logger), logger)

    try:
        return await _run(args, cfg, logger)

    except RuntimeError:
        logger.critical("Startup failed", exc_info=True)

        return _EXIT_CONFIG

    except (KeyError, IndexError) as ex:
        logger.critical("Notification templates are invalid: %s", ex)

        return _EXIT_CONFIG


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

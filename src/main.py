import argparse
import asyncio
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from getpass import getpass
from logging import Logger
from pathlib import Path
import signal
import sys
import time
from typing import Any, cast
from zoneinfo import ZoneInfo

from pydantic import ValidationError
import qrcode
from telethon import TelegramClient, errors
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
_LOGIN_PHONE = "phone"
_LOGIN_QR = "qr"
_QR_MAX_ROUNDS = 6
_QR_BORDER = 2
# Matches the `max_attempts` that `TelegramClient.start()` gives the phone flow.
_PASSWORD_MAX_ATTEMPTS = 3


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
        nargs="?",
        const=_LOGIN_PHONE,
        choices=(_LOGIN_PHONE, _LOGIN_QR),
        default=None,
        help=(
            "authorize the userbot session and exit; "
            f"'{_LOGIN_PHONE}' (default) prompts for phone and code, "
            f"'{_LOGIN_QR}' prints a QR code to scan from an authorized device"
        ),
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


def _describe(user: object) -> object:
    return getattr(user, "username", None) or getattr(user, "id", user)


def _seconds_until(expires: datetime | None) -> float:
    if expires is None:
        return 0.0

    return max((expires - datetime.now(tz=UTC)).total_seconds(), 0.0)


def _print_qr(url: str) -> None:
    code = qrcode.QRCode(border=_QR_BORDER)

    code.add_data(url)
    # `invert=True` puts the light modules on the terminal background, which is
    # the polarity QR readers expect on a dark terminal.
    code.print_ascii(invert=True)


async def _login(client: TelegramClient, logger: Logger, mode: str) -> int:
    if mode == _LOGIN_QR:
        return await _login_qr(client, logger)

    return await _login_phone(client, logger)


async def _login_phone(client: TelegramClient, logger: Logger) -> int:
    # `start()` returns a coroutine when awaited from async code; the stub says otherwise.
    await cast("Coroutine[Any, Any, None]", client.start())

    try:
        logger.info("Authorized as %s", _describe(await client.get_me()))

    finally:
        await client.disconnect()

    return _EXIT_OK


async def _sign_in_with_password(client: TelegramClient, logger: Logger) -> object:
    """Prompt for the two-step password until it is accepted.

    `sign_in(password=...)` refetches the SRP parameters on every call, so a
    rejected attempt can simply be retried. Returns `None` once the attempts are
    spent, which keeps a typo from throwing the whole login away.
    """
    for attempt in range(1, _PASSWORD_MAX_ATTEMPTS + 1):
        password = getpass(
            f"Two-step verification password ({attempt}/{_PASSWORD_MAX_ATTEMPTS}): "
        )

        if not password:
            logger.error("Password was empty")

            continue

        try:
            return await client.sign_in(password=password)

        except errors.PasswordHashInvalidError:
            logger.error("Password rejected by Telegram")

    logger.critical(
        "Password was not accepted after %d attempts; reset it in Telegram "
        "(Settings -> Privacy and Security -> Two-Step Verification) if it is lost",
        _PASSWORD_MAX_ATTEMPTS,
    )

    return None


async def _login_qr(client: TelegramClient, logger: Logger) -> int:
    await client.connect()

    try:
        if await client.is_user_authorized():
            logger.info(
                "Session is already authorized as %s", _describe(await client.get_me())
            )

            return _EXIT_OK

        try:
            qr = await client.qr_login()

        except errors.SessionPasswordNeededError:
            # A previous run's code was already scanned and accepted: the session
            # only lacks the password, so asking for another scan cannot help.
            logger.info("A previously scanned code is still pending its password")

            user = await _sign_in_with_password(client, logger)

            if user is None:
                return _EXIT_CONFIG

            logger.info("Authorized as %s", _describe(user))

            return _EXIT_OK

        for round_index in range(1, _QR_MAX_ROUNDS + 1):
            _print_qr(qr.url)

            print(
                "Scan the code above in Telegram: Settings -> Devices -> Link Desktop Device.\n"
                f"Or open this link on an already authorized device: {qr.url}\n"
                f"Waiting for the scan; code {round_index}/{_QR_MAX_ROUNDS} "
                f"is valid for {_seconds_until(qr.expires):.0f}s..."
            )

            try:
                user = await qr.wait()

            except TimeoutError:
                logger.warning(
                    "QR code expired before it was scanned, generating a new one"
                )

                await qr.recreate()

                continue

            except errors.SessionPasswordNeededError:
                user = await _sign_in_with_password(client, logger)

                if user is None:
                    return _EXIT_CONFIG

            logger.info("Authorized as %s", _describe(user))

            return _EXIT_OK

        logger.error("QR login did not complete after %d codes", _QR_MAX_ROUNDS)

        return _EXIT_CONFIG

    finally:
        await client.disconnect()


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


async def _run_login(mode: str, cfg: Config, logger: Logger) -> int:
    client = build_client(
        cfg.telegram,
        logger,
        # QR login completes only when the `UpdateLoginToken` update arrives.
        receive_updates=mode == _LOGIN_QR,
    )

    try:
        return await _login(client, logger, mode)

    except errors.RPCError as ex:
        logger.critical("Login rejected by Telegram: %s", ex)

        return _EXIT_CONFIG

    except EOFError:  # stdin is not a terminal
        logger.critical("Login needs an interactive terminal")

        return _EXIT_CONFIG


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

    if args.login is not None:
        return await _run_login(args.login, cfg, logger)

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

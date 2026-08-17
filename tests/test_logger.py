import logging

from src import constants
from src.config import LoggingConfig
from src.logger import get_logger


def _fresh(name: str) -> None:
    for logger_name in list(logging.root.manager.loggerDict):
        if logger_name == name or logger_name.startswith(f"{name}."):
            logging.getLogger(logger_name).handlers.clear()
            logging.getLogger(logger_name).setLevel(logging.NOTSET)


def test_telethon_children_are_clamped_below_our_own_level(tmp_path):
    name = "tests_logger_clamp"

    _fresh(name)

    logger = get_logger(
        name,
        LoggingConfig.model_validate(
            {
                "console_level": "DEBUG",
                "file_level": "DEBUG",
                "telethon_level": "WARNING",
                "filepath": tmp_path / "main.log",
            }
        ),
    )

    # Telethon logs through `<name>.telethon.<module>` (see `build_client`).
    telethon_child = logging.getLogger(
        f"{name}.{constants.TELETHON_LOGGER_CHILD}.network.mtprotosender"
    )

    assert logger.isEnabledFor(logging.DEBUG) is True
    assert telethon_child.isEnabledFor(logging.DEBUG) is False
    assert telethon_child.isEnabledFor(logging.WARNING) is True

    # Handlers are still inherited, so warnings and errors reach the same files.
    assert telethon_child.getEffectiveLevel() == logging.WARNING
    assert len(logger.handlers) == 2


def test_telethon_level_can_be_opened_up(tmp_path):
    name = "tests_logger_debug"

    _fresh(name)

    get_logger(
        name,
        LoggingConfig.model_validate(
            {
                "console_level": "DEBUG",
                "file_level": "DEBUG",
                "telethon_level": "DEBUG",
                "filepath": tmp_path / "main.log",
            }
        ),
    )

    child = logging.getLogger(f"{name}.{constants.TELETHON_LOGGER_CHILD}.network")

    assert child.isEnabledFor(logging.DEBUG) is True


def test_repeated_calls_do_not_duplicate_handlers(tmp_path):
    name = "tests_logger_idempotent"

    _fresh(name)

    cfg = LoggingConfig.model_validate({"filepath": tmp_path / "main.log"})

    first = get_logger(name, cfg)
    second = get_logger(name, cfg)

    assert first is second
    assert len(first.handlers) == 2

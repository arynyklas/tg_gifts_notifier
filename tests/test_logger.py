from collections.abc import Iterator
import json
import logging
import sys

import pytest

from src import constants
from src.config import ConsoleLogConfig, FileLogConfig, LoggingConfig
from src.logger import JsonFormatter, get_logger

_NAMES = (
    "tests_logger_clamp",
    "tests_logger_debug",
    "tests_logger_idempotent",
)


@pytest.fixture(autouse=True)
def _fresh() -> Iterator[None]:
    """Close the file handlers these tests create, before and after each one.

    A `RotatingFileHandler` left open on a `tmp_path` file makes pytest's
    retention sweep fail to delete the directory on Windows.
    """
    _reset()

    yield

    _reset()


def _reset() -> None:
    for logger_name in list(logging.root.manager.loggerDict):
        if any(
            logger_name == name or logger_name.startswith(f"{name}.") for name in _NAMES
        ):
            instance = logging.getLogger(logger_name)

            for handler in instance.handlers:
                handler.close()

            instance.handlers.clear()
            instance.setLevel(logging.NOTSET)


def test_telethon_children_are_clamped_below_our_own_level(tmp_path):
    name = "tests_logger_clamp"

    logger = get_logger(
        name,
        LoggingConfig(
            telethon_level="WARNING",
            console=ConsoleLogConfig(level="DEBUG"),
            file=FileLogConfig(level="DEBUG", path=tmp_path / "main.log"),
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

    get_logger(
        name,
        LoggingConfig(
            telethon_level="DEBUG",
            console=ConsoleLogConfig(level="DEBUG"),
            file=FileLogConfig(level="DEBUG", path=tmp_path / "main.log"),
        ),
    )

    child = logging.getLogger(f"{name}.{constants.TELETHON_LOGGER_CHILD}.network")

    assert child.isEnabledFor(logging.DEBUG) is True


def test_repeated_calls_do_not_duplicate_handlers(tmp_path):
    name = "tests_logger_idempotent"

    cfg = LoggingConfig(file=FileLogConfig(path=tmp_path / "main.log"))

    first = get_logger(name, cfg)
    second = get_logger(name, cfg)

    assert first is second
    assert len(first.handlers) == 2


def test_json_formatter_emits_one_object_per_line():
    record = logging.LogRecord(
        "x.y", logging.WARNING, __file__, 1, "  hello %s  ", ("world",), None
    )
    record.gift_id = 42
    line = JsonFormatter().format(record)
    entry = json.loads(line)

    assert "\n" not in line
    assert entry["level"] == "WARNING"
    assert entry["logger"] == "x.y"
    assert entry["msg"] == "hello world"
    assert entry["gift_id"] == 42
    assert entry["ts"].endswith("Z")
    assert len(entry["ts"]) == len("2026-01-01T00:00:00.000Z")
    assert "exc" not in entry


def test_json_formatter_includes_traceback():
    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord(
            "x", logging.ERROR, __file__, 1, "failed", None, sys.exc_info()
        )

    entry = json.loads(JsonFormatter().format(record))

    assert "ValueError: boom" in entry["exc"]

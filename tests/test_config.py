import logging
from pathlib import Path

from pydantic import ValidationError
import pytest
import yaml

from src.config import Config

EXAMPLE_PATH = Path(__file__).resolve().parents[1] / "config.example.yaml"


def _example_dict() -> dict:
    return yaml.safe_load(EXAMPLE_PATH.read_text(encoding="utf-8"))


def _write(tmp_path: Path, raw: dict) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw, allow_unicode=True), encoding="utf-8")

    return path


def test_example_config_loads():
    cfg = Config.load(EXAMPLE_PATH)

    assert cfg.telegram.api_id == 1234
    assert cfg.notifications.chat_id == -1002452764624
    assert cfg.storage.data_filepath == Path("star_gifts.json")
    assert cfg.texts.notify.startswith("{title}")


def test_log_level_names_are_coerced_to_numbers():
    cfg = Config.load(EXAMPLE_PATH)

    assert cfg.logging.console_level == logging.DEBUG
    assert cfg.logging.file_level == logging.INFO


def test_unknown_top_level_key_is_rejected(tmp_path):
    raw = _example_dict()
    raw["unexpected"] = 1

    with pytest.raises(ValidationError):
        Config.load(_write(tmp_path, raw))


def test_empty_bot_tokens_is_rejected(tmp_path):
    raw = _example_dict()
    raw["telegram"]["bot_tokens"] = []

    with pytest.raises(ValidationError):
        Config.load(_write(tmp_path, raw))


def test_unknown_timezone_is_rejected(tmp_path):
    raw = _example_dict()
    raw["notifications"]["timezone"] = "Mars/Phobos"

    with pytest.raises(ValidationError):
        Config.load(_write(tmp_path, raw))

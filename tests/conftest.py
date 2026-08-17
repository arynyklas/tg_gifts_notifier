import logging
from pathlib import Path

import pytest
import yaml

from src.config import Config

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG_PATH = REPO_ROOT / "config.example.yaml"


@pytest.fixture
def logger() -> logging.Logger:
    instance = logging.getLogger("tests")
    instance.addHandler(logging.NullHandler())

    return instance


@pytest.fixture
def example_config_dict() -> dict:
    return yaml.safe_load(EXAMPLE_CONFIG_PATH.read_text(encoding="utf-8"))


@pytest.fixture
def config(example_config_dict: dict) -> Config:
    """Example config with every delay zeroed so tests never sleep."""
    raw = example_config_dict
    raw["notifications"]["after_sticker_delay"] = 0.0
    raw["notifications"]["after_text_delay"] = 0.0
    raw["detector"]["check_interval"] = 0.0
    raw["detector"]["updates_batch_delay"] = 0.0
    raw["detector"]["check_upgrades_interval"] = 0.0
    raw["storage"]["save_debounce"] = 0.0

    return Config.model_validate(raw)

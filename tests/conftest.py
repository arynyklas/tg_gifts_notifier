from collections.abc import Iterator
import logging
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from src.config import Config

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG_PATH = REPO_ROOT / "config.example.yaml"


@pytest.fixture
def logger() -> Iterator[logging.Logger]:
    """A logger of its own per test, discarded on teardown.

    A single shared `"tests"` logger accumulated one `NullHandler` per use for
    the whole session, and any level or handler a test set leaked into the next.
    """
    instance = logging.getLogger(f"tests.{uuid4().hex}")
    handler = logging.NullHandler()

    instance.addHandler(handler)
    instance.propagate = False

    yield instance

    instance.removeHandler(handler)
    handler.close()


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

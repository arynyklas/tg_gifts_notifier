from collections.abc import Callable, Iterator, Mapping
from dataclasses import replace
import logging
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from ruamel.yaml import YAML

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


@pytest.fixture(scope="session")
def example_config() -> Config:
    """Loaded once: `Config` is frozen, so tests can share it."""
    return Config.load(EXAMPLE_CONFIG_PATH)


@pytest.fixture
def config(example_config: Config) -> Config:
    """Example config with every delay zeroed so tests never sleep."""
    return replace(
        example_config,
        detector=replace(
            example_config.detector, check_interval=0.0, updates_batch_delay=0.0
        ),
        storage=replace(example_config.storage, save_debounce=0.0),
        notifications=replace(
            example_config.notifications, after_sticker_delay=0.0, after_text_delay=0.0
        ),
        upgrades=replace(example_config.upgrades, check_interval=0.0),
    )


@pytest.fixture
def config_file(tmp_path: Path) -> Callable[..., Path]:
    """Write config.example.yaml, edited, to `tmp_path / "config.yaml"`.

    `config_file({"upgrades.chat_id": None})` sets a dotted key (adding it if
    new); `config_file(delete=("upgrades",))` drops one.
    """

    def write(
        edits: Mapping[str, object] | None = None, *, delete: tuple[str, ...] = ()
    ) -> Path:
        yaml = YAML(typ="safe", pure=True)
        yaml.default_flow_style = False
        yaml.allow_unicode = True
        data = yaml.load(EXAMPLE_CONFIG_PATH.read_text(encoding="utf-8"))

        for dotted, value in (edits or {}).items():
            parent, key = _parent(data, dotted)
            parent[key] = value

        for dotted in delete:
            parent, key = _parent(data, dotted)
            del parent[key]

        path = tmp_path / "config.yaml"

        with path.open("w", encoding="utf-8") as stream:
            yaml.dump(data, stream)

        return path

    return write


def _parent(data: dict[str, Any], dotted: str) -> tuple[dict[str, Any], str]:
    *sections, key = dotted.split(".")

    for section in sections:
        data = data[section]

    return data, key

from pathlib import Path
import re

from dature.errors import DatureConfigError
import jsonschema
import pytest
from ruamel.yaml import YAML

from src.config import Config
from src.config_schema import (
    REGENERATE_COMMAND,
    build_schema,
    render_readme_table,
    render_schema_json,
    splice_readme,
)
from src.config_validators import SECURE_URL_PATTERN, is_secure_url

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_yaml(path: Path) -> object:
    return YAML(typ="safe", pure=True).load(path.read_text(encoding="utf-8"))


def _schema_accepts(data: object) -> bool:
    return jsonschema.Draft7Validator(build_schema(Config)).is_valid(data)


def _loader_accepts(path: Path) -> bool:
    try:
        Config.load(path)

    except DatureConfigError:
        return False

    return True


def test_config_schema_json_is_current():
    committed = (REPO_ROOT / "config.schema.json").read_text(encoding="utf-8")

    assert committed == render_schema_json(Config), (
        f"config.schema.json is stale; run: {REGENERATE_COMMAND}"
    )


def test_readme_key_table_is_current():
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert splice_readme(readme, render_readme_table(Config)) == readme, (
        f"the README key table is stale; run: {REGENERATE_COMMAND}"
    )


def test_the_schema_is_a_valid_draft_07_schema():
    jsonschema.Draft7Validator.check_schema(build_schema(Config))


def test_the_example_config_satisfies_the_schema():
    assert _schema_accepts(_load_yaml(REPO_ROOT / "config.example.yaml"))


@pytest.mark.parametrize(
    ("edits", "delete"),
    [
        pytest.param({"unexpected": 1}, (), id="unknown-top-level-key"),
        pytest.param({"logging.file.filepath": "x.log"}, (), id="unknown-nested-key"),
        pytest.param({}, ("texts.new_gift",), id="missing-required-key"),
        pytest.param({"bot_api.tokens": []}, (), id="no-bot-tokens"),
        pytest.param(
            {"bot_api.base_url": "http://api.telegram.org"}, (), id="plaintext-url"
        ),
        pytest.param({"upgrades.check_concurrency": 0}, (), id="zero-concurrency"),
        pytest.param({"logging.console.level": "debug"}, (), id="lower-case-level"),
        pytest.param({"logging.file.max_bytes": 1.5}, (), id="float-for-int"),
        pytest.param({"upgrades": None}, (), id="null-upgrades-section"),
        pytest.param({"detector.check_interval": 1}, (), id="int-for-float"),
        pytest.param({"notifications.silent": 1}, (), id="int-for-bool"),
        pytest.param({"upgrades.chat_id": None}, (), id="null-upgrades-chat"),
        pytest.param({}, ("upgrades",), id="omitted-upgrades"),
        pytest.param({}, ("detector",), id="omitted-defaulted-section"),
    ],
)
def test_the_schema_accepts_exactly_what_the_loader_accepts(config_file, edits, delete):
    """Timezones and template placeholders are load-time checks only."""
    path = config_file(edits, delete=delete)

    assert _schema_accepts(_load_yaml(path)) is _loader_accepts(path)


@pytest.mark.parametrize(
    "url",
    [
        "https://api.telegram.org",
        "https://bot.example.com:8443",
        "http://localhost:8081",
        "http://127.0.0.1:8081",
        "http://[::1]:8081",
        "http://localhost/bot",
    ],
)
def test_the_documented_base_urls_pass_the_editor_check(url):
    assert re.search(SECURE_URL_PATTERN, url)


@pytest.mark.parametrize(
    "url",
    [
        "http://api.telegram.org",
        "http://192.168.1.10:8081",
        "http://localhost.evil.example",
        "http://127.0.0.1@evil.example",
        "http://localhost:8081@evil.example",
        "http://[::1",
        "HTTP://api.telegram.org",
        "api.telegram.org",
        "ftp://api.telegram.org",
        "https://",
    ],
)
def test_the_editor_check_never_passes_a_url_the_loader_rejects(url):
    """A plaintext Bot API URL puts the token on the wire."""
    assert is_secure_url(url) or not re.search(SECURE_URL_PATTERN, url)


def test_the_example_config_names_its_schema():
    path = REPO_ROOT / "config.example.yaml"
    first = path.read_text(encoding="utf-8").splitlines()[0]

    assert first == "# yaml-language-server: $schema=./config.schema.json"

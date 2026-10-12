from pathlib import Path

from dature.errors import DatureConfigError, StrictModeError
import pytest

from src.config import Config, is_legacy_layout

EXAMPLE_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.example.yaml"


def _errors(path: Path) -> list[str]:
    """The `[key]  message` line of every error the load reports."""
    with pytest.raises(DatureConfigError) as excinfo:
        Config.load(path)

    return [str(error).splitlines()[0].strip() for error in excinfo.value.exceptions]


def test_example_config_loads():
    cfg = Config.load(EXAMPLE_CONFIG_PATH)

    assert cfg.userbot.api_id == 1234
    assert cfg.bot_api.tokens == ("1234:abcd", "2345:bcda", "3456:cdef")
    assert cfg.notifications.chat_id == -1002452764624
    assert cfg.upgrades.chat_id == -1002751596218
    assert cfg.storage.data_filepath == Path("star_gifts.json")
    assert cfg.logging.console.level == "DEBUG"
    assert cfg.texts.new_gift.startswith("{title}")
    assert cfg.texts.requirements.emoji == "✨"


@pytest.mark.parametrize(
    ("edits", "delete"),
    [
        pytest.param({}, ("upgrades",), id="section-omitted"),
        pytest.param({"upgrades.chat_id": None}, (), id="chat-id-null"),
    ],
)
def test_upgrades_are_off_without_a_chat(config_file, edits, delete):
    upgrades = Config.load(config_file(edits, delete=delete)).upgrades

    assert upgrades.chat_id is None
    assert upgrades.check_concurrency == 4
    assert upgrades.text == "Gift is upgradable! (<code>{id}</code>)"


def test_integers_are_accepted_for_float_fields(config_file):
    """JSON Schema's `number` accepts `1`, so the loader has to as well."""
    cfg = Config.load(config_file({"detector.check_interval": 1}))

    assert cfg.detector.check_interval == 1.0
    assert isinstance(cfg.detector.check_interval, float)


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("bot_api.tokens", [], "Value length must be greater than or equal to 1"),
        ("bot_api.base_url", "http://api.telegram.org", "must use https"),
        ("bot_api.base_url", "http://192.168.1.10:8081", "must use https"),
        ("bot_api.base_url", "api.telegram.org", "must use https"),
        ("bot_api.base_url", "ftp://api.telegram.org", "must use https"),
        ("bot_api.base_url", "http://[::1", "must use https"),
        ("notifications.timezone", "Mars/Phobos", "unknown timezone"),
        ("logging.console.level", "debug", "Invalid variant"),
        ("upgrades.check_concurrency", 0, "greater than or equal to 1"),
        ("detector.check_interval", True, "expected a number, got bool"),
        ("upgrades", None, "Expected Mapping"),
        # dature 1.6 drops the key for a float or bool in an `int | None` field;
        # `_optional_int` restores it.
        ("upgrades.chat_id", 1.5, "expected an integer or null, got float"),
        ("upgrades.chat_id", True, "expected an integer or null, got bool"),
    ],
)
def test_invalid_values_are_reported_with_their_key(config_file, key, value, message):
    (error,) = _errors(config_file({key: value}))

    assert error.startswith(f"[{key}]")
    assert message in error


@pytest.mark.parametrize(
    "url",
    [
        "https://api.telegram.org",
        "https://bot.example.com:8443",
        "http://localhost:8081",
        "http://127.0.0.1:8081",
        "http://[::1]:8081",
    ],
)
def test_accepted_bot_api_base_urls(config_file, url):
    assert Config.load(config_file({"bot_api.base_url": url})).bot_api.base_url == url


@pytest.mark.parametrize("key", ["unexpected", "logging.file.filepath"])
def test_unknown_keys_are_rejected(config_file, key):
    path = config_file({key: 1})

    with pytest.raises(StrictModeError) as excinfo:
        Config.load(path)

    (error,) = excinfo.value.exceptions

    assert str(error).strip().startswith(f"[{key}]")


def test_a_missing_required_key_is_reported(config_file):
    assert _errors(config_file(delete=("notifications.chat_id",))) == [
        "[notifications.chat_id]  Missing required field"
    ]


def test_template_text_is_kept_verbatim(config_file):
    """Env expansion would rewrite `$NAME` and `%NAME%`, and `$$` to `$`."""
    text = "Costs $HOME %PATH% $$5 100%"

    cfg = Config.load(config_file({"texts.title.regular": text}))

    assert cfg.texts.title.regular == text


@pytest.mark.parametrize(
    ("key", "value"),
    [
        pytest.param("userbot.api_hsh", "deadbeef-secret", id="unknown-key"),
        # dature does not mask the message of a custom type loader.
        pytest.param("upgrades.chat_id", "-100-deadbeef-secret", id="custom-loader"),
    ],
)
def test_a_rejected_value_is_never_echoed(config_file, key, value):
    """A typo in a secret-bearing key would otherwise print the secret."""
    path = config_file({key: value})

    with pytest.raises(DatureConfigError) as excinfo:
        Config.load(path)

    rendered = "\n".join(str(error) for error in excinfo.value.exceptions)

    assert f"[{key}]" in rendered
    assert "deadbeef-secret" not in rendered


def test_no_config_is_searched_for_outside_the_given_path(
    config_file, tmp_path, monkeypatch
):
    """dature looks in %APPDATA%, ~/.config and /etc unless told not to."""
    elsewhere = config_file()
    workdir = tmp_path / "work"
    workdir.mkdir()

    for variable in ("APPDATA", "XDG_CONFIG_HOME"):
        monkeypatch.setenv(variable, str(elsewhere.parent))

    monkeypatch.chdir(workdir)

    with pytest.raises(DatureConfigError) as excinfo:
        Config.load(Path(elsewhere.name))

    assert [type(error) for error in excinfo.value.exceptions] == [FileNotFoundError]


@pytest.mark.parametrize(
    ("key", "template"),
    [
        ("texts.new_gift", "{title} {nonexistent}"),
        ("texts.new_gift", "{title} {}"),
        ("texts.new_gift", "{title:d}"),
        ("texts.sold_out", "sold out in {sold_out"),
        ("texts.requirements.user_limited", "{count} per user"),
        ("upgrades.text", "Upgradable: {gift}"),
    ],
)
def test_broken_templates_are_rejected_at_load(config_file, key, template):
    (error,) = _errors(config_file({key: template}))

    assert error.startswith(f"[{key}]")
    assert "must be a str.format template using only" in error


def test_format_specs_matching_the_placeholder_type_are_accepted(config_file):
    """`{number}` and `{id}` are ints when the notifier renders them."""
    template = "{title} {number:,} {id:>8}"

    cfg = Config.load(config_file({"texts.new_gift": template}))

    assert cfg.texts.new_gift == template


def test_the_error_names_the_allowed_placeholders(config_file):
    (error,) = _errors(config_file({"texts.sold_out": "{when}"}))

    assert error.endswith("using only {sold_out}")


def test_a_2x_config_is_recognized(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("telegram:\n  api_id: 1\n", encoding="utf-8")

    assert is_legacy_layout(path) is True


@pytest.mark.parametrize(
    "content",
    [
        None,
        "userbot:\n  api_id: 1\n",
        "- telegram\n",
        "telegram: [\n",
        b"\xff\xfe",
        "x: 2024-02-30\n",
    ],
    ids=["missing", "3.x", "list", "broken-yaml", "not-utf-8", "bad-date"],
)
def test_anything_else_is_not_taken_for_a_2x_config(tmp_path, content):
    """The check runs while reporting a load error, so it must never raise."""
    path = tmp_path / "config.yaml"

    if isinstance(content, bytes):
        path.write_bytes(content)

    elif content is not None:
        path.write_text(content, encoding="utf-8")

    assert is_legacy_layout(path) is False

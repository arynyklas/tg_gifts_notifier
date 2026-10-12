"""Configuration, loaded from YAML by dature.

Every field docstring is user documentation: `scripts/generate_config_schema.py`
turns it into the editor hover text in `config.schema.json` and into the README
key table, and fails on a field without one.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal, Self, cast

import dature
from dature import V
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from src import constants
from src.config_validators import SecureUrl, Template, Timezone

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


# Every 2.x config has this top-level section; the 3.x layout has none.
_LEGACY_SECTION = "telegram"


def _float_from_number(value: object) -> float:
    """Accept YAML integers for float fields, as JSON Schema's `number` does.

    dature alone rejects `check_interval: 1`, which the editor schema cannot
    tell apart from `1.0`.
    """
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"expected a number, got {type(value).__name__}")

    return float(value)


def _optional_int(value: object) -> int | None:
    """Load an `int | None` field without dature 1.6's broken error path.

    dature reports a float or a bool there as "'NoneType' object has no
    attribute '__name__'", without the key. The message never embeds the
    value: dature masks values only in its own errors.
    """
    if value is None or (isinstance(value, int) and not isinstance(value, bool)):
        return value

    raise TypeError(f"expected an integer or null, got {type(value).__name__}")


@dataclass(frozen=True, slots=True, kw_only=True)
class UserbotConfig:
    """MTProto account that polls the gift catalog and posts the stickers."""

    api_id: int
    """Telegram API ID from my.telegram.org."""
    api_hash: str
    """Telegram API hash matching `api_id`."""
    session_path: Path = Path("sessions/userbot.session")
    """Telethon session file."""
    flood_sleep_threshold: int = 60
    """Flood waits shorter than this many seconds are slept through by Telethon."""


@dataclass(frozen=True, slots=True, kw_only=True)
class BotApiConfig:
    """Bots that send and edit the notification texts."""

    tokens: Annotated[tuple[str, ...], V.len() >= 1]
    """Bot tokens from @BotFather; requests rotate through them. At least one."""
    base_url: Annotated[str, SecureUrl()] = "https://api.telegram.org"
    """Bot API server: `https`, or `http` only for a local server (`localhost`, `127.0.0.1`, `[::1]`)."""
    request_timeout: float = 20.0
    """Bot API request timeout, in seconds."""


@dataclass(frozen=True, slots=True, kw_only=True)
class DetectorConfig:
    """Polling cadence of the gift catalog."""

    check_interval: float = 1.0
    """Delay between gift catalog polls, in seconds."""
    updates_batch_delay: float = 1.0
    """Delay between batches of stock-update edits, in seconds."""
    announce_retry_max_delay: float = 60.0
    """Upper bound of the backoff for retrying a failed announcement, in seconds."""


@dataclass(frozen=True, slots=True, kw_only=True)
class StorageConfig:
    """Gift data file."""

    data_filepath: Path = Path("star_gifts.json")
    """Path of the gift data file."""
    save_debounce: float = 2.0
    """Debounce before the data file is rewritten, in seconds."""


@dataclass(frozen=True, slots=True, kw_only=True)
class NotificationsConfig:
    """New gift posts; the delays pace upgrade posts too."""

    chat_id: int
    """Chat where new gift messages are posted."""
    after_sticker_delay: float = 1.0
    """Delay after posting a sticker, before its text, in seconds. Paces upgrade posts too."""
    after_text_delay: float = 2.0
    """Delay after posting a text, in seconds. Paces upgrade posts too."""
    timezone: Annotated[str, Timezone()] = "UTC"
    """IANA timezone of the rendered timestamps and of `--save-only` dump names (e.g. `Europe/Moscow`)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class UpgradesConfig:
    """Upgrade posts. Omit the section, or set `chat_id` to `null`, to disable upgrade checks."""

    chat_id: int | None = None
    """Chat where upgrade messages are posted; `null` disables upgrade checks."""
    check_interval: float = 2.0
    """Delay between upgradability check cycles, in seconds."""
    check_concurrency: Annotated[int, V >= 1] = 4
    """Concurrent upgradability probes; `--save-only` and a data file migration use it even with upgrades off."""
    text: Annotated[str, Template.of(id=0)] = "Gift is upgradable! (<code>{id}</code>)"
    """Upgrade message (HTML)."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ConsoleLogConfig:
    """Console log output."""

    level: LogLevel = "INFO"
    """Console log level."""


@dataclass(frozen=True, slots=True, kw_only=True)
class FileLogConfig:
    """Rotating log file."""

    level: LogLevel = "INFO"
    """File log level."""
    path: Path = Path("logs/main.log")
    """Log file path."""
    max_bytes: int = 10 * 1024 * 1024
    """Size at which the log file is rotated, in bytes."""
    backup_count: int = 5
    """Number of rotated log files kept."""


@dataclass(frozen=True, slots=True, kw_only=True)
class LoggingConfig:
    """Log levels and destinations."""

    # Telethon is chatty at DEBUG; keep it out of our own DEBUG stream by default.
    telethon_level: LogLevel = "WARNING"
    """Level of Telethon's own loggers, kept apart from ours; `DEBUG` to debug MTProto."""
    console: ConsoleLogConfig = field(default_factory=ConsoleLogConfig)
    """Console log output."""
    file: FileLogConfig = field(default_factory=FileLogConfig)
    """Rotating log file."""


@dataclass(frozen=True, slots=True, kw_only=True)
class TitleTexts:
    """Title line of a new gift message."""

    limited: str
    """Inserted as `{title}` for a limited gift."""
    regular: str
    """Inserted as `{title}` for an unlimited gift."""


@dataclass(frozen=True, slots=True, kw_only=True)
class RequirementTexts:
    """Line inserted as `{require_premium_or_user_limited}` when a gift needs Premium or is limited per user."""

    template: Annotated[
        str,
        Template.of(emoji="", require_premium="", separator="", user_limited=""),
    ]
    """Layout of the line."""
    emoji: str
    """Inserted as `{emoji}`."""
    premium: str
    """Inserted as `{require_premium}` when the gift needs Telegram Premium."""
    user_limited: Annotated[str, Template.of(user_limited="")]
    """Inserted as `{user_limited}` when the gift is limited per user."""
    separator: str
    """Inserted as `{separator}` when both parts are present."""


@dataclass(frozen=True, slots=True, kw_only=True)
class TextsConfig:
    """New gift message templates (HTML)."""

    new_gift: Annotated[
        str,
        Template.of(
            title="",
            number=0,
            id=0,
            total_amount="",
            available_amount="",
            sold_out="",
            price="",
            convert_price="",
            require_premium_or_user_limited="",
        ),
    ]
    """New gift message."""
    title: TitleTexts
    """Title line, inserted as `{title}`."""
    total_amount: Annotated[str, Template.of(total_amount="")]
    """Inserted as `{total_amount}` for a limited gift."""
    available_amount: Annotated[
        str,
        Template.of(
            available_amount="",
            same_str="",
            available_percentage="",
            updated_datetime="",
        ),
    ]
    """Inserted as `{available_amount}` for a limited gift."""
    sold_out: Annotated[str, Template.of(sold_out="")]
    """Inserted as `{sold_out}` once a gift is sold out."""
    requirements: RequirementTexts
    """Premium and per-user limit line, inserted as `{require_premium_or_user_limited}`."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    """TG Gifts Notifier configuration."""

    userbot: UserbotConfig
    """MTProto account that polls the gift catalog and posts the stickers."""
    bot_api: BotApiConfig
    """Bots that send and edit the notification texts."""
    detector: DetectorConfig = field(default_factory=DetectorConfig)
    """Polling cadence of the gift catalog."""
    storage: StorageConfig = field(default_factory=StorageConfig)
    """Gift data file."""
    notifications: NotificationsConfig
    """New gift posts."""
    upgrades: UpgradesConfig = field(default_factory=UpgradesConfig)
    """Upgrade posts; disabled unless `chat_id` is set."""
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    """Log levels and destinations."""
    texts: TextsConfig
    """New gift message templates (HTML)."""

    @classmethod
    def load(cls, path: Path) -> Self:
        """Load and validate `path`; raises `dature.errors.DatureConfigError`."""
        return dature.load(
            # No fallback to a config.yaml in %APPDATA% or ~/.config.
            dature.Yaml12Source(file=path, encoding=constants.ENCODING, config_dirs=()),
            schema=cls,
            strict="error",
            # Templates contain `%` and may contain `$`: nothing is an env reference.
            expand_env_vars="disabled",
            # Errors never echo a value: a typo in a secret key would print it.
            masking_mode="all",
            # dature types `type_loaders` keys as `type`; adaptix takes any type hint.
            type_loaders={
                float: _float_from_number,
                cast("type", int | None): _optional_int,
            },
        )


def is_legacy_layout(path: Path) -> bool:
    """Whether `path` holds a 2.x config, recognized by its `telegram:` section."""
    try:
        data = YAML(typ="safe", pure=True).load(
            path.read_text(encoding=constants.ENCODING)
        )

    except (OSError, ValueError, YAMLError):
        return False

    return isinstance(data, dict) and _LEGACY_SECTION in data

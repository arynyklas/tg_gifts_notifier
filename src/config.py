import logging
from pathlib import Path
from typing import Annotated, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
)
import yaml

from src import constants

_LEVELS = logging.getLevelNamesMapping()


def _parse_level(value: object) -> object:
    if isinstance(value, str):
        try:
            return _LEVELS[value.strip().upper()]

        except KeyError as ex:
            raise ValueError(f"unknown log level {value!r}") from ex

    return value


LogLevel = Annotated[int, BeforeValidator(_parse_level)]


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TelegramConfig(_Base):
    api_id: int
    api_hash: str
    session_path: Path = Path("sessions/userbot.session")
    flood_sleep_threshold: int = 60
    bot_tokens: list[str] = Field(min_length=1)
    bot_api_base_url: str = "https://api.telegram.org"
    request_timeout: float = 20.0


class DetectorConfig(_Base):
    check_interval: float = 1.0
    updates_batch_delay: float = 1.0
    check_upgrades_interval: float = 2.0
    upgrades_check_concurrency: int = Field(default=4, ge=1)
    announce_retry_max_delay: float = 60.0


class StorageConfig(_Base):
    data_filepath: Path = Path("star_gifts.json")
    save_debounce: float = 2.0


class NotificationsConfig(_Base):
    chat_id: int
    upgrades_chat_id: int | None = None
    after_sticker_delay: float = 1.0
    after_text_delay: float = 2.0
    timezone: str = "UTC"

    @field_validator("timezone")
    @classmethod
    def _check_tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)

        except (ZoneInfoNotFoundError, ValueError) as ex:
            raise ValueError(f"unknown timezone {value!r}") from ex

        return value


class LoggingConfig(_Base):
    console_level: LogLevel = logging.INFO
    file_level: LogLevel = logging.INFO
    # Telethon is chatty at DEBUG; keep it out of our own DEBUG stream by default.
    telethon_level: LogLevel = logging.WARNING
    filepath: Path = Path("logs/main.log")
    max_bytes: int = 10 * 1024 * 1024
    backup_count: int = 5


class TextsConfig(_Base):
    notify: str
    title_limited: str
    title_regular: str
    total_amount: str
    available_amount: str
    sold_out: str
    require_premium_or_user_limited: str
    require_premium_or_user_limited_emoji: str
    require_premium: str
    user_limited: str
    require_premium_and_user_limited_separator: str
    upgrades: str


class Config(_Base):
    telegram: TelegramConfig
    detector: DetectorConfig = Field(default_factory=DetectorConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    notifications: NotificationsConfig
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    texts: TextsConfig

    @classmethod
    def load(cls, path: Path) -> Self:
        with path.open("r", encoding=constants.ENCODING) as file:
            return cls.model_validate(yaml.safe_load(file))

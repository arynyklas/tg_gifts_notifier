from pathlib import Path

ENCODING = "utf-8"
NULL_STR = ""
LOGGER_NAME = "gifts_notifier"
TELETHON_LOGGER_CHILD = "telethon"
# Fallback extension when a gift's sticker document carries no filename.
STICKER_SUFFIX = ".tgs"
SCHEMA_VERSION = 2
# A gift whose payload advertises no upgrade price is re-probed at most once an
# hour, so a payload that lags reality costs an hour of delay, not a permanent
# miss.
NEGATIVE_PROBE_COOLDOWN = 3600.0
DEFAULT_CONFIG_PATH = Path("config.yaml")

from pathlib import Path

ENCODING = "utf-8"
NULL_STR = ""
LOGGER_NAME = "gifts_notifier"
TELETHON_LOGGER_CHILD = "telethon"
# Fallback extension when a gift's sticker document carries no filename.
STICKER_SUFFIX = ".tgs"
SCHEMA_VERSION = 2
# A gift whose payload advertises no upgrade price is re-probed at most this
# often, so a payload that lags reality costs a delay rather than a permanent
# miss. Measured live: `upgrade_stars` is None for 2 of the 118 gifts that
# `getStarGiftUpgradePreview` accepts, so the field is not a reliable positive
# signal and the cooldown is the only thing that finds those. 31 such gifts at
# 300s is 0.10 RPC/s, against ~15 RPC/s when every cycle probed all of them.
NEGATIVE_PROBE_COOLDOWN = 300.0
DEFAULT_CONFIG_PATH = Path("config.yaml")

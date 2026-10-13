from datetime import datetime
from decimal import Decimal
import time
from zoneinfo import ZoneInfo

_SECONDS_PER_DAY = 24 * 3600
_SECONDS_PER_HOUR = 3600
_SECONDS_PER_MINUTE = 60


def get_current_datetime(timezone: ZoneInfo) -> str:
    return datetime.now(tz=timezone).strftime("%d-%m-%Y %H:%M:%S")


def current_timestamp() -> int:
    return int(time.time())


def pretty_int(number: int) -> str:
    return f"{number:,}"


def pretty_float(number: float) -> tuple[str, bool]:
    """Round to one significant digit, rendered without exponent.

    Returns the rendered value and whether it still equals the input.
    """
    rounded = float(f"{float(number):.1g}")
    text = format(Decimal(repr(rounded)), "f").rstrip("0").rstrip(".") or "0"

    return text, rounded == number


def format_seconds_to_human_readable(total_seconds: int) -> str:
    total_seconds = max(int(total_seconds), 0)

    days = total_seconds // _SECONDS_PER_DAY
    total_seconds %= _SECONDS_PER_DAY
    hours = total_seconds // _SECONDS_PER_HOUR
    total_seconds %= _SECONDS_PER_HOUR
    minutes = total_seconds // _SECONDS_PER_MINUTE
    seconds = total_seconds % _SECONDS_PER_MINUTE

    parts: list[str] = []

    if days > 0:
        parts.append(f"{days} day" + ("s" if days != 1 else ""))

    if hours > 0:
        parts.append(f"{hours} hour" + ("s" if hours != 1 else ""))

    if minutes > 0:
        parts.append(f"{minutes} minute" + ("s" if minutes != 1 else ""))

    if seconds > 0 or not parts:
        parts.append(f"{seconds} second" + ("s" if seconds != 1 else ""))

    if len(parts) == 1:
        return parts[0]

    if len(parts) == 2:
        return f"{parts[0]} and {parts[1]}"

    return ", ".join(parts[:-1]) + f" and {parts[-1]}"

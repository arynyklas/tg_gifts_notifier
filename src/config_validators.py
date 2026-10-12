"""dature predicates for the config checks that `V` cannot express."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dature.validators.predicate import Predicate

# A plaintext request line would carry the bot token; only a Bot API server on
# this machine is allowed to be reached without TLS. `urlsplit` strips the
# brackets of an IPv6 host, so `::1` is the form seen here.
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def is_secure_url(value: str) -> bool:
    try:
        split = urlsplit(value)

    except ValueError:  # e.g. an unclosed IPv6 bracket
        return False

    return split.scheme == "https" or (
        split.scheme == "http" and split.hostname in _LOCAL_HOSTS
    )


def _is_timezone(value: str) -> bool:
    try:
        ZoneInfo(value)

    except (ZoneInfoNotFoundError, ValueError):
        return False

    return True


@dataclass(frozen=True, slots=True)
class SecureUrl(Predicate):
    """`https`, or `http` only to a Bot API server on this machine."""

    def check_type(self, field_type: Any, *, field_path: list[str]) -> None:  # noqa: ANN401
        return

    def get_validator_func(self) -> Callable[[Any], bool]:
        return is_secure_url

    def get_error_message(self) -> str:
        return "must use https, or http only for a local server"


@dataclass(frozen=True, slots=True)
class Timezone(Predicate):
    """An IANA timezone name that `zoneinfo` can load."""

    def check_type(self, field_type: Any, *, field_path: list[str]) -> None:  # noqa: ANN401
        return

    def get_validator_func(self) -> Callable[[Any], bool]:
        return _is_timezone

    def get_error_message(self) -> str:
        return "unknown timezone"

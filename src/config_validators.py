"""dature predicates for the config checks that `V` cannot express.

`src.config_schema` maps every predicate used in `src.config` to JSON Schema,
so a new predicate class has to be taught there as well.
"""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Self
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dature.validators.predicate import Predicate

# A plaintext request line would carry the bot token; only a Bot API server on
# this machine is allowed to be reached without TLS. `urlsplit` strips the
# brackets of an IPv6 host, so `::1` is the form seen here.
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

# The editor-side form of `is_secure_url`, for `config.schema.json`. It may
# reject more (an upper-case scheme, userinfo) but never accepts an `http` URL
# that `is_secure_url` rejects; `tests/test_config_schema.py` pins that. It can
# accept a malformed `https` one (`https://[::1`), which is harmless: plaintext
# is what matters.
SECURE_URL_PATTERN = (
    r"^(https://|http://(localhost|127\.0\.0\.1|\[::1\])(:[0-9]+)?(/|$))"
)


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


@dataclass(frozen=True, slots=True)
class Template(Predicate):
    """A `str.format` template restricted to the given placeholders.

    The template is rendered once with one sample per placeholder, typed as the
    notifier passes it, so an unknown or positional field, a stray brace or a
    bad format spec fails at load instead of at the first announcement.
    """

    samples: tuple[tuple[str, object], ...]

    @classmethod
    def of(cls, **samples: object) -> Self:
        return cls(tuple(samples.items()))

    @property
    def placeholders(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.samples)

    def check_type(self, field_type: Any, *, field_path: list[str]) -> None:  # noqa: ANN401
        return

    def get_validator_func(self) -> Callable[[Any], bool]:
        samples = dict(self.samples)

        def renders(value: str) -> bool:
            try:
                value.format(**samples)

            except (KeyError, IndexError, ValueError, AttributeError, TypeError):
                return False

            return True

        return renders

    def get_error_message(self) -> str:
        names = ", ".join(f"{{{name}}}" for name in self.placeholders)

        return f"must be a str.format template using only {names}"

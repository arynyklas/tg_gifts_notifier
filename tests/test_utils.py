import pytest

from src.utils import (
    format_seconds_to_human_readable,
    pretty_float,
    pretty_int,
)


@pytest.mark.parametrize(
    ("number", "expected"),
    [
        (0.09, ("0.09", True)),
        (5.0, ("5", True)),
        (100.0, ("100", True)),
        (12.34, ("10", False)),
        (0.005, ("0.005", True)),
        (1e-05, ("0.00001", True)),
    ],
)
def test_pretty_float_matches_numpy_era_output(number, expected):
    assert pretty_float(number) == expected


@pytest.mark.parametrize(
    ("total_seconds", "expected"),
    [
        (0, "0 seconds"),
        (-5, "0 seconds"),
        (59, "59 seconds"),
        (60, "1 minute"),
        (3600, "1 hour"),
        (90061, "1 day, 1 hour, 1 minute and 1 second"),
    ],
)
def test_format_seconds_to_human_readable(total_seconds, expected):
    assert format_seconds_to_human_readable(total_seconds) == expected


def test_pretty_int_groups_thousands():
    assert pretty_int(1234567) == "1,234,567"

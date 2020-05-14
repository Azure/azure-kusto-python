"""Conversion utils"""

import re
from datetime import timedelta

from dateutil import parser

# Regex for TimeSpan
_TIMESPAN_PATTERN = re.compile(r"(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)")
_MICROSECONDS_PATTERN = re.compile(r"(.*\.[0-9]{0,6})[0-9]*([A-Z]*)")


def to_datetime(value):
    """Converts a string to a datetime."""
    if isinstance(value, int):
        return parser.parse(value)
    # Until v2.8.0 dateutil does not support more than 6 digits for fractional seconds
    try:
        return parser.isoparse(value)
    except ValueError:
        match = _MICROSECONDS_PATTERN.match(value)
        if match:
            value = match.group(1) + match.group(2)
        return parser.isoparse(value)


def to_timedelta(value):
    """Converts a string to a timedelta."""
    if isinstance(value, (int, float)):
        return timedelta(microseconds=(float(value) / 10))
    match = _TIMESPAN_PATTERN.match(value)
    if match:
        if match.group(1) == "-":
            factor = -1
        else:
            factor = 1
        return factor * timedelta(days=int(match.group("d") or 0), hours=int(match.group("h")), minutes=int(match.group("m")), seconds=float(match.group("s")))
    else:
        raise ValueError("Timespan value '{}' cannot be decoded".format(value))

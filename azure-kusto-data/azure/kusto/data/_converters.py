"""Convertion utils"""

from datetime import timedelta
import re
from dateutil import parser
from dateutil.tz import UTC
import six

# Regex for TimeSpan
_TIMESPAN_PATTERN = re.compile(r"(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)")


def to_high_precision_type(kusto_type, raw_value, typed_value):
    import pandas as pd
    if kusto_type == "datetime":    
        return pd.to_datetime(raw_value)
    
    if kusto_type == "timespan":
        if isinstance(raw_value, (six.integer_types, float)):
            # https://docs.microsoft.com/en-us/dotnet/api/system.datetime.ticks
            # kusto saves up to ticks, 1 tick == 100 nanoseconds            
            return pd.Timedelta(raw_value * 100, unit='ns')
        if isinstance(raw_value, six.string_types):
            time_parts = raw_value.split('.')
            if len(time_parts) == 3:
                seconds_fractions_part = time_parts[-1]
                whole_part = int(typed_value.total_seconds())
                fractions = str(whole_part) + "." + seconds_fractions_part
                total_seconds = float(fractions)
                
                return pd.Timedelta(total_seconds, unit='s')
            else:
                return pd.Timedelta(typed_value)

        return typed_value.total_seconds()
    
    raise ValueError("Unknown type {t}".format(kusto_type))

def to_datetime(value):
    """Converts a string to a datetime."""
    if value is None:
        return None

    if isinstance(value, six.integer_types):
        parsed = parser.parse(value)

    return parser.isoparse(value)    

def to_timedelta(value):
    """Converts a string to a timedelta."""
    if value is None:
        return None
    if isinstance(value, (six.integer_types, float)):
        return timedelta(microseconds=(float(value) / 10))
    match = _TIMESPAN_PATTERN.match(value)
    if match:
        if match.group(1) == "-":
            factor = -1
        else:
            factor = 1
        return factor * timedelta(
            days=int(match.group("d") or 0),
            hours=int(match.group("h")),
            minutes=int(match.group("m")),
            seconds=float(match.group("s")),
        )
    else:
        raise ValueError("Timespan value '{}' cannot be decoded".format(value))

from __future__ import annotations

import datetime
from enum import Enum
from uuid import UUID


# TODO: this should be done better
def cdm_type_to_kusto(cdm_type) -> KustoType:
    return KustoType.STRING


class KustoType(Enum):
    STRING = 'string'
    INT = 'int'
    DYNAMIC = 'dynamic'
    BOOL = 'boolean'
    REAL = 'real'
    DATETIME = 'datetime'
    TIMESPAN = 'timespan'
    GUID = 'guid'
    LONG = 'long'
    DECIMAL = 'decimal'

    def is_numeric(self):
        return self in [KustoType.INT, KustoType.DECIMAL, KustoType.DECIMAL, KustoType.LONG]


def dotnet_to_kusto_type(dotnet_type: str) -> KustoType:
    if dotnet_type == 'System.String':
        return KustoType.STRING
    if dotnet_type == 'System.Int32':
        return KustoType.INT
    if dotnet_type == 'System.Int64':
        return KustoType.LONG
    if dotnet_type == 'System.Object':
        return KustoType.DYNAMIC
    if dotnet_type == 'System.Boolean':
        return KustoType.BOOL
    if dotnet_type == 'System.Double':
        return KustoType.REAL
    if dotnet_type == 'System.Data.SqlTypes.SqlDecimal':
        return KustoType.DECIMAL
    if dotnet_type == 'System.DateTime':
        return KustoType.DATETIME
    if dotnet_type == 'System.TimeSpan':
        return KustoType.TIMESPAN
    if dotnet_type == 'System.Guid':
        return KustoType.GUID


def python_to_kusto_type(python_type: type) -> KustoType:
    if issubclass(python_type, str):
        return KustoType.STRING
    if issubclass(python_type, int):
        # technically, python int can be short or long, no way to know without checking values, so assuming the worst
        return KustoType.LONG
    if issubclass(python_type, (list, set, dict)):
        return KustoType.DYNAMIC
    if issubclass(python_type, bool):
        return KustoType.BOOL
    if issubclass(python_type, float):
        return KustoType.DECIMAL
    if issubclass(python_type, datetime.datetime):
        return KustoType.DATETIME
    if issubclass(python_type, datetime.timedelta):
        return KustoType.TIMESPAN
    if issubclass(python_type, UUID):
        return KustoType.GUID

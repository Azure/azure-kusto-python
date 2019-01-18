"""Kusto Data Models"""

import json
from datetime import datetime, timedelta
from enum import Enum
import six
from . import _converters
from .exceptions import KustoServiceError
from decimal import Decimal


class WellKnownDataSet(Enum):
    """Categorizes data tables according to the role they play in the data set that a Kusto query returns."""

    PrimaryResult = "PrimaryResult"
    QueryCompletionInformation = "QueryCompletionInformation"
    TableOfContents = "TableOfContents"
    QueryProperties = "QueryProperties"


class KustoResultRow(object):
    """Iterator over a Kusto result row."""

    convertion_funcs = {
        "datetime": _converters.to_datetime,
        "timespan": _converters.to_timedelta,
        "decimal": Decimal,
        "DateTime": _converters.to_datetime,
        "TimeSpan": _converters.to_timedelta,
        "Decimal": Decimal,
        "dynamic": json.loads,
    }

    def __init__(self, columns, row):
        self._value_by_name = {}
        self._value_by_index = []
        self._seventh_digit = {}
        for i, value in enumerate(row):
            column = columns[i]
            if column.column_type in KustoResultRow.convertion_funcs:
                if not value and column.column_type == "dynamic":
                    typed_value = json.loads("null")
                else:
                    typed_value = KustoResultRow.convertion_funcs[column.column_type](value)
                    if isinstance(typed_value, (datetime, timedelta)) and not isinstance(value, six.integer_types):
                        try:
                            char = value.split(":")[2].split(".")[1][6]
                            if char and char.isdigit():
                                tick = int(char)
                                if isinstance(typed_value, datetime):
                                    if tick < 5:
                                        self._seventh_digit[column.column_name] = -tick
                                    else:
                                        self._seventh_digit[column.column_name] = tick - 10
                                else:
                                    if typed_value < timedelta(0) and tick < 5:
                                        self._seventh_digit[column.column_name] = -tick
                                    elif typed_value > timedelta(0) and tick >= 5:
                                        self._seventh_digit[column.column_name] = tick - 10
                                    elif typed_value > timedelta(0) and tick < 5:
                                        self._seventh_digit[column.column_name] = tick
                                    else:
                                        self._seventh_digit[column.column_name] = 10 - tick


                        except IndexError:
                            pass
            else:
                typed_value = value

            self._value_by_index.append(typed_value)
            self._value_by_name[column.column_name] = typed_value

    @property
    def columns_count(self):
        return len(self._value_by_name)

    def __iter__(self):
        for i in range(self.columns_count):
            yield self[i]

    def __getitem__(self, key):
        if isinstance(key, six.integer_types):
            return self._value_by_index[key]
        return self._value_by_name[key]

    def __len__(self):
        return self.columns_count

    def to_dict(self):
        return self._value_by_name

    def to_list(self):
        return self._value_by_index

    def __str__(self):
        return ", ".join(self._value_by_index)

    def __repr__(self):
        return "KustoResultRow({})".format(", ".join(self._value_by_name))


class KustoResultColumn(object):
    def __init__(self, json_column, ordianl):
        self.column_name = json_column["ColumnName"]
        self.column_type = json_column.get("ColumnType") or json_column["DataType"]
        self.ordinal = ordianl

    def __repr__(self):
        return "KustoResultColumn({},{})".format(
            json.dumps({"ColumnName": self.column_name, "ColumnType": self.column_type}), self.ordinal
        )


class KustoResultTable(object):
    """Iterator over a Kusto result table."""

    def __init__(self, json_table):
        self.table_name = json_table.get("TableName")
        self.table_id = json_table.get("TableId")
        self.table_kind = WellKnownDataSet[json_table["TableKind"]] if "TableKind" in json_table else None
        self.columns = [KustoResultColumn(column, index) for index, column in enumerate(json_table["Columns"])]

        errors = [row for row in json_table["Rows"] if isinstance(row, dict)]
        if errors:
            raise KustoServiceError(errors[0]["OneApiErrors"][0]["error"]["@message"], json_table)

        self.rows = [KustoResultRow(self.columns, row) for row in json_table["Rows"]]

    @property
    def rows_count(self):
        return len(self.rows)

    @property
    def columns_count(self):
        return len(self.columns)

    def __len__(self):
        return self.rows_count

    def __iter__(self):
        for row in self.rows:
            yield row

    def __getitem__(self, key):
        return self.rows[key]

    def to_dict(self):
        return {"name": self.table_name, "kind": self.table_kind, "data": [r.to_dict() for r in self]}

    def __str__(self):
        d = self.to_dict()
        # enum is not serializable, using value instead
        d["kind"] = d["kind"].value
        return json.dumps(d)

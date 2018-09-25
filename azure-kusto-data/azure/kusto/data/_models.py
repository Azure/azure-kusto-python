"""Kusto Data Models"""

import json
import six
from enum import Enum
from . import _converters


class WellKnownDataSet(Enum):
    """Categorizes data tables according to the role they play in the data set that a Kusto query returns."""

    PrimaryResult = "PrimaryResult"
    QueryCompletionInformation = "QueryCompletionInformation"
    TableOfContents = "TableOfContents"
    QueryProperties = "QueryProperties"


class KustoResultRow(object):
    """Iterator over a Kusto result row."""

    def __init__(self, columns, row):
        self.columns = columns
        self.row = row

        # Here we keep converter functions for each type that we need to take special care
        # (e.g. convert)
        self.convertion_funcs = {
            "datetime": _converters.to_datetime,
            "timespan": _converters.to_timedelta,
            "DateTime": _converters.to_datetime,
            "TimeSpan": _converters.to_timedelta,
        }

    @property
    def columns_count(self):
        return len(self.columns)

    def __iter__(self):
        for i in range(self.columns_count):
            yield self[i]

    def __getitem__(self, key):
        if isinstance(key, six.integer_types):
            column = self.columns[key]
            value = self.row[key]
        else:
            column = next((column for column in self.columns if column.column_name == key), None)
            if not column:
                raise LookupError(key)
            value = self.row[column.ordinal]
        if column.column_type in self.convertion_funcs:
            return self.convertion_funcs[column.column_type](value)
        return value

    def __len__(self):
        return self.columns_count

    def to_dict(self):
        return {c.column_name: self.row[c.ordinal] for c in self.columns}

    def __str__(self):
        return self.row

    def __repr__(self):
        return "KustoResultRow({},{})".format(self.columns, self.row)


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

        self.rows = json_table["Rows"]

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
            yield KustoResultRow(self.columns, row)

    def __getitem__(self, key):
        return KustoResultRow(self.columns, self.rows[key])

    def to_dict(self):
        return {"name": self.table_name, "kind": self.table_kind, "data": [r.to_dict() for r in self]}

    def __str__(self):
        return json.dumps(self.to_dict())

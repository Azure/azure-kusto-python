# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import json
from decimal import Decimal
from enum import Enum
from typing import Iterator

from . import _converters
from .exceptions import KustoServiceError

HAS_PANDAS = True

try:
    import pandas
except ImportError as e:
    HAS_PANDAS = False

# This part is outside the try/catch because a failure here should raise an error
if HAS_PANDAS:
    from .helpers import to_pandas_datetime, to_pandas_timedelta, to_decimal


class WellKnownDataSet(Enum):
    """Categorizes data tables according to the role they play in the data set that a Kusto query returns."""

    PrimaryResult = "PrimaryResult"
    QueryCompletionInformation = "QueryCompletionInformation"
    TableOfContents = "TableOfContents"
    QueryProperties = "QueryProperties"


class KustoResultRow:
    """Iterator over a Kusto result row."""

    conversion_funcs = {"datetime": _converters.to_datetime, "timespan": _converters.to_timedelta, "decimal": Decimal}

    if HAS_PANDAS:
        pandas_funcs = {"datetime": to_pandas_datetime, "timespan": to_pandas_timedelta, "decimal": to_decimal}

    def __init__(self, columns, row):
        self._value_by_name = {}
        self._value_by_index = []
        self._hidden_values = []

        for i, value in enumerate(row):
            column = columns[i]
            try:
                column_type = column.column_type.lower()
            except AttributeError:
                self._value_by_index.append(value)
                self._value_by_name[columns[i]] = value
                if HAS_PANDAS:
                    self._hidden_values.append(value)
                continue

            # If you are here to read this, you probably hit some datetime/timedelta inconsistencies.
            # Azure-Data-Explorer(Kusto) supports 7 decimal digits, while the corresponding python types supports only 6.
            # One example why one might want this precision, is when working with pandas.
            # In that case, use azure.kusto.data.helpers.dataframe_from_result_table which takes into account the original value.
            typed_value = KustoResultRow.conversion_funcs[column_type](value) if value is not None and column_type in KustoResultRow.conversion_funcs else value

            # This is a special case where plain python will lose precision, so we keep the precise value hidden.
            # When transforming to pandas, we can use the hidden value to convert to precise pandas/numpy types
            if HAS_PANDAS:
                self._hidden_values.append(
                    KustoResultRow.pandas_funcs[column_type](value, typed_value) if value is not None and column_type in KustoResultRow.pandas_funcs else value
                )

            self._value_by_index.append(typed_value)
            self._value_by_name[column.column_name] = typed_value

    @property
    def columns_count(self) -> int:
        return len(self._value_by_name)

    def __iter__(self):
        for i in range(self.columns_count):
            yield self[i]

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._value_by_index[key]
        return self._value_by_name[key]

    def __len__(self):
        return self.columns_count

    def to_dict(self) -> dict:
        return self._value_by_name

    def to_list(self) -> list:
        return self._value_by_index

    def __str__(self):
        return "['{}']".format("', '".join([str(val) for val in self._value_by_index]))

    def __repr__(self):
        values = [repr(val) for val in self._value_by_name.values()]
        return "KustoResultRow(['{}'], [{}])".format("', '".join(self._value_by_name), ", ".join(values))


class KustoResultColumn:
    def __init__(self, json_column: dict, ordinal: int):
        self.column_name = json_column["ColumnName"]
        self.column_type = json_column.get("ColumnType") or json_column["DataType"]
        self.ordinal = ordinal

    def __repr__(self):
        return "KustoResultColumn({},{})".format(json.dumps({"ColumnName": self.column_name, "ColumnType": self.column_type}), self.ordinal)


class KustoResultTable:
    """Iterator over a Kusto result table."""

    def __init__(self, json_table: dict):
        self.table_name = json_table.get("TableName")
        self.table_id = json_table.get("TableId")
        self.table_kind = WellKnownDataSet[json_table["TableKind"]] if "TableKind" in json_table else None
        self.columns = [KustoResultColumn(column, index) for index, column in enumerate(json_table["Columns"])]

        errors = [row for row in json_table["Rows"] if isinstance(row, dict)]
        if errors:
            raise KustoServiceError(errors[0]["OneApiErrors"][0]["error"]["@message"], json_table)

        self.rows = [KustoResultRow(self.columns, row) for row in json_table["Rows"]]

    @property
    def _rows(self) -> Iterator:
        for row in self.rows:
            yield row._hidden_values

    @property
    def rows_count(self) -> int:
        return len(self.rows)

    @property
    def columns_count(self) -> int:
        return len(self.columns)

    def to_dict(self):
        """Converts the table to a dict."""
        return {"name": self.table_name, "kind": self.table_kind, "data": [r.to_dict() for r in self]}

    def __len__(self):
        return self.rows_count

    def __iter__(self):
        for row in self.rows:
            yield row

    def __getitem__(self, key):
        return self.rows[key]

    def __str__(self):
        d = self.to_dict()
        # enum is not serializable, using value instead
        d["kind"] = d["kind"].value
        return json.dumps(d, default=str)

    def __bool__(self):
        return any(self.columns)

    __nonzero__ = __bool__

"""Kusto Data Models"""

import json
from datetime import datetime, timedelta
from enum import Enum
from decimal import Decimal
import six
from . import _converters
from .exceptions import KustoServiceError


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
        "dynamic": json.loads,
    }

    def __init__(self, columns, row):
        self._value_by_name = {}
        self._value_by_index = []
        self._seventh_digit = {}
        for i, value in enumerate(row):
            column = columns[i]
            try:
                lower_column_type = column.column_type.lower()
            except AttributeError:
                self._value_by_index.append(value)
                self._value_by_name[columns[i]] = value
                continue

            if lower_column_type == "dynamic" and not value:
                typed_value = json.loads("null")
                # TODO: Remove this if clause.
                # Today there are two types of responses to jull json onject: empty string, or null.
                # There is current work being done to stay with the last, as this is the official representation for
                # empty json. Once this work is done this if should be removed.
                # The servers should not return empty string anymore as a valid json.
            elif lower_column_type in ["datetime", "timespan"]:
                if value is None:
                    typed_value = None
                else:
                    try:
                        # If you are here to read this, you probably hit some datetime/timedelta inconsistencies.
                        # Azure-Data-Explorer(Kusto) supports 7 decimal digits, while the corresponding python types supports only 6.
                        # What we do here, is remove the 7th digit, if exists, and create a datetime/timedelta
                        # from whats left. The reason we are keeping the 7th digit, is to allow users to work with
                        # this precision in case they want it. One example why one might want this precision, is when
                        # working with pandas. In that case, use azure.kusto.data.helpers.dataframe_from_result_table
                        # which takes into account the 7th digit.
                        char = value.split(":")[2].split(".")[1][6]
                        if char.isdigit():
                            tick = int(char)
                            last = value[-1] if value[-1].isalpha() else ""
                            typed_value = KustoResultRow.convertion_funcs[lower_column_type](value[:-2] + last)
                            if tick:
                                if lower_column_type == "datetime":
                                    self._seventh_digit[column.column_name] = tick
                                else:
                                    self._seventh_digit[column.column_name] = (
                                        tick if abs(typed_value) == typed_value else -tick
                                    )
                        else:
                            typed_value = KustoResultRow.convertion_funcs[lower_column_type](value)
                    except (IndexError, AttributeError):
                        typed_value = KustoResultRow.convertion_funcs[lower_column_type](value)
            elif lower_column_type in KustoResultRow.convertion_funcs:
                typed_value = KustoResultRow.convertion_funcs[lower_column_type](value)
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
        return "['{}']".format("', '".join([str(val) for val in self._value_by_index]))

    def __repr__(self):
        values = [repr(val) for val in self._value_by_name.values()]
        return "KustoResultRow(['{}'], [{}])".format("', '".join(self._value_by_name), ", ".join(values))


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

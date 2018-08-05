"""This module constains all classes to get Kusto responses. Including error handling."""

from datetime import timedelta
import re

import json
import uuid
import dateutil.parser
import requests
import pandas
import six
import numbers

from .exceptions import KustoServiceError
from .security import _AadHelper
from .version import VERSION

# Regex for TimeSpan
TIMESPAN_PATTERN = re.compile(
    r"(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)"
)


class _KustoResult(dict):
    """Simple wrapper around dictionary, to enable both index and key access to rows in result."""

    def __init__(self, index2column_mapping, *args, **kwargs):
        super(_KustoResult, self).__init__(*args, **kwargs)
        # TODO: this is not optimal, if client will not access all fields.
        # In that case, we are having unnecessary perf hit to convert Timestamp,
        # even if client don't use it.
        # In this case, it would be better for KustoResult to extend list class. In this case,
        # KustoResultIter.index2column_mapping should be reversed, e.g. column2index_mapping.
        self.index2column_mapping = index2column_mapping

    def __getitem__(self, key):
        if isinstance(key, numbers.Number):
            val = dict.__getitem__(self, self.index2column_mapping[key])
        else:
            val = dict.__getitem__(self, key)
        return val


class _KustoResultIter(six.Iterator):
    """Iterator over returned rows."""

    def __init__(self, json_result):
        self.json_result = json_result
        self.index2column_mapping = []
        self.index2type_mapping = []
        for column in json_result["Columns"]:
            self.index2column_mapping.append(column["ColumnName"])
            self.index2type_mapping.append(
                column["ColumnType"] if "ColumnType" in column else column["DataType"]
            )
        self.row_index = 0
        self.rows_count = len(json_result["Rows"])
        # Here we keep converter functions for each type that we need to take special care
        # (e.g. convert)
        self.converters_lambda_mappings = {
            "datetime": self.to_datetime,
            "timespan": self.to_timedelta,
            "DateTime": self.to_datetime,
            "TimeSpan": self.to_timedelta,
        }

    @staticmethod
    def to_datetime(value):
        """Converts a string to a datetime."""
        if value is None:
            return None
        return dateutil.parser.parse(value)

    @staticmethod
    def to_timedelta(value):
        """Converts a string to a timedelta."""
        if value is None:
            return None
        if isinstance(value, numbers.Number):
            return timedelta(microseconds=(float(value) / 10))
        match = TIMESPAN_PATTERN.match(value)
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

    def __iter__(self):
        return self

    def __next__(self):
        if self.row_index >= self.rows_count:
            raise StopIteration
        row = self.json_result["Rows"][self.row_index]
        result_dict = {}
        for index, value in enumerate(row):
            data_type = self.index2type_mapping[index]
            if data_type in self.converters_lambda_mappings:
                result_dict[self.index2column_mapping[index]] = self.converters_lambda_mappings[
                    data_type
                ](value)
            else:
                result_dict[self.index2column_mapping[index]] = value
        self.row_index = self.row_index + 1
        return _KustoResult(self.index2column_mapping, result_dict)


class _KustoResponse(object):
    """Wrapper for response."""

    # TODO: add support to get additional information from response, like execution time

    def __init__(self, json_response):
        self.json_response = json_response

    def get_raw_response(self):
        """Gets the json response got from Kusto."""
        return self.json_response

    def get_table_count(self):
        """Gets the tables Count."""
        if isinstance(self.json_response, list):
            return len(self.json_response)
        return len(self.json_response["Tables"])

    def has_exceptions(self):
        """Checkes whether an exception was thrown."""
        if isinstance(self.json_response, list):
            return list(
                filter(lambda x: x["FrameType"] == "DataSetCompletion", self.json_response)
            )[0]["HasErrors"]
        return "Exceptions" in self.json_response

    def get_exceptions(self):
        """ Gets the excpetions got from Kusto if exists. """
        if self.has_exceptions():
            if isinstance(self.json_response, list):
                return list(
                    filter(lambda x: x["FrameType"] == "DataSetCompletion", self.json_response)
                )[0]["OneApiErrors"]
            return self.json_response["Exceptions"]
        return None

    def iter_all(self, table_id=-1):
        """ Returns iterator to get rows from response """
        if table_id == -1:
            table_id = self._get_default_table_id()
        return _KustoResultIter(self._get_table(table_id))

    def to_dataframe(self, errors="raise"):
        """Returns Pandas data frame."""
        if not self.json_response:
            return pandas.DataFrame()

        table = self._get_table(self._get_default_table_id())
        rows_data = table["Rows"]
        kusto_columns = table["Columns"]

        col_names = [col["ColumnName"] for col in kusto_columns]
        frame = pandas.DataFrame(rows_data, columns=col_names)

        for col in kusto_columns:
            col_name = col["ColumnName"]
            col_type = col["ColumnType"] if "ColumnType" in col else col["DataType"]
            if col_type.lower() == "timespan":
                frame[col_name] = pandas.to_timedelta(
                    frame[col_name].apply(
                        lambda t: t.replace(".", " days ") if t and "." in t.split(":")[0] else t
                    )
                )
            elif col_type.lower() == "dynamic":
                frame[col_name] = frame[col_name].apply(lambda x: json.loads(x) if x else None)
            elif col_type in self._kusto_to_data_frame_data_types:
                pandas_type = self._kusto_to_data_frame_data_types[col_type]
                frame[col_name] = frame[col_name].astype(pandas_type, errors=errors)

        return frame

    def _get_default_table_id(self):
        if isinstance(self.json_response, list):
            return 2
        return 0

    def _get_table(self, table_id):
        if isinstance(self.json_response, list):
            return self.json_response[table_id]
        return self.json_response["Tables"][table_id]

    _kusto_to_data_frame_data_types = {
        "bool": "bool",
        "uint8": "int64",
        "int16": "int64",
        "uint16": "int64",
        "int": "int64",
        "uint": "int64",
        "long": "int64",
        "ulong": "int64",
        "float": "float64",
        "real": "float64",
        "decimal": "float64",
        "string": "object",
        "datetime": "datetime64[ns]",
        "guid": "object",
        "timespan": "timedelta64[ns]",
        "dynamic": "object",
        # Support V1
        "DateTime": "datetime64[ns]",
        "Int32": "int32",
        "Int64": "int64",
        "Double": "float64",
        "String": "object",
        "SByte": "object",
        "Guid": "object",
        "TimeSpan": "object",
    }

"""This module constains all classes to get Kusto responses. Including error handling."""

from datetime import timedelta
import re

from abc import ABCMeta, abstractmethod
import json
from enum import Enum
import numbers
import dateutil.parser
import pandas
import six

# Regex for TimeSpan
_TIMESPAN_PATTERN = re.compile(r"(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)")


class WellKnownDataSet(Enum):
    """Categorizes data tables according to the role they play in the data set that a Kusto query returns."""

    PrimaryResult = "PrimaryResult"
    QueryCompletionInformation = "QueryCompletionInformation"
    TableOfContents = "TableOfContents"
    QueryProperties = "QueryProperties"


class _KustoResultRow(object):
    """Iterator over a Kusto result row."""

    def __init__(self, columns_count, columns, row):
        self._columns_count = columns_count
        self._columns = columns
        self._row = row
        # Here we keep converter functions for each type that we need to take special care
        # (e.g. convert)
        self.converters_lambda_mappings = {
            "datetime": self.to_datetime,
            "timespan": self.to_timedelta,
            "DateTime": self.to_datetime,
            "TimeSpan": self.to_timedelta,
        }

    def __iter__(self):
        for i in range(self._columns_count):
            yield self[i]

    def __getitem__(self, key):
        if isinstance(key, numbers.Number):
            column = self._columns[key]
            value = self._row[key]
        else:
            column = next((column for column in self._columns if column.column_name == key), None)
            if not column:
                raise LookupError(key)
            value = self._row[column.ordinal]
        if column.column_type in self.converters_lambda_mappings:
            return self.converters_lambda_mappings[column.column_type](value)
        return value

    def __len__(self):
        return self._columns_count

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


class _KustoResultColumn(object):
    def __init__(self, json_column, ordianl):
        self.column_name = json_column["ColumnName"]
        self.column_type = json_column["ColumnType"] if "ColumnType" in json_column else json_column["DataType"]
        self.ordinal = ordianl


class _KustoResultTable(object):
    """Iterator over a Kusto result table."""

    def __init__(self, json_table):
        self.table_name = json_table["TableName"]
        self.table_id = json_table["TableId"] if "TableId" in json_table else None
        self.table_kind = WellKnownDataSet[json_table["TableKind"]] if "TableKind" in json_table else None
        self.columns = []
        ordinal = 0
        for column in json_table["Columns"]:
            self.columns.append(_KustoResultColumn(column, ordinal))
            ordinal += 1
        self.rows_count = len(json_table["Rows"])
        self.columns_count = len(self.columns)
        self._rows = json_table["Rows"]

    def __iter__(self):
        for row in self._rows:
            yield _KustoResultRow(self.columns_count, self.columns, row)

    def __getitem__(self, key):
        return _KustoResultRow(self.columns_count, self.columns, self._rows[key])

    def __len__(self):
        return self.rows_count

    def to_dataframe(self, errors="raise"):
        """Returns Pandas data frame."""
        if not self.columns or not self._rows:
            return pandas.DataFrame()

        frame = pandas.DataFrame(self._rows, columns=[column.column_name for column in self.columns])

        for column in self.columns:
            col_name = column.column_name
            col_type = column.column_type
            if col_type.lower() == "timespan":
                frame[col_name] = pandas.to_timedelta(
                    frame[col_name].apply(lambda t: t.replace(".", " days ") if t and "." in t.split(":")[0] else t)
                )
            elif col_type.lower() == "dynamic":
                frame[col_name] = frame[col_name].apply(lambda x: json.loads(x) if x else None)
            elif col_type in self._kusto_to_data_frame_data_types:
                pandas_type = self._kusto_to_data_frame_data_types[col_type]
                frame[col_name] = frame[col_name].astype(pandas_type, errors=errors)

        return frame

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


@six.add_metaclass(ABCMeta)
class _KustoResponseDataSet:
    """Represents the parsed data set carried by the response to a Kusto request."""

    def __init__(self, json_response):
        self.tables = [_KustoResultTable(t) for t in json_response]
        self.tables_count = len(self.tables)
        self._tables_names = [t.table_name for t in self.tables]

    @property
    @abstractmethod
    def _error_column(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def _crid_column(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def _status_column(self):
        raise NotImplementedError

    @property
    def primary_results(self):
        """Returns primary results. If there is more than one returns a list."""
        if self.tables_count == 1:
            return self.tables
        primary = list(filter(lambda x: x.table_kind == WellKnownDataSet.PrimaryResult, self.tables))

        return primary

    @property
    def errors_count(self):
        """Checks whether an exception was thrown."""
        query_status_table = next(
            (t for t in self.tables if t.table_kind == WellKnownDataSet.QueryCompletionInformation), None
        )
        if not query_status_table:
            return 0
        min_level = 4
        errors = 0
        for row in query_status_table:
            if row[self._error_column] < 4:
                if row[self._error_column] < min_level:
                    min_level = row[self._error_column]
                    errors = 1
                elif row[self._error_column] == min_level:
                    errors += 1

        return errors

    def get_exceptions(self):
        """Gets the excpetions retrieved from Kusto if exists."""
        query_status_table = next(
            (t for t in self.tables if t.table_kind == WellKnownDataSet.QueryCompletionInformation), None
        )
        if not query_status_table:
            return []
        result = []
        for row in query_status_table:
            if row[self._error_column] < 4:
                result.append(
                    "Please provide the following data to Kusto: CRID='{0}' Description:'{1}'".format(
                        row[self._crid_column], row[self._status_column]
                    )
                )
        return result

    def __iter__(self):
        return iter(self.tables)

    def __getitem__(self, key):
        if isinstance(key, numbers.Number):
            return self.tables[key]
        try:
            return self.tables[self._tables_names.index(key)]
        except ValueError:
            raise LookupError(key)

    def __len__(self):
        return self.tables_count


class _KustoResponseDataSetV1(_KustoResponseDataSet):

    _status_column = "StatusDescription"
    _crid_column = "ClientActivityId"
    _error_column = "Severity"
    _tables_kinds = {
        "QueryResult": WellKnownDataSet.PrimaryResult,
        "QueryProperties": WellKnownDataSet.QueryProperties,
        "QueryStatus": WellKnownDataSet.QueryCompletionInformation,
    }

    def __init__(self, json_response):
        super(_KustoResponseDataSetV1, self).__init__(json_response["Tables"])
        if self.tables_count <= 2:
            self.tables[0].table_kind = WellKnownDataSet.PrimaryResult
            self.tables[0].table_id = 0

            if self.tables_count == 2:
                self.tables[1].table_kind = WellKnownDataSet.QueryProperties
                self.tables[1].table_id = 1
        else:
            toc = self.tables[-1]
            toc.table_kind = WellKnownDataSet.TableOfContents
            toc.table_id = self.tables_count - 1
            for i in range(self.tables_count - 1):
                self.tables[i].table_name = toc[i]["Name"]
                self.tables[i].table_id = toc[i]["Id"]
                self.tables[i].table_kind = self._tables_kinds[toc[i]["Kind"]]


class _KustoResponseDataSetV2(_KustoResponseDataSet):
    _status_column = "Payload"
    _error_column = "Level"
    _crid_column = "ClientRequestId"

    def __init__(self, json_response):
        super(_KustoResponseDataSetV2, self).__init__([t for t in json_response if t["FrameType"] == "DataTable"])

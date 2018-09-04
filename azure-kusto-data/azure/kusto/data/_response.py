"""This module constains all classes to get Kusto responses. Including error handling."""
import json

from datetime import timedelta
from abc import ABCMeta, abstractmethod

import six

from enum import Enum
import dateutil.parser

from . import converters


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
            "datetime": converters.to_datetime,
            "timespan": converters.to_timedelta,
            "DateTime": converters.to_datetime,
            "TimeSpan": converters.to_timedelta,
        }

    @property
    def columns_count(self):
        return len(self.columns)

    def __iter__(self):
        for i in range(self.columns_count):
            yield self[i]

    def __getitem__(self, key):
        if type(key) == int:
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

    def __repr__(self):
        return "KustoResultRow({},{})".format(self.columns, self.row)


class KustoResultColumn(object):
    def __init__(self, json_column, ordianl):
        self.column_name = json_column["ColumnName"]
        self.column_type = json_column.get("ColumnType", json_column["DataType"])
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

    def __str__(self):
        return "<{table_name} : rows [{rows_count}], cols [{cols_count}]>".format(
            self.table_name, self.rows_count, self.columns_count
        )

    # TODO: not sure why this is here...
    def to_dataframe(self, errors="raise"):
        import pandas

        kusto_to_dataframe_data_types = {
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

        """Returns Pandas data frame."""
        if not self.columns or not self.rows:
            return pandas.DataFrame()

        frame = pandas.DataFrame(self.rows, columns=[column.column_name for column in self.columns])

        for column in self.columns:
            col_name = column.column_name
            col_type = column.column_type
            if col_type.lower() == "timespan":
                frame[col_name] = pandas.to_timedelta(
                    frame[col_name].apply(lambda t: t.replace(".", " days ") if t and "." in t.split(":")[0] else t)
                )
            elif col_type.lower() == "dynamic":
                frame[col_name] = frame[col_name].apply(lambda x: json.loads(x) if x else None)
            elif col_type in kusto_to_dataframe_data_types:
                pandas_type = kusto_to_dataframe_data_types[col_type]
                frame[col_name] = frame[col_name].astype(pandas_type, errors=errors)

        return frame


@six.add_metaclass(ABCMeta)
class KustoResponseDataSet:
    """Represents the parsed data set carried by the response to a Kusto request."""

    def __init__(self, json_response):
        self.tables = [KustoResultTable(t) for t in json_response]
        self.tables_count = len(self.tables)
        self.tables_names = [t.table_name for t in self.tables]

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
        """Gets the exceptions retrieved from Kusto if exists."""
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
        if type(key) == int():
            return self.tables[key]
        try:
            return self.tables[self.tables_names.index(key)]
        except ValueError:
            raise LookupError(key)

    def __len__(self):
        return self.tables_count


class KustoResponseDataSetV1(KustoResponseDataSet):

    _status_column = "StatusDescription"
    _crid_column = "ClientActivityId"
    _error_column = "Severity"
    _tables_kinds = {
        "QueryResult": WellKnownDataSet.PrimaryResult,
        "QueryProperties": WellKnownDataSet.QueryProperties,
        "QueryStatus": WellKnownDataSet.QueryCompletionInformation,
    }

    def __init__(self, json_response):
        super(KustoResponseDataSetV1, self).__init__(json_response["Tables"])
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


class KustoResponseDataSetV2(KustoResponseDataSet):
    _status_column = "Payload"
    _error_column = "Level"
    _crid_column = "ClientRequestId"

    def __init__(self, json_response):
        super(KustoResponseDataSetV2, self).__init__([t for t in json_response if t["FrameType"] == "DataTable"])

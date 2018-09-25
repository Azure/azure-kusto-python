"""This module constains all classes to get Kusto responses. Including error handling."""

import json

from datetime import timedelta
from abc import ABCMeta, abstractmethod

import six

from ._models import KustoResultColumn, KustoResultRow, KustoResultTable, WellKnownDataSet


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
        if isinstance(key, six.integer_types):
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

"""This module constains all classes to get Kusto responses. Including error handling."""

from datetime import timedelta
import re

from abc import ABCMeta, abstractmethod
import json
import uuid
import dateutil.parser
from enum import Enum
import requests
import pandas
import six
import numbers

from .aad_helper import _AadHelper
from .exceptions import KustoServiceError
from ._version import VERSION

# Regex for TimeSpan
_TIMESPAN_PATTERN = re.compile(
    r"(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)"
)


class WellKnownDataSet(Enum):
    """Categorizes data tables according to the role they play in the data set that a Kusto query returns."""

    PrimaryResult = ("PrimaryResult",)
    QueryCompletionInformation = ("QueryCompletionInformation",)
    TableOfContents = ("TableOfContents",)
    QueryProperties = ("QueryProperties",)


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
        self.column_type = (
            json_column["ColumnType"] if "ColumnType" in json_column else json_column["DataType"]
        )
        self.ordinal = ordianl


class _KustoResultTable(object):
    """Iterator over a Kusto result table."""

    def __init__(self, json_table):
        self.table_name = json_table["TableName"]
        self.table_id = json_table["TableId"] if "TableId" in json_table else None
        self.table_kind = (
            WellKnownDataSet[json_table["TableKind"]] if "TableKind" in json_table else None
        )
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

        frame = pandas.DataFrame(
            self._rows, columns=[column.column_name for column in self.columns]
        )

        for column in self.columns:
            col_name = column.column_name
            col_type = column.column_type
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
        if self.tables_count == 1:
            return self.tables[0]
        primary = list(
            filter(lambda x: x.table_kind == WellKnownDataSet.PrimaryResult, self.tables)
        )
        if len(primary) == 1:
            return primary[0]
        return primary

    @property
    def errors_count(self):
        """Checks whether an exception was thrown."""
        query_status_table = next(
            (t for t in self.tables if t.table_kind == WellKnownDataSet.QueryCompletionInformation),
            None,
        )
        if not query_status_table:
            return 0
        min_level = 4
        errors = 0
        for q in query_status_table:
            if q[self._error_column] < 4:
                if q[self._error_column] < min_level:
                    min_level = q[self._error_column]
                    errors = 1
                elif q[self._error_column] == min_level:
                    errors += 1

        return errors

    def get_exceptions(self):
        """Gets the excpetions retrieved from Kusto if exists."""
        query_status_table = next(
            (t for t in self.tables if t.table_kind == WellKnownDataSet.QueryCompletionInformation),
            None,
        )
        if not query_status_table:
            return
        result = []
        for q in query_status_table:
            if q[self._error_column] < 4:
                result.append(
                    "Please provide the following data ot Kusto: CRID='{0}' Description:'{1}'".format(
                        q[self._crid_column], q[self._status_column]
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

    def to_dataframe(self):
        return pandas.DataFrame(data=self.tables, columns=self._tables_names)


class _KustoResponseDataSetV1(_KustoResponseDataSet):

    _status_column = "StatusDescription"
    _crid_column = "ClientActivityId"
    _error_column = "Severity"
    _tables_kinds = {
        "QueryResult": WellKnownDataSet.PrimaryResult,
        "@ExtendedProperties": WellKnownDataSet.QueryProperties,
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
            for i in range(self.tables_count - 1):
                self.tables[i].table_name = toc[i]["Name"]
                self.tables[i].table_id = toc[i]["Id"]
                self.tables[i].table_kind = self._tables_kinds[toc[i]["Kind"]]


class _KustoResponseDataSetV2(_KustoResponseDataSet):
    _status_column = "Payload"
    _error_column = "Level"
    _crid_column = "ClientRequestId"

    def __init__(self, json_response):
        super(_KustoResponseDataSetV2, self).__init__(
            [t for t in json_response if t["FrameType"] == "DataTable"]
        )


class KustoClient(object):
    """
    Kusto client for Python.

    KustoClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    KustoClient takes care of ADAL authentication, parsing response and giving you typed result set.

    Test are run using nose.

    Examples
    --------
    When using KustoClient, you can choose between three options for authenticating:

    Option 1:
    You'll need to have your own AAD application and know your client credentials (client_id and client_secret).
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster, client_id='your_app_id', client_secret='your_app_secret')

    Option 2:
    You can use KustoClient's client id (set as a default in the constructor) and authenticate using your username and password.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster, username='your_username', password='your_password')

    Option 3:
    You can use KustoClient's client id (set as a default in the constructor) and authenticate using your username and an AAD pop up.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster)

    After connecting, use the kusto_client instance to execute a management command or a query:
    >>> kusto_database = 'Samples'
    >>> response = kusto_client.execute_query(kusto_database, 'StormEvents | take 10')
    You can access rows now by index or by key.
    >>> for row in response.iter_all():
    >>>    print(row[0])
    >>>    print(row["ColumnName"])    """

    def __init__(
        self,
        kusto_cluster,
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        authority=None,
    ):
        """
        Kusto Client constructor.

        Parameters
        ----------
        kusto_cluster : str
            Kusto cluster endpoint. Example: https://help.kusto.windows.net
        client_id : str
            The AAD application ID of the application making the request to Kusto
        client_secret : str
            The AAD application key of the application making the request to Kusto.
            if this is given, then username/password should not be.
        username : str
            The username of the user making the request to Kusto.
            if this is given, then password must follow and the client_secret should not be given.
        password : str
            The password matching the username of the user making the request to Kusto
        authority : 'microsoft.com', optional
            In case your tenant is not microsoft please use this param.
        """
        self.kusto_cluster = kusto_cluster
        self._mgmt_endpoint = "{0}/v1/rest/mgmt".format(self.kusto_cluster)
        self._query_endpoint = "{0}/v2/rest/query".format(self.kusto_cluster)
        self._aad_helper = _AadHelper(
            kusto_cluster, client_id, client_secret, username, password, authority
        )

    def execute(self, kusto_database, query, accept_partial_results=False, timeout=None):
        """ Execute a simple query or management command

        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        query : str
            Query to be executed
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        if query.startswith("."):
            return self.execute_mgmt(kusto_database, query, accept_partial_results, timeout)
        return self.execute_query(kusto_database, query, accept_partial_results, timeout)

    def execute_query(self, kusto_database, query, accept_partial_results=False, timeout=None):
        """ Execute a simple query

        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        kusto_query : str
            Query to be executed
        query_endpoint : str
            The query's endpoint
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        return self._execute(
            self._query_endpoint, kusto_database, query, accept_partial_results, timeout
        )

    def execute_mgmt(self, kusto_database, query, accept_partial_results=False, timeout=None):
        """ Execute a management command

        Parameters
        ----------
        kusto_database : str
            Database against query will be executed.
        kusto_query : str
            Query to be executed
        query_endpoint : str
            The query's endpoint
        accept_partial_results : bool
            Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        timeout : float, optional
            Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        return self._execute(
            self._mgmt_endpoint, kusto_database, query, accept_partial_results, timeout
        )

    def _execute(
        self,
        endpoint,
        kusto_database,
        kusto_query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
        """Executes given query against this client"""

        request_payload = {"db": kusto_database, "csl": kusto_query}

        access_token = self._aad_helper.acquire_token()
        request_headers = {
            "Authorization": access_token,
            "Accept": "application/json",
            "Accept-Encoding": "gzip,deflate",
            "Content-Type": "application/json; charset=utf-8",
            "Fed": "True",
            "x-ms-client-version": "Kusto.Python.Client:" + VERSION,
            "x-ms-client-request-id": "KPC.execute;" + str(uuid.uuid4()),
        }

        response = requests.post(
            endpoint, headers=request_headers, json=request_payload, timeout=timeout
        )

        if response.status_code == 200:
            if get_raw_response:
                return response.json()

            if endpoint.endswith("v2/rest/query"):
                kusto_response = _KustoResponseDataSetV2(response.json())
            else:
                kusto_response = _KustoResponseDataSetV1(response.json())

            if kusto_response.errors_count > 0 and not accept_partial_results:
                raise KustoServiceError(kusto_response.get_exceptions(), response, kusto_response)
            return kusto_response
        else:
            raise KustoServiceError([response.json()], response)

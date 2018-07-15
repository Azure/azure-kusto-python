"""
This module constains all classes to get Kusto responses.
Including error handling.
"""

from datetime import timedelta
import re

import json
import uuid
import dateutil.parser
import requests
import pandas
import six
import numbers

from .aad_helper import _AadHelper
from .kusto_exceptions import KustoServiceError
from .version import VERSION

# Regex for TimeSpan
TIMESPAN_PATTERN = re.compile(r'(-?)((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2}(\.[0-9]+)?$)')

class KustoResult(dict):
    """ Simple wrapper around dictionary, to enable both index and key access to rows in result """
    def __init__(self, index2column_mapping, *args, **kwargs):
        super(KustoResult, self).__init__(*args, **kwargs)
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


class KustoResultIter(six.Iterator):
    """ Iterator over returned rows """
    def __init__(self, json_result):
        self.json_result = json_result
        self.index2column_mapping = []
        self.index2type_mapping = []
        for column in json_result['Columns']:
            self.index2column_mapping.append(column['ColumnName'])
            self.index2type_mapping.append(column['ColumnType'] if 'ColumnType' in column else column['DataType'])
        self.row_index = 0
        self.rows_count = len(json_result['Rows'])
        # Here we keep converter functions for each type that we need to take special care
        # (e.g. convert)
        self.converters_lambda_mappings = {
            'datetime': self.to_datetime,
            'timespan': self.to_timedelta,
            'DateTime': self.to_datetime,
            'TimeSpan': self.to_timedelta,
            }

    @staticmethod
    def to_datetime(value):
        """ Converts a string to a datetime """
        if value is None:
            return None
        return dateutil.parser.parse(value)

    @staticmethod
    def to_timedelta(value):
        """ Converts a string to a timedelta """
        if value is None:
            return None
        if isinstance(value, numbers.Number):
            return timedelta(microseconds=(float(value)/10))
        match = TIMESPAN_PATTERN.match(value)
        if match:
            if match.group(1) == '-':
                factor = -1
            else:
                factor = 1
            return factor * timedelta(
                days=int(match.group('d') or 0),
                hours=int(match.group('h')),
                minutes=int(match.group('m')),
                seconds=float(match.group('s')))
        else:
            raise ValueError('Timespan value \'{}\' cannot be decoded'.format(value))

    def __iter__(self):
        return self

    def __next__(self):
        if self.row_index >= self.rows_count:
            raise StopIteration
        row = self.json_result['Rows'][self.row_index]
        result_dict = {}
        for index, value in enumerate(row):
            data_type = self.index2type_mapping[index]
            if data_type in self.converters_lambda_mappings:
                result_dict[self.index2column_mapping[index]] = self.converters_lambda_mappings[data_type](value)
            else:
                result_dict[self.index2column_mapping[index]] = value
        self.row_index = self.row_index + 1
        return KustoResult(self.index2column_mapping, result_dict)


class KustoResponse(object):
    """ Wrapper for response """
    # TODO: add support to get additional information from response, like execution time

    def __init__(self, json_response):
        self.json_response = json_response

    def get_raw_response(self):
        """ Gets the json response got from Kusto """
        return self.json_response

    def get_table_count(self):
        """ Gets the tables Count. """
        if isinstance(self.json_response, list):
            return len(self.json_response)
        return len(self.json_response["Tables"])

    def has_exceptions(self):
        """ Checkes whether an exception was thrown. """
        if isinstance(self.json_response, list):
            return list(filter(lambda x: x['FrameType'] == 'DataSetCompletion', self.json_response))[0]['HasErrors']
        return 'Exceptions' in self.json_response

    def get_exceptions(self):
        """ Gets the excpetions got from Kusto if exists. """
        if self.has_exceptions():
            if isinstance(self.json_response, list):
                return list(filter(lambda x: x['FrameType'] == 'DataSetCompletion', self.json_response))[0]['OneApiErrors']
            return self.json_response['Exceptions']
        return None

    def iter_all(self, table_id=-1):
        """ Returns iterator to get rows from response """
        if table_id == -1:
            table_id = self._get_default_table_id()
        return KustoResultIter(self._get_table(table_id))

    def to_dataframe(self, errors='raise'):
        """ Returns Pandas data frame. """
        if not self.json_response:
            return pandas.DataFrame()

        table = self._get_table(self._get_default_table_id())
        rows_data = table["Rows"]
        kusto_columns = table["Columns"]

        col_names = [col["ColumnName"] for col in kusto_columns]
        frame = pandas.DataFrame(rows_data, columns=col_names)

        for col in kusto_columns:
            col_name = col["ColumnName"]
            col_type = col['ColumnType'] if 'ColumnType' in col else col['DataType']
            if col_type.lower() == "timespan":
                frame[col_name] = pandas.to_timedelta(frame[col_name].apply(lambda t: t.replace(".", " days ") if t and '.' in t.split(":")[0] else t))
            elif col_type.lower() == "dynamic":
                frame[col_name] = frame[col_name].apply(lambda x: json.loads(x) if x else None)
            else:
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
        return self.json_response['Tables'][table_id]

    _kusto_to_data_frame_data_types = {
        'datetime' : 'datetime64[ns]',
        'int' : 'int32',
        'long' : 'int64',
        'real' : 'float64',
        'string' : 'object',
        'bool' : 'object',
        'guid' : 'object',
        'timespan' : 'object',

        # Support V1
        'DateTime' : 'datetime64[ns]',
        'Int32' : 'int32',
        'Int64' : 'int64',
        'Double' : 'float64',
        'String' : 'object',
        'SByte' : 'object',
        'Guid' : 'object',
        'TimeSpan' : 'object',
    }

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

    def __init__(self, kusto_cluster, client_id=None, client_secret=None, username=None, password=None, authority=None):
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
        self._aad_helper = _AadHelper(kusto_cluster, client_id, client_secret, username, password, authority)

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
        if query.startswith('.'):
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
        query_endpoint = '{0}/v2/rest/query'.format(self.kusto_cluster)
        return self._execute(kusto_database, query, query_endpoint, accept_partial_results, timeout)

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
        query_endpoint = '{0}/v1/rest/mgmt'.format(self.kusto_cluster)
        return self._execute(kusto_database, query, query_endpoint, accept_partial_results, timeout)

    def _execute(self, kusto_database, kusto_query, query_endpoint, accept_partial_results=False, timeout=None):
        """ Executes given query against this client """

        request_payload = {
            'db': kusto_database,
            'csl': kusto_query
        }

        access_token = self._aad_helper.acquire_token()
        request_headers = {
            'Authorization': 'Bearer {0}'.format(access_token),
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip,deflate',
            'Content-Type': 'application/json; charset=utf-8',
            'Fed': 'True',
            'x-ms-client-version': 'Kusto.Python.Client:' + VERSION,
            'x-ms-client-request-id': 'KPC.execute;' + str(uuid.uuid4()),
        }

        response = requests.post(
            query_endpoint,
            headers=request_headers,
            json=request_payload,
            timeout=timeout
        )

        if response.status_code == 200:
            kusto_response = KustoResponse(response.json())
            if kusto_response.has_exceptions() and not accept_partial_results:
                raise KustoServiceError(kusto_response.get_exceptions(), response, kusto_response)
            return kusto_response
        else:
            raise KustoServiceError([response.json(),], response)

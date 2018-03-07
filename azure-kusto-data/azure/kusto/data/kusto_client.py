"""
This module constains all classes to get Kusto responses.
Including error handling.
"""

from datetime import timedelta, datetime
import re

import webbrowser
import json
import adal
import dateutil.parser
import requests
import pandas

from .kusto_exceptions import KustoServiceError
from .version import VERSION

# Regex for TimeSpan
TIMESPAN_PATTERN = re.compile(r'((?P<d>[0-9]*).)?(?P<h>[0-9]{2}):(?P<m>[0-9]{2}):(?P<s>[0-9]{2})(.(?P<ms>[0-9]*))?')

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
        if isinstance(key, int):
            val = dict.__getitem__(self, self.index2column_mapping[key])
        else:
            val = dict.__getitem__(self, key)
        return val


class KustoResultIter(object):
    """ Iterator over returned rows """
    def __init__(self, json_result):
        self.json_result = json_result
        self.index2column_mapping = []
        self.index2type_mapping = []
        for column in json_result['Columns']:
            self.index2column_mapping.append(column['ColumnName'])
            self.index2type_mapping.append(column['DataType'])
        self.next = 0
        self.last = len(json_result['Rows'])
        # Here we keep converter functions for each type that we need to take special care
        # (e.g. convert)
        self.converters_lambda_mappings = {'DateTime': self.to_datetime, 'TimeSpan': self.to_timedelta}

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
        match = TIMESPAN_PATTERN.match(value)
        if match:
            return timedelta(
                days=int(match.group('d') or 0),
                hours=int(match.group('h')),
                minutes=int(match.group('m')),
                seconds=int(match.group('s')),
                milliseconds=int(match.group('ms') or 0))
        else:
            raise ValueError('Timespan value \'{}\' cannot be decoded'.format(value))

    def __iter__(self):
        return self

    def next(self):
        """ Gets the next value """
        return self.__next__()

    def __next__(self):
        if self.next >= self.last:
            raise StopIteration
        else:
            row = self.json_result['Rows'][self.next]
            result_dict = {}
            for index, value in enumerate(row):
                data_type = self.index2type_mapping[index]
                if data_type in self.converters_lambda_mappings:
                    result_dict[self.index2column_mapping[index]] = self.converters_lambda_mappings[data_type](value)
                else:
                    result_dict[self.index2column_mapping[index]] = value
            self.next = self.next + 1
            return KustoResult(self.index2column_mapping, result_dict)


class KustoResponse(object):
    """ Wrapper for response """
    # TODO: add support to get additional infromation from response, like execution time

    def __init__(self, json_response):
        self.json_response = json_response

    def get_raw_response(self):
        """ Gets the json response got from Kusto """
        return self.json_response

    def get_table_count(self):
        """ Gets the tables Count. """
        return len(self.json_response['Tables'])

    def has_exceptions(self):
        """ Checkes whether an exception was thrown. """
        return 'Exceptions' in self.json_response

    def get_exceptions(self):
        """ Gets the excpetions got from Kusto if exists. """
        if 'Exceptions' in self.json_response:
            return self.json_response['Exceptions']
        return None

    def iter_all(self, table_id=0):
        """ Returns iterator to get rows from response """
        return KustoResultIter(self.json_response['Tables'][table_id])

    _kusto_to_data_frame_data_types = {
        'DateTime' : 'datetime64',
        'Int32' : 'int32',
        'Int64' : 'int64',
        'Double' : 'float64',
        'String' : 'object',
        'SByte' : 'object',
        'Guid' : 'object',
        'TimeSpan' : 'object',
    }

    def to_dataframe(self):
        """ Returns Pandas data frame. """
        if not self.json_response:
            return pandas.DataFrame()
        rows_data = self.json_response["Tables"][0]["Rows"]
        kusto_columns = self.json_response["Tables"][0]["Columns"]

        col_names = [col["ColumnName"] for col in kusto_columns]
        frame = pandas.DataFrame(rows_data, columns=col_names)

        for col in kusto_columns:
            col_name = col["ColumnName"]
            if col["ColumnType"].lower() == "timespan":
                frame[col_name] = pandas.to_timedelta(frame[col_name])
            else:
                frame[col_name] = frame[col_name].astype(self._kusto_to_data_frame_data_types[col["DataType"]])

            if col["ColumnType"].lower() == "dynamic":
                frame[col_name] = frame[col_name].apply(json.loads)
        return frame

class KustoClient(object):
    """
    Kusto client for Python.

    KustoClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    KustoClient takes care of ADAL authentication, parsing response and giving you typed result set.

    Test are run using nose.

    Examples
    --------
    To use KustoClient, you can choose between three ways of authentication.

    For the first option, you'll need to have your own AAD application and know your client credentials (client_id and client_secret).
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster, client_id='your_app_id', client_secret='your_app_secret')

    For the second option, you can use KustoClient's client id (set as a default in the constructor) and authenticate using your username and password.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster, username='your_username', password='your_password')

    For the third option, you can use KustoClient's client id (set as a default in the constructor) and authenticate using your username and an AAD pop up.
    >>> kusto_cluster = 'https://help.kusto.windows.net'
    >>> kusto_client = KustoClient(kusto_cluster)

    After connecting, use the kusto_client instance to execute a management command or a query:
    >>> kusto_database = 'Samples'
    >>> response = kusto_client.execute_query(kusto_database, 'StormEvents | take 10')
    You can access rows now by index or by key.
    >>> for row in response.iter_all():
    >>>    print(row[0])
    >>>    print(row["ColumnName"])    """

    def __init__(self, kusto_cluster, client_id=None, client_secret=None, username=None, password=None, version='v1', authority=None):
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
        version : 'v1', optional
            REST API version, defaults to v1.
        """
        self.kusto_cluster = kusto_cluster
        self.version = version
        self.adal_context = adal.AuthenticationContext('https://login.windows.net/{0}/'.format(authority or 'microsoft.com'))
        self.client_id = client_id or "ad30ae9e-ac1b-4249-8817-d24f5d7ad3de"
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.request_headers = None

    def execute(self, kusto_database, query, accept_partial_results=False):
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
        """
        if query.startswith('.'):
            return self.execute_mgmt(kusto_database, query, accept_partial_results)
        return self.execute_query(kusto_database, query, accept_partial_results)

    def execute_query(self, kusto_database, query, accept_partial_results=False):
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
        """
        query_endpoint = '{0}/{1}/rest/query'.format(self.kusto_cluster, self.version)
        return self._execute(kusto_database, query, query_endpoint, accept_partial_results)

    def execute_mgmt(self, kusto_database, query, accept_partial_results=False):
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
        """
        query_endpoint = '{0}/{1}/rest/mgmt'.format(self.kusto_cluster, self.version)
        return self._execute(kusto_database, query, query_endpoint, accept_partial_results)

    def _execute(self, kusto_database, kusto_query, query_endpoint, accept_partial_results=False):
        """ Executes given query against this client """

        request_payload = {
            'db': kusto_database,
            'csl': kusto_query
        }

        access_token = self._acquire_token()
        self.request_headers = {
            'Authorization': 'Bearer {0}'.format(access_token),
            'Content-Type': 'application/json',
            'Accept-Encoding': 'gzip,deflate',
            'Fed': 'True',
            'x-ms-client-version':'Kusto.Python.Client:' + VERSION,
        }

        response = requests.post(
            query_endpoint,
            headers=self.request_headers,
            json=request_payload
        )

        if response.status_code == 200:
            kusto_response = KustoResponse(response.json())
            if kusto_response.has_exceptions() and not accept_partial_results:
                raise KustoServiceError(kusto_response.get_exceptions(), response, kusto_response)
            return kusto_response
        else:
            raise KustoServiceError([response.text,], response)

    def _acquire_token(self):
        token_response = self.adal_context.acquire_token(self.kusto_cluster, self.username, self.client_id)
        if token_response is not None:
            expiration_date = dateutil.parser.parse(token_response['expiresOn'])
            if expiration_date > datetime.utcnow() + timedelta(minutes=5):
                return token_response['accessToken']

        if self.client_secret is not None and self.client_id is not None:
            token_response = self.adal_context.acquire_token_with_client_credentials(
                self.kusto_cluster,
                self.client_id,
                self.client_secret)
        elif self.username is not None and self.password is not None:
            token_response = self.adal_context.acquire_token_with_username_password(
                self.kusto_cluster,
                self.username,
                self.password,
                self.client_id)
        else:
            code = self.adal_context.acquire_user_code(self.kusto_cluster, self.client_id)
            print(code['message'])
            webbrowser.open(code['verification_url'])
            token_response = self.adal_context.acquire_token_with_device_code(self.kusto_cluster, code, self.client_id)

        return token_response['accessToken']

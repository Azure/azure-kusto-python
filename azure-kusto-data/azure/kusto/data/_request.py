import requests
import uuid

from .aad_helper import _AadHelper
from .exceptions import KustoServiceError
from ._response import _KustoResponseDataSetV1, _KustoResponseDataSetV2
from ._version import VERSION


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

    def execute(
        self,
        kusto_database,
        query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
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
        get_raw_response : bool, optional
            Optional parameter. Whether to get a raw response, or a parsed one.
        """
        if query.startswith("."):
            return self.execute_mgmt(
                kusto_database, query, accept_partial_results, timeout, get_raw_response
            )
        return self.execute_query(
            kusto_database, query, accept_partial_results, timeout, get_raw_response
        )

    def execute_query(
        self,
        kusto_database,
        query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
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
        get_raw_response : bool, optional
            Optional parameter. Whether to get a raw response, or a parsed one.
        """
        return self._execute(
            self._query_endpoint,
            kusto_database,
            query,
            accept_partial_results,
            timeout,
            get_raw_response,
        )

    def execute_mgmt(
        self,
        kusto_database,
        query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
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
        get_raw_response : bool, optional
            Optional parameter. Whether to get a raw response, or a parsed one.
        """
        return self._execute(
            self._mgmt_endpoint,
            kusto_database,
            query,
            accept_partial_results,
            timeout,
            get_raw_response,
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

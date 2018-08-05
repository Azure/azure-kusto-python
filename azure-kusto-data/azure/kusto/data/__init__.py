import uuid
import requests
from enum import Enum, unique

from six import text_type

from ._response import _KustoResponse
from .exceptions import KustoServiceError as _KustoServiceError
from .security import _AadHelper
from .version import VERSION

__version__ = VERSION


class KustoConnectionStringBuilder(object):
    """Parses Kusto conenction strings.

    For usages, check out the sample at:
        https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py
    """

    @unique
    class _ValidKeywords(Enum):
        data_source = "Data Source"
        aad_user_id = "AAD User ID"
        password = "Password"
        application_client_id = "Application Client Id"
        application_key = "Application Key"
        authority_id = "Authority Id"

    _Keywords = {
        "Data Source": _ValidKeywords.data_source,
        "Addr": _ValidKeywords.data_source,
        "Address": _ValidKeywords.data_source,
        "Network Address": _ValidKeywords.data_source,
        "Server": _ValidKeywords.data_source,
        "AAD User ID": _ValidKeywords.aad_user_id,
        "Password": _ValidKeywords.password,
        "Pwd": _ValidKeywords.password,
        "Application Client Id": _ValidKeywords.application_client_id,
        "AppClientId": _ValidKeywords.application_client_id,
        "Application Key": _ValidKeywords.application_key,
        "AppKey": _ValidKeywords.application_key,
        "Authority Id": _ValidKeywords.authority_id,
        "AuthorityId": _ValidKeywords.authority_id,
        "Authority": _ValidKeywords.authority_id,
        "TenantId": _ValidKeywords.authority_id,
        "Tenant": _ValidKeywords.authority_id,
        "tid": _ValidKeywords.authority_id,
    }

    def __init__(self, connection_string):
        """Creates new KustoConnectionStringBuilder.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net;AAD User ID="user@microsoft.com";Password=P@ssWord
        For more information please look at:
        https://kusto.azurewebsites.net/docs/concepts/kusto_connection_strings.html
        """
        self._internal_dict = {}
        if connection_string is not None and "=" not in connection_string.partition(";")[0]:
            connection_string = "Data Source=" + connection_string
        for kvp_string in connection_string.split(";"):
            kvp = kvp_string.split("=")
            self[kvp[0]] = kvp[1]

    def __setitem__(self, key, value):
        try:
            keyword = self._Keywords[key]
        except ValueError:
            raise ValueError("%s is not supported as an item in KustoConnectionStringBuilder" % key)

        self._internal_dict[keyword] = value

    @classmethod
    def with_aad_user_password_authentication(cls, connection_string, user_id, password):
        """Creates a KustoConnection string builder that will authenticate with AAD user name and password
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str user_id: AAD user ID.
        :param str password: Corresponding password of the AAD user.
        """
        _ensure_value_is_valid(connection_string)
        _ensure_value_is_valid(user_id)
        _ensure_value_is_valid(password)
        kcsb = cls(connection_string)
        kcsb[kcsb._ValidKeywords.aad_user_id] = user_id
        kcsb[kcsb._ValidKeywords.password] = password
        return kcsb

    @classmethod
    def with_aad_application_key_authentication(
        cls, connection_string, application_client_id, application_key
    ):
        """Creates a KustoConnection string builder that will authenticate with AAD application and password
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str user_id: AAD application ID.
        :param str password: Corresponding password of the AAD application.
        """
        _ensure_value_is_valid(connection_string)
        _ensure_value_is_valid(application_client_id)
        _ensure_value_is_valid(application_key)
        kcsb = cls(connection_string)
        kcsb[kcsb._ValidKeywords.application_client_id] = application_client_id
        kcsb[kcsb._ValidKeywords.application_key] = application_key
        return kcsb

    @classmethod
    def with_aad_device_authentication(cls, connection_string):
        """Creates a KustoConnection string builder that will authenticate with AAD application and password
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        """
        _ensure_value_is_valid(connection_string)
        return cls(connection_string)

    @property
    def data_source(self):
        if self._ValidKeywords.data_source in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.data_source]

    @property
    def aad_user_id(self):
        if self._ValidKeywords.aad_user_id in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.aad_user_id]

    @property
    def password(self):
        if self._ValidKeywords.password in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.password]

    @property
    def application_client_id(self):
        if self._ValidKeywords.application_client_id in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.application_client_id]

    @property
    def application_key(self):
        if self._ValidKeywords.application_key in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.application_key]

    @property
    def authority_id(self):
        if self._ValidKeywords.authority_id in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.authority_id]

    @authority_id.setter
    def authority_id(self, value):
        self[self._ValidKeywords.authority_id] = value


def _ensure_value_is_valid(value):
    if not value or not value.strip():
        raise ValueError


class KustoClient(object):
    """Kusto client for Python.

    KustoClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    KustoClient takes care of ADAL authentication, parsing response and giving you typed result set.

    Test are run using pytest.
    """

    def __init__(self, kcsb):
        """Kusto Client constructor.
        :param KustoConnectionStringBuilder kcsb: The connection string to initialize KustoClient.
        """
        self.kusto_cluster = kcsb.data_source
        self._aad_helper = _AadHelper(kcsb)

    def execute(self, kusto_database, query, accept_partial_results=False, timeout=None):
        """Executes a query or management command.
        :param str kusto_database: Database against query will be executed.
        :param str query: Query to be executed.
        :param bool accept_partial_results: Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        :param float timeout: Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        if query.startswith("."):
            return self.execute_mgmt(kusto_database, query, accept_partial_results, timeout)
        return self.execute_query(kusto_database, query, accept_partial_results, timeout)

    def execute_query(self, kusto_database, query, accept_partial_results=False, timeout=None):
        """Executes a query.
        :param str kusto_database: Database against query will be executed.
        :param str query: Query to be executed.
        :param bool accept_partial_results: Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        :param float timeout: Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        query_endpoint = "{0}/v2/rest/query".format(self.kusto_cluster)
        return self._execute(kusto_database, query, query_endpoint, accept_partial_results, timeout)

    def execute_mgmt(self, kusto_database, query, accept_partial_results=False, timeout=None):
        """Executes a management command.
        :param str kusto_database: Database against query will be executed.
        :param str query: Query to be executed.
        :param bool accept_partial_results: Optional parameter. If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        :param float timeout: Optional parameter. Network timeout in seconds. Default is no timeout.
        """
        query_endpoint = "{0}/v1/rest/mgmt".format(self.kusto_cluster)
        return self._execute(kusto_database, query, query_endpoint, accept_partial_results, timeout)

    def _execute(
        self,
        kusto_database,
        kusto_query,
        query_endpoint,
        accept_partial_results=False,
        timeout=None,
    ):

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
            query_endpoint, headers=request_headers, json=request_payload, timeout=timeout
        )

        if response.status_code == 200:
            kusto_response = _KustoResponse(response.json())
            if kusto_response.has_exceptions() and not accept_partial_results:
                raise _KustoServiceError(kusto_response.get_exceptions(), response, kusto_response)
            return kusto_response
        else:
            raise _KustoServiceError([response.json()], response)

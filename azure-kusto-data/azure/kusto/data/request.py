"""A module to make a Kusto request."""

import uuid
from enum import Enum, unique
import requests

from .security import _AadHelper
from .exceptions import KustoServiceError
from ._response import _KustoResponseDataSetV1, _KustoResponseDataSetV2
from ._version import VERSION


class KustoConnectionStringBuilder(object):
    """Parses Kusto conenction strings.
    For usages, check out the sample at:
        https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py
    """

    @unique
    class ValidKeywords(Enum):
        data_source = "Data Source"
        aad_user_id = "AAD User ID"
        password = "Password"
        application_client_id = "Application Client Id"
        application_key = "Application Key"
        authority_id = "Authority Id"

    _Keywords = {
        "Data Source": ValidKeywords.data_source,
        "Addr": ValidKeywords.data_source,
        "Address": ValidKeywords.data_source,
        "Network Address": ValidKeywords.data_source,
        "Server": ValidKeywords.data_source,
        "AAD User ID": ValidKeywords.aad_user_id,
        "Password": ValidKeywords.password,
        "Pwd": ValidKeywords.password,
        "Application Client Id": ValidKeywords.application_client_id,
        "AppClientId": ValidKeywords.application_client_id,
        "Application Key": ValidKeywords.application_key,
        "AppKey": ValidKeywords.application_key,
        "Authority Id": ValidKeywords.authority_id,
        "AuthorityId": ValidKeywords.authority_id,
        "Authority": ValidKeywords.authority_id,
        "TenantId": ValidKeywords.authority_id,
        "Tenant": ValidKeywords.authority_id,
        "tid": ValidKeywords.authority_id,
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
            keyword = key if isinstance(key, self.ValidKeywords) else self._Keywords[key.strip()]
        except KeyError:
            raise KeyError("%s is not supported as an item in KustoConnectionStringBuilder" % key)

        self._internal_dict[keyword] = value.strip()

    @classmethod
    def with_aad_user_password_authentication(cls, connection_string, user_id, password):
        """Creates a KustoConnection string builder that will authenticate with AAD user name and
        password.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str user_id: AAD user ID.
        :param str password: Corresponding password of the AAD user.
        """
        _ensure_value_is_valid(connection_string)
        _ensure_value_is_valid(user_id)
        _ensure_value_is_valid(password)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_user_id] = user_id
        kcsb[kcsb.ValidKeywords.password] = password
        return kcsb

    @classmethod
    def with_aad_application_key_authentication(cls, connection_string, aad_app_id, app_key):
        """Creates a KustoConnection string builder that will authenticate with AAD application and
        password.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str aad_app_id: AAD application ID.
        :param str app_key: Corresponding password of the AAD application.
        """
        _ensure_value_is_valid(connection_string)
        _ensure_value_is_valid(aad_app_id)
        _ensure_value_is_valid(app_key)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.application_client_id] = aad_app_id
        kcsb[kcsb.ValidKeywords.application_key] = app_key
        return kcsb

    @classmethod
    def with_aad_device_authentication(cls, connection_string):
        """Creates a KustoConnection string builder that will authenticate with AAD application and
        password.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        """
        _ensure_value_is_valid(connection_string)
        return cls(connection_string)

    @property
    def data_source(self):
        """The URI specifying the Kusto service endpoint.
        For example, https://kuskus.kusto.windows.net or net.tcp://localhost
        """
        if self.ValidKeywords.data_source in self._internal_dict:
            return self._internal_dict[self.ValidKeywords.data_source]
        return None

    @property
    def aad_user_id(self):
        """The username to use for AAD Federated AuthN."""
        if self.ValidKeywords.aad_user_id in self._internal_dict:
            return self._internal_dict[self.ValidKeywords.aad_user_id]
        return None

    @property
    def password(self):
        """The password to use for authentication when username/password authentication is used.
        Must be accompanied by UserID property
        """
        if self.ValidKeywords.password in self._internal_dict:
            return self._internal_dict[self.ValidKeywords.password]
        return None

    @property
    def application_client_id(self):
        """The application client id to use for authentication when federated
        authentication is used.
        """
        if self.ValidKeywords.application_client_id in self._internal_dict:
            return self._internal_dict[self.ValidKeywords.application_client_id]
        return None

    @property
    def application_key(self):
        """The application key to use for authentication when federated authentication is used"""
        if self.ValidKeywords.application_key in self._internal_dict:
            return self._internal_dict[self.ValidKeywords.application_key]
        return None

    @property
    def authority_id(self):
        """The ID of the AAD tenant where the application is configured.
        (should be supplied only for non-Microsoft tenant)"""
        if self.ValidKeywords.authority_id in self._internal_dict:
            return self._internal_dict[self.ValidKeywords.authority_id]
        return None

    @authority_id.setter
    def authority_id(self, value):
        self[self.ValidKeywords.authority_id] = value


def _ensure_value_is_valid(value):
    if not value or not value.strip():
        raise ValueError


class KustoClient(object):
    """Kusto client for Python.
    KustoClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    KustoClient takes care of ADAL authentication, parsing response and giving you typed result set.

    Tests are run using pytest.
    """

    def __init__(self, kcsb):
        """Kusto Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        """
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)
        kusto_cluster = kcsb.data_source
        self._mgmt_endpoint = "{0}/v1/rest/mgmt".format(kusto_cluster)
        self._query_endpoint = "{0}/v2/rest/query".format(kusto_cluster)
        self._aad_helper = _AadHelper(kcsb)

    def execute(
        self,
        kusto_database,
        query,
        accept_partial_results=False,
        timeout=None,
        get_raw_response=False,
    ):
        """Executes a query or management command.
        :param str kusto_database: Database against query will be executed.
        :param str query: Query to be executed.
        :param bool accept_partial_results: Optional parameter.
            If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        :param float timeout: Optional parameter. Network timeout in seconds. Default is no timeout.
        :param bool get_raw_response: Optional parameter. Whether to get a raw response, or a parsed one.
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
        """Executes a query.
        :param str kusto_database: Database against query will be executed.
        :param str query: Query to be executed.
        :param bool accept_partial_results: Optional parameter.
            If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        :param float timeout: Optional parameter. Network timeout in seconds. Default is no timeout.
        :param bool get_raw_response: Optional parameter.
            Whether to get a raw response, or a parsed one.
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
        """Executes a management command.
        :param str kusto_database: Database against query will be executed.
        :param str query: Query to be executed.
        :param bool accept_partial_results: Optional parameter.
            If query fails, but we receive some results, we consider results as partial.
            If this is True, results are returned to client, even if there are exceptions.
            If this is False, exception is raised. Default is False.
        :param float timeout: Optional parameter. Network timeout in seconds. Default is no timeout.
        :param bool get_raw_response: Optional parameter.
            Whether to get a raw response, or a parsed one.
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

"""A module to make a Kusto request."""

import uuid
from enum import Enum, unique
import requests

from .security import _AadHelper
from .exceptions import KustoServiceError
from ._response import KustoResponseDataSetV1, KustoResponseDataSetV2
from ._version import VERSION


class KustoConnectionStringBuilder(object):
    """Parses Kusto conenction strings.
    For usages, check out the sample at:
        https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py
    """

    @unique
    class ValidKeywords(Enum):
        """Distinct set of values KustoConnectionStringBuilder can have."""

        data_source = "Data Source"
        aad_user_id = "AAD User ID"
        password = "Password"
        application_client_id = "Application Client Id"
        application_key = "Application Key"
        application_certificate = "Application Certificate"
        application_certificate_thumbprint = "Application Certificate Thumbprint"
        authority_id = "Authority Id"

        @classmethod
        def parse(cls, key):
            """Create a valid keyword."""
            key = key.lower().strip()
            if key in ["data source", "addr", "address", "network address", "server"]:
                return cls.data_source
            if key in ["aad user id"]:
                return cls.aad_user_id
            if key in ["password", "pwd"]:
                return cls.password
            if key in ["application client id", "appclientid"]:
                return cls.application_client_id
            if key in ["application key", "appkey"]:
                return cls.application_key
            if key in ["application certificate"]:
                return cls.application_certificate
            if key in ["application certificate thumbprint"]:
                return cls.application_certificate_thumbprint
            if key in ["authority id", "authorityid", "authority", "tenantid", "tenant", "tid"]:
                return cls.authority_id
            raise KeyError(key)

    def __init__(self, connection_string):
        """Creates new KustoConnectionStringBuilder.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net;AAD User ID="user@microsoft.com";Password=P@ssWord
        For more information please look at:
        https://kusto.azurewebsites.net/docs/concepts/kusto_connection_strings.html
        """
        _assert_value_is_valid(connection_string)
        self._internal_dict = {}
        if connection_string is not None and "=" not in connection_string.partition(";")[0]:
            connection_string = "Data Source=" + connection_string

        self[self.ValidKeywords.authority_id] = "common"

        for kvp_string in connection_string.split(";"):
            key, _, value = kvp_string.partition("=")
            self[key] = value

    def __setitem__(self, key, value):
        try:
            keyword = key if isinstance(key, self.ValidKeywords) else self.ValidKeywords.parse(key)
        except KeyError:
            raise KeyError("%s is not supported as an item in KustoConnectionStringBuilder" % key)

        self._internal_dict[keyword] = value.strip()

    @classmethod
    def with_aad_user_password_authentication(cls, connection_string, user_id, password, authority_id="common"):
        """Creates a KustoConnection string builder that will authenticate with AAD user name and
        password.
        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        :param str user_id: AAD user ID.
        :param str password: Corresponding password of the AAD user.
        :param str authority_id: optional param. defaults to "common"
        """
        _assert_value_is_valid(user_id)
        _assert_value_is_valid(password)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_user_id] = user_id
        kcsb[kcsb.ValidKeywords.password] = password
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @classmethod
    def with_aad_application_key_authentication(cls, connection_string, aad_app_id, app_key, authority_id="common"):
        """Creates a KustoConnection string builder that will authenticate with AAD application and
        password.
        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        :param str aad_app_id: AAD application ID.
        :param str app_key: Corresponding password of the AAD application.
        :param str authority_id: optional param. defaults to "common"
        """
        _assert_value_is_valid(aad_app_id)
        _assert_value_is_valid(app_key)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.application_client_id] = aad_app_id
        kcsb[kcsb.ValidKeywords.application_key] = app_key
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @classmethod
    def with_aad_application_certificate_authentication(
        cls, connection_string, aad_app_id, certificate, thumbprint, authority_id="common"
    ):
        """Creates a KustoConnection string builder that will authenticate with AAD application and
        a certificate credentials.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str aad_app_id: AAD application ID.
        :param str certificate: A PEM encoded certificate private key.
        :param str thumbprint: hex encoded thumbprint of the certificate.
        :param str authority_id: optional param. defaults to "common"
        """
        _assert_value_is_valid(aad_app_id)
        _assert_value_is_valid(certificate)
        _assert_value_is_valid(thumbprint)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.application_client_id] = aad_app_id
        kcsb[kcsb.ValidKeywords.application_certificate] = certificate
        kcsb[kcsb.ValidKeywords.application_certificate_thumbprint] = thumbprint
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @classmethod
    def with_aad_device_authentication(cls, connection_string, authority_id="common"):
        """Creates a KustoConnection string builder that will authenticate with AAD application and
        password.
        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        :param str authority_id: optional param. defaults to "common"
        """
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @property
    def data_source(self):
        """The URI specifying the Kusto service endpoint.
        For example, https://kuskus.kusto.windows.net or net.tcp://localhost
        """
        return self._internal_dict.get(self.ValidKeywords.data_source)

    @property
    def aad_user_id(self):
        """The username to use for AAD Federated AuthN."""
        return self._internal_dict.get(self.ValidKeywords.aad_user_id)

    @property
    def password(self):
        """The password to use for authentication when username/password authentication is used.
        Must be accompanied by UserID property
        """
        return self._internal_dict.get(self.ValidKeywords.password)

    @property
    def application_client_id(self):
        """The application client id to use for authentication when federated
        authentication is used.
        """
        return self._internal_dict.get(self.ValidKeywords.application_client_id)

    @property
    def application_key(self):
        """The application key to use for authentication when federated authentication is used"""
        return self._internal_dict.get(self.ValidKeywords.application_key)

    @property
    def application_certificate(self):
        """A PEM encoded certificate private key."""
        return self._internal_dict.get(self.ValidKeywords.application_certificate)

    @application_certificate.setter
    def application_certificate(self, value):
        self[self.ValidKeywords.application_certificate] = value

    @property
    def application_certificate_thumbprint(self):
        """hex encoded thumbprint of the certificate."""
        return self._internal_dict.get(self.ValidKeywords.application_certificate_thumbprint)

    @application_certificate_thumbprint.setter
    def application_certificate_thumbprint(self, value):
        self[self.ValidKeywords.application_certificate_thumbprint] = value

    @property
    def authority_id(self):
        """The ID of the AAD tenant where the application is configured.
        (should be supplied only for non-Microsoft tenant)"""
        return self._internal_dict.get(self.ValidKeywords.authority_id)

    @authority_id.setter
    def authority_id(self, value):
        self[self.ValidKeywords.authority_id] = value


def _assert_value_is_valid(value):
    if not value or not value.strip():
        raise ValueError("Should not be empty")


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

    def execute(self, kusto_database, query, accept_partial_results=False, timeout=None, get_raw_response=False):
        """Executes a query or management command.
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
        if query.startswith("."):
            return self.execute_mgmt(kusto_database, query, accept_partial_results, timeout, get_raw_response)
        return self.execute_query(kusto_database, query, accept_partial_results, timeout, get_raw_response)

    def execute_query(self, kusto_database, query, accept_partial_results=False, timeout=None, get_raw_response=False):
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
            self._query_endpoint, kusto_database, query, accept_partial_results, timeout, get_raw_response
        )

    def execute_mgmt(self, kusto_database, query, accept_partial_results=False, timeout=None, get_raw_response=False):
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
            self._mgmt_endpoint, kusto_database, query, accept_partial_results, timeout, get_raw_response
        )

    def _execute(
        self, endpoint, kusto_database, kusto_query, accept_partial_results=False, timeout=None, get_raw_response=False
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

        response = requests.post(endpoint, headers=request_headers, json=request_payload, timeout=timeout)

        if response.status_code == 200:
            if get_raw_response:
                return response.json()

            if endpoint.endswith("v2/rest/query"):
                kusto_response = KustoResponseDataSetV2(response.json())
            else:
                kusto_response = KustoResponseDataSetV1(response.json())

            if kusto_response.errors_count > 0 and not accept_partial_results:
                raise KustoServiceError(kusto_response.get_exceptions(), response, kusto_response)
            return kusto_response
        else:
            raise KustoServiceError([response.json()], response)

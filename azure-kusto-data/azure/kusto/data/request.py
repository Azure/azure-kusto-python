"""A module to make a Kusto request."""

import uuid
import json
import urllib3
import certifi

from datetime import timedelta
from enum import Enum, unique

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
        aad_federated_security = "AAD Federated Security"
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
            if key in ["aad federated security", "federated security", "federated", "fed", "aadfed"]:
                return cls.aad_federated_security
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

        self._internal_dict[keyword] = value if keyword is self.ValidKeywords.aad_federated_security else value.strip()

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
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.aad_user_id] = user_id
        kcsb[kcsb.ValidKeywords.password] = password
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @classmethod
    def with_aad_application_key_authentication(cls, connection_string, aad_app_id, app_key, authority_id):
        """Creates a KustoConnection string builder that will authenticate with AAD application and key.
        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        :param str aad_app_id: AAD application ID.
        :param str app_key: Corresponding key of the AAD application.
        :param str authority_id: Authority id (aka Tenant id) must be provided
        """
        _assert_value_is_valid(aad_app_id)
        _assert_value_is_valid(app_key)
        _assert_value_is_valid(authority_id)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.application_client_id] = aad_app_id
        kcsb[kcsb.ValidKeywords.application_key] = app_key
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @classmethod
    def with_aad_application_certificate_authentication(
        cls, connection_string, aad_app_id, certificate, thumbprint, authority_id
    ):
        """Creates a KustoConnection string builder that will authenticate with AAD application and
        a certificate credentials.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str aad_app_id: AAD application ID.
        :param str certificate: A PEM encoded certificate private key.
        :param str thumbprint: hex encoded thumbprint of the certificate.
        :param str authority_id: Authority id (aka Tenant id) must be provided
        """
        _assert_value_is_valid(aad_app_id)
        _assert_value_is_valid(certificate)
        _assert_value_is_valid(thumbprint)
        _assert_value_is_valid(authority_id)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
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
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
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

    @property
    def aad_federated_security(self):
        """A Boolean value that instructs the client to perform AAD federated authentication."""
        return self._internal_dict.get(self.ValidKeywords.aad_federated_security)


def _assert_value_is_valid(value):
    if not value or not value.strip():
        raise ValueError("Should not be empty")


class KustoClient(object):
    """Kusto client for Python.
    KustoClient works with both 2.x and 3.x flavors of Python. All primitive types are supported.
    KustoClient takes care of ADAL authentication, parsing response and giving you typed result set.

    Tests are run using pytest.
    """

    _mgmt_default_timeout = timedelta(hours=1, seconds=30).seconds
    _query_default_timeout = timedelta(minutes=4, seconds=30).seconds

    # The maximum amount of connections to be able to operate in parallel
    _max_pool_size = 100

    def __init__(self, kcsb):
        """Kusto Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        :type kcsb: azure.kusto.data.request.KustoConnectionStringBuilder or str
        """
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)
        kusto_cluster = kcsb.data_source

        # Create a pool manager
        self._pool_mgr = urllib3.PoolManager(
            num_pools=1, maxsize=self._max_pool_size, cert_reqs="CERT_REQUIRED", ca_certs=certifi.where()
        )

        self._mgmt_endpoint = "{0}/v1/rest/mgmt".format(kusto_cluster)
        self._query_endpoint = "{0}/v2/rest/query".format(kusto_cluster)
        self._auth_provider = _AadHelper(kcsb) if kcsb.aad_federated_security else None

    def execute(self, database, query, properties=None):
        """Executes a query or management command.
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.request.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data._response.KustoResponseDataSet
        """
        if query.startswith("."):
            return self.execute_mgmt(database, query, properties)
        return self.execute_query(database, query, properties)

    def execute_query(self, database, query, properties=None):
        """Executes a query.
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.request.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data._response.KustoResponseDataSet
        """
        return self._execute(self._query_endpoint, database, query, KustoClient._query_default_timeout, properties)

    def execute_mgmt(self, database, query, properties=None):
        """Executes a management command.
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.request.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data._response.KustoResponseDataSet
        """
        return self._execute(self._mgmt_endpoint, database, query, KustoClient._mgmt_default_timeout, properties)

    def _execute(self, endpoint, database, query, default_timeout, properties=None):
        """Executes given query against this client"""

        request_payload = {"db": database, "csl": query}
        if properties:
            request_payload["properties"] = properties.to_json()

        request_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip,deflate",
            "Content-Type": "application/json; charset=utf-8",
            "x-ms-client-version": "Kusto.Python.Client:" + VERSION,
            "x-ms-client-request-id": "KPC.execute;" + str(uuid.uuid4()),
        }

        if self._auth_provider:
            request_headers["Authorization"] = self._auth_provider.acquire_authorization_header()

        timeout = self._get_timeout(properties, default_timeout)
        response = self._pool_mgr.request(
            "POST", endpoint, headers=request_headers, body=json.dumps(request_payload), timeout=timeout
        )

        if response.status == 200:
            if endpoint.endswith("v2/rest/query"):
                return KustoResponseDataSetV2(json.loads(response.data))
            return KustoResponseDataSetV1(json.loads(response.data))

        raise KustoServiceError([json.loads(response.data)], response)

    def _get_timeout(self, properties, default):
        if properties:
            return properties.get_option(ClientRequestProperties.OptionServerTimeout, default)
        return default


class ClientRequestProperties(object):
    """This class is a POD used by client making requests to describe specific needs from the service executing the requests.
    For more information please look at: https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties
    Not all of the documented options are implemented. You are welcome to open an issue in case you need one of them.
    """

    OptionDeferPartialQueryFailures = "deferpartialqueryfailures"  # TODO: Rename: results_defer_partial_query_failures
    OptionServerTimeout = "servertimeout"  # TODO: Rename: request_timeout

    def __init__(self):
        self._options = {}

    def set_option(self, name, value):
        """Sets an option's value"""
        _assert_value_is_valid(name)
        self._options[name] = value

    def has_option(self, name):
        """Checks if an option is specified."""
        return name in self._options

    def get_option(self, name, default_value):
        """Gets an option's value."""
        return self._options.get(name, default_value)

    def to_json(self):
        """Safe serialization to a JSON string."""
        return json.dumps({"Options": self._options})

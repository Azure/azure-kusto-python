# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import io
import json
import uuid
from copy import copy
from datetime import timedelta
from enum import Enum, unique
from typing import Union, Callable

import requests
from requests.adapters import HTTPAdapter

from ._version import VERSION
from .data_format import DataFormat
from .exceptions import KustoServiceError
from .response import KustoResponseDataSetV1, KustoResponseDataSetV2, KustoResponseDataSet
from .security import _AadHelper


class KustoConnectionStringBuilder:
    """
    Parses Kusto connection strings.
    For usages, check out the sample at:
        https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py
    """

    @unique
    class ValidKeywords(Enum):
        """
        Set of properties that can be use in a connection string provided to KustoConnectionStringBuilder.
        For a complete list of properties go to https://docs.microsoft.com/en-us/azure/kusto/api/connection-strings/kusto
        """

        data_source = "Data Source"
        aad_federated_security = "AAD Federated Security"
        aad_user_id = "AAD User ID"
        password = "Password"
        application_client_id = "Application Client Id"
        application_key = "Application Key"
        application_certificate = "Application Certificate"
        application_certificate_thumbprint = "Application Certificate Thumbprint"
        public_application_certificate = "Public Application Certificate"
        authority_id = "Authority Id"
        application_token = "Application Token"
        user_token = "User Token"
        msi_auth = "MSI Authentication"
        msi_params = "MSI Params"
        az_cli = "AZ CLI"

        @classmethod
        def parse(cls, key: str) -> "ValidKeywords":
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
            if key in ["public application certificate"]:
                return cls.public_application_certificate
            if key in ["authority id", "authorityid", "authority", "tenantid", "tenant", "tid"]:
                return cls.authority_id
            if key in ["aad federated security", "federated security", "federated", "fed", "aadfed"]:
                return cls.aad_federated_security
            if key in ["application token", "apptoken"]:
                return cls.application_token
            if key in ["user token", "usertoken", "usrtoken"]:
                return cls.user_token
            if key in ["msi_auth"]:
                return cls.msi_auth
            if key in ["msi_type"]:
                return cls.msi_params
            raise KeyError(key)

        def is_secret(self) -> bool:
            """States for each property if it contains secret"""
            return self in [self.password, self.application_key, self.application_certificate, self.application_token, self.user_token]

        def is_str_type(self) -> bool:
            """States whether a word is of type str or not."""
            return self in [
                self.aad_user_id,
                self.application_certificate,
                self.application_certificate_thumbprint,
                self.public_application_certificate,
                self.application_client_id,
                self.data_source,
                self.password,
                self.application_key,
                self.authority_id,
                self.application_token,
                self.user_token,
            ]

        def is_dict_type(self) -> bool:
            return self in [self.msi_params]

        def is_bool_type(self) -> bool:
            """States whether a word is of type bool or not."""
            return self in [self.aad_federated_security, self.msi_auth, self.az_cli]

    def __init__(self, connection_string: str):
        """
        Creates new KustoConnectionStringBuilder.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net;AAD User ID="user@microsoft.com";Password=P@ssWord
        For more information please look at:
        https://kusto.azurewebsites.net/docs/concepts/kusto_connection_strings.html
        """
        _assert_value_is_valid(connection_string)
        self._internal_dict = {}
        self._token_provider = None
        if connection_string is not None and "=" not in connection_string.partition(";")[0]:
            connection_string = "Data Source=" + connection_string

        self[self.ValidKeywords.authority_id] = "common"

        for kvp_string in connection_string.split(";"):
            key, _, value = kvp_string.partition("=")
            keyword = self.ValidKeywords.parse(key)
            if keyword.is_str_type():
                self[keyword] = value.strip()
            if keyword.is_bool_type():
                if value.strip() in ["True", "true"]:
                    self[keyword] = True
                elif value.strip() in ["False", "false"]:
                    self[keyword] = False
                else:
                    raise KeyError("Expected aad federated security to be bool. Recieved %s" % value)

    def __setitem__(self, key, value):
        try:
            keyword = key if isinstance(key, self.ValidKeywords) else self.ValidKeywords.parse(key)
        except KeyError:
            raise KeyError("%s is not supported as an item in KustoConnectionStringBuilder" % key)

        if value is None:
            raise TypeError("Value cannot be None.")

        if keyword.is_str_type():
            self._internal_dict[keyword] = value.strip()
        elif keyword.is_bool_type():
            if not isinstance(value, bool):
                raise TypeError("Expected %s to be bool" % key)
            self._internal_dict[keyword] = value
        elif keyword.is_dict_type():
            if not isinstance(value, dict):
                raise TypeError("Expected %s to be dict" % key)
            self._internal_dict[keyword] = value
        else:
            raise KeyError("KustoConnectionStringBuilder supports only bools and strings.")

    @classmethod
    def with_aad_user_password_authentication(
        cls, connection_string: str, user_id: str, password: str, authority_id: str = "common"
    ) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD user name and
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
    def with_aad_user_token_authentication(cls, connection_string: str, user_token: str) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application and
        a certificate credentials.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str user_token: AAD user token.
        """
        _assert_value_is_valid(user_token)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.user_token] = user_token

        return kcsb

    @classmethod
    def with_aad_application_key_authentication(
        cls, connection_string: str, aad_app_id: str, app_key: str, authority_id: str
    ) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application and key.
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
        cls, connection_string: str, aad_app_id: str, certificate: str, thumbprint: str, authority_id: str
    ) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application using
        a certificate.
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
    def with_aad_application_certificate_sni_authentication(
        cls, connection_string: str, aad_app_id: str, private_certificate: str, public_certificate: str, thumbprint: str, authority_id: str
    ) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application using
        a certificate Subject Name and Issuer.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str aad_app_id: AAD application ID.
        :param str private_certificate: A PEM encoded certificate private key.
        :param str public_certificate: A public certificate matching the provided PEM certificate private key.
        :param str thumbprint: hex encoded thumbprint of the certificate.
        :param str authority_id: Authority id (aka Tenant id) must be provided
        """
        _assert_value_is_valid(aad_app_id)
        _assert_value_is_valid(private_certificate)
        _assert_value_is_valid(public_certificate)
        _assert_value_is_valid(thumbprint)
        _assert_value_is_valid(authority_id)

        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.application_client_id] = aad_app_id
        kcsb[kcsb.ValidKeywords.application_certificate] = private_certificate
        kcsb[kcsb.ValidKeywords.public_application_certificate] = public_certificate
        kcsb[kcsb.ValidKeywords.application_certificate_thumbprint] = thumbprint
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @classmethod
    def with_aad_application_token_authentication(cls, connection_string: str, application_token: str) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application and
        an application token.
        :param str connection_string: Kusto connection string should by of the format:
        https://<clusterName>.kusto.windows.net
        :param str application_token: AAD application token.
        """
        _assert_value_is_valid(application_token)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.application_token] = application_token

        return kcsb

    @classmethod
    def with_aad_device_authentication(cls, connection_string: str, authority_id: str = "common") -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application and
        password.
        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        :param str authority_id: optional param. defaults to "common"
        """
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.authority_id] = authority_id

        return kcsb

    @classmethod
    def with_az_cli_authentication(cls, connection_string: str) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will use existing authenticated az cli profile
        password.
        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        """
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.az_cli] = True
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True

        return kcsb

    @classmethod
    def with_aad_managed_service_identity_authentication(
        cls, connection_string: str, client_id: str = None, object_id: str = None, msi_res_id: str = None, timeout: int = None
    ) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application, using
        an application token obtained from a Microsoft Service Identity endpoint. An optional user
        assigned application ID can be added to the token.

        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        :param client_id: an optional user assigned identity provided as an Azure ID of a client
        :param object_id: an optional user assigned identity provided as an Azure ID of an object
        :param msi_res_id: an optional user assigned identity provided as an Azure ID of an MSI resource
        :param timeout: an optional timeout (seconds) to wait for an MSI Authentication to occur
        """

        kcsb = cls(connection_string)
        params = {}
        exclusive_pcount = 0

        if timeout is not None:
            params["connection_timeout"] = timeout

        if client_id is not None:
            params["client_id"] = client_id
            exclusive_pcount += 1

        if object_id is not None:
            # Until we upgrade azure-identity to version 1.4.1, only client_id is excepted as a hint for user managed service identity
            raise ValueError("User Managed Service Identity with object_id is temporarily not supported by azure identity 1.3.1. Please use client_id instead.")
            params["object_id"] = object_id
            exclusive_pcount += 1

        if msi_res_id is not None:
            # Until we upgrade azure-identity to version 1.4.1, only client_id is excepted as a hint for user managed service identity
            raise ValueError(
                "User Managed Service Identity with msi_res_id is temporarily not supported by azure identity 1.3.1. Please use client_id instead."
            )
            params["msi_res_id"] = msi_res_id
            exclusive_pcount += 1

        if exclusive_pcount > 1:
            raise ValueError("the following parameters are mutually exclusive and can not be provided at the same time: client_uid, object_id, msi_res_id")

        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.msi_auth] = True
        kcsb[kcsb.ValidKeywords.msi_params] = params

        return kcsb

    @classmethod
    def with_token_provider(cls, connection_string: str, token_provider: Callable[[], str]) -> "KustoConnectionStringBuilder":
        """
        Create a KustoConnectionStringBuilder that uses a callback function to obtain a connection token
        :param str connection_string: Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
        :param token_provider: a parameterless function that returns a valid bearer token for the relevant kusto resource as a string
        """

        assert callable(token_provider)

        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb._token_provider = token_provider

        return kcsb

    @property
    def data_source(self) -> str:
        """The URI specifying the Kusto service endpoint.
        For example, https://kuskus.kusto.windows.net or net.tcp://localhost
        """
        return self._internal_dict.get(self.ValidKeywords.data_source)

    @property
    def aad_user_id(self) -> str:
        """The username to use for AAD Federated AuthN."""
        return self._internal_dict.get(self.ValidKeywords.aad_user_id)

    @property
    def password(self) -> str:
        """The password to use for authentication when username/password authentication is used.
        Must be accompanied by UserID property
        """
        return self._internal_dict.get(self.ValidKeywords.password)

    @property
    def application_client_id(self) -> str:
        """The application client id to use for authentication when federated
        authentication is used.
        """
        return self._internal_dict.get(self.ValidKeywords.application_client_id)

    @property
    def application_key(self) -> str:
        """The application key to use for authentication when federated authentication is used"""
        return self._internal_dict.get(self.ValidKeywords.application_key)

    @property
    def application_certificate(self) -> str:
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
    def application_public_certificate(self) -> str:
        """A public certificate matching the PEM encoded certificate private key."""
        return self._internal_dict.get(self.ValidKeywords.public_application_certificate)

    @application_public_certificate.setter
    def application_public_certificate(self, value):
        self[self.ValidKeywords.public_application_certificate] = value

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

    @property
    def user_token(self):
        """User token."""
        return self._internal_dict.get(self.ValidKeywords.user_token)

    @property
    def application_token(self):
        """Application token."""
        return self._internal_dict.get(self.ValidKeywords.application_token)

    @property
    def msi_authentication(self):
        """ A value stating the MSI identity type to obtain """
        return self._internal_dict.get(self.ValidKeywords.msi_auth)

    @property
    def msi_parameters(self):
        """ A user assigned MSI ID to be obtained """
        return self._internal_dict.get(self.ValidKeywords.msi_params)

    @property
    def az_cli(self):
        return self._internal_dict.get(self.ValidKeywords.az_cli)

    @property
    def token_provider(self):
        return self._token_provider

    def __str__(self):
        dict_copy = self._internal_dict.copy()
        for key in dict_copy:
            if key.is_secret():
                dict_copy[key] = "****"
        return self._build_connection_string(dict_copy)

    def __repr__(self):
        return self._build_connection_string(self._internal_dict)

    def _build_connection_string(self, kcsb_as_dict) -> str:
        return ";".join(["{0}={1}".format(word.value, kcsb_as_dict[word]) for word in self.ValidKeywords if word in kcsb_as_dict])


def _assert_value_is_valid(value):
    if not value or not value.strip():
        raise ValueError("Should not be empty")


class ClientRequestProperties:
    """This class is a POD used by client making requests to describe specific needs from the service executing the requests.
    For more information please look at: https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties
    """

    results_defer_partial_query_failures_option_name = "deferpartialqueryfailures"
    request_timeout_option_name = "servertimeout"

    def __init__(self):
        self._options = {}
        self._parameters = {}
        self.client_request_id = None
        self.application = None
        self.user = None

    def set_parameter(self, name: str, value: str):
        """Sets a parameter's value"""
        _assert_value_is_valid(name)
        self._parameters[name] = value

    def has_parameter(self, name):
        """Checks if a parameter is specified."""
        return name in self._parameters

    def get_parameter(self, name, default_value):
        """Gets a parameter's value."""
        return self._parameters.get(name, default_value)

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
        return json.dumps({"Options": self._options, "Parameters": self._parameters}, default=str)


class KustoClient:
    """
    Kusto client for Python.
    The client is a wrapper around the Kusto REST API.
    To read more about it, go to https://docs.microsoft.com/en-us/azure/kusto/api/rest/

    The primary methods are:
    `execute_query`:  executes a KQL query against the Kusto service.
    `execute_mgmt`: executes a KQL control command against the Kusto service.
    """

    _mgmt_default_timeout = timedelta(hours=1, seconds=30)
    _query_default_timeout = timedelta(minutes=4, seconds=30)
    _streaming_ingest_default_timeout = timedelta(minutes=10)

    # The maximum amount of connections to be able to operate in parallel
    _max_pool_size = 100

    def __init__(self, kcsb: Union[KustoConnectionStringBuilder, str]):
        """
        Kusto Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        :type kcsb: azure.kusto.data.KustoConnectionStringBuilder or str
        """
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)
        kusto_cluster = kcsb.data_source

        # Create a session object for connection pooling
        self._session = requests.Session()
        self._session.mount("http://", HTTPAdapter(pool_maxsize=self._max_pool_size))
        self._session.mount("https://", HTTPAdapter(pool_maxsize=self._max_pool_size))
        self._mgmt_endpoint = "{0}/v1/rest/mgmt".format(kusto_cluster)
        self._query_endpoint = "{0}/v2/rest/query".format(kusto_cluster)
        self._streaming_ingest_endpoint = "{0}/v1/rest/ingest/".format(kusto_cluster)
        # notice that in this context, federated actually just stands for add auth, not aad federated auth (legacy code)
        self._auth_provider = _AadHelper(kcsb) if kcsb.aad_federated_security else None
        self._request_headers = {"Accept": "application/json", "Accept-Encoding": "gzip,deflate", "x-ms-client-version": "Kusto.Python.Client:" + VERSION}

    def execute(self, database: str, query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        """
        Executes a query or management command.
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data.response.KustoResponseDataSet
        """
        query = query.strip()
        if query.startswith("."):
            return self.execute_mgmt(database, query, properties)
        return self.execute_query(database, query, properties)

    def execute_query(self, database: str, query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        """
        Execute a KQL query.
        To learn more about KQL go to https://docs.microsoft.com/en-us/azure/kusto/query/
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data.response.KustoResponseDataSet
        """
        return self._execute(self._query_endpoint, database, query, None, KustoClient._query_default_timeout, properties)

    def execute_mgmt(self, database: str, query: str, properties: ClientRequestProperties = None) -> KustoResponseDataSet:
        """
        Execute a KQL control command.
        To learn more about KQL control commands go to  https://docs.microsoft.com/en-us/azure/kusto/management/
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data.response.KustoResponseDataSet
        """
        return self._execute(self._mgmt_endpoint, database, query, None, KustoClient._mgmt_default_timeout, properties)

    def execute_streaming_ingest(
        self,
        database: str,
        table: str,
        stream: io.IOBase,
        stream_format: Union[DataFormat, str],
        properties: ClientRequestProperties = None,
        mapping_name: str = None,
    ):
        """
        Execute streaming ingest against this client
        If the Kusto service is not configured to allow streaming ingestion, this may raise an error
        To learn more about streaming ingestion go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming
        :param str database: Target database.
        :param str table: Target table.
        :param io.BaseIO stream: stream object which contains the data to ingest.
        :param DataFormat stream_format: Format of the data in the stream.
        :param ClientRequestProperties properties: additional request properties.
        :param str mapping_name: Pre-defined mapping of the table. Required when stream_format is json/avro.
        """
        stream_format = stream_format.value if isinstance(stream_format, DataFormat) else DataFormat(stream_format.lower()).value
        endpoint = self._streaming_ingest_endpoint + database + "/" + table + "?streamFormat=" + stream_format
        if mapping_name is not None:
            endpoint = endpoint + "&mappingName=" + mapping_name

        self._execute(endpoint, database, None, stream, KustoClient._streaming_ingest_default_timeout, properties)

    def _execute(self, endpoint: str, database: str, query: str, payload: io.IOBase, timeout: timedelta, properties: ClientRequestProperties = None):
        """Executes given query against this client"""
        request_headers = copy(self._request_headers)
        json_payload = None
        if not payload:
            json_payload = {"db": database, "csl": query}
            if properties:
                json_payload["properties"] = properties.to_json()

            client_request_id_prefix = "KPC.execute;"
            request_headers["Content-Type"] = "application/json; charset=utf-8"
        else:
            if properties:
                request_headers.update(json.loads(properties.to_json())["Options"])

            client_request_id_prefix = "KPC.execute_streaming_ingest;"
            request_headers["Content-Encoding"] = "gzip"

        request_headers["x-ms-client-request-id"] = client_request_id_prefix + str(uuid.uuid4())

        if properties is not None:
            if properties.client_request_id is not None:
                request_headers["x-ms-client-request-id"] = properties.client_request_id
            if properties.application is not None:
                request_headers["x-ms-app"] = properties.application
            if properties.user is not None:
                request_headers["x-ms-user"] = properties.user

        if self._auth_provider:
            request_headers["Authorization"] = self._auth_provider.acquire_authorization_header()

        if properties:
            timeout = properties.get_option(ClientRequestProperties.request_timeout_option_name, timeout)

        response = self._session.post(endpoint, headers=request_headers, data=payload, json=json_payload, timeout=timeout.seconds)

        if response.status_code == 200:
            if endpoint.endswith("v2/rest/query"):
                return KustoResponseDataSetV2(response.json())
            return KustoResponseDataSetV1(response.json())

        if response.status_code == 404:
            if payload:
                raise KustoServiceError("The ingestion endpoint does not exist. Please enable streaming ingestion on your cluster.", response)
            else:
                raise KustoServiceError("The requested endpoint '{}' does not exist.".format(endpoint), response)

        if payload:
            raise KustoServiceError(
                "An error occurred while trying to ingest: Status: {0.status_code}, Reason: {0.reason}, Text: {0.text}.".format(response), response
            )

        try:
            raise KustoServiceError([response.json()], response)
        except ValueError:
            if response.text:
                raise KustoServiceError([response.text], response)
            else:
                raise KustoServiceError("Server error response contains no data.", response)

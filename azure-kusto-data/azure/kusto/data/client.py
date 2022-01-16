# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import abc
import io
import json
import socket
import sys
import uuid
from copy import copy
from datetime import timedelta
from enum import Enum, unique
from typing import TYPE_CHECKING, Union, Callable, Optional, Any, Coroutine, List, Tuple, AnyStr, IO, NoReturn

import requests
from requests import Response
from urllib3.connection import HTTPConnection

from ._version import VERSION
from .data_format import DataFormat
from .exceptions import KustoServiceError, KustoApiError
from .response import KustoResponseDataSetV1, KustoResponseDataSetV2, KustoStreamingResponseDataSet, KustoResponseDataSet
from .security import _AadHelper
from .streaming_response import StreamingDataSetEnumerator, JsonTokenReader

if TYPE_CHECKING:
    import aiohttp
    import aiohttp.web_exceptions


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
        interactive_login = "Interactive Login"
        login_hint = "Login Hint"
        domain_hint = "Domain Hint"

        @classmethod
        def parse(cls, key: str) -> "KustoConnectionStringBuilder.ValidKeywords":
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
            if key in ["az cli"]:
                return cls.az_cli
            if key in ["interactive login"]:
                return cls.interactive_login
            if key in ["login hint"]:
                return cls.login_hint
            if key in ["domain hint"]:
                return cls.domain_hint
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
                self.login_hint,
                self.domain_hint,
            ]

        def is_dict_type(self) -> bool:
            return self in [self.msi_params]

        def is_bool_type(self) -> bool:
            """States whether a word is of type bool or not."""
            return self in [self.aad_federated_security, self.msi_auth, self.az_cli, self.interactive_login]

    def __init__(self, connection_string: str):
        """
        Creates new KustoConnectionStringBuilder.
        :param str connection_string: Kusto connection string should be of the format:
        https://<clusterName>.kusto.windows.net;AAD User ID="user@microsoft.com";Password=P@ssWord
        For more information please look at:
        https://kusto.azurewebsites.net/docs/concepts/kusto_connection_strings.html
        """
        _assert_value_is_valid(connection_string)
        self._internal_dict = {}
        self._token_provider = None
        self._async_token_provider = None
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

    def __setitem__(self, key: "Union[KustoConnectionStringBuilder.ValidKeywords, str]", value: Union[str, bool, dict]):
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
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
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
        :param str connection_string: Kusto connection string should be of the format:
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
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
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
        :param str connection_string: Kusto connection string should be of the format:
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
        :param str connection_string: Kusto connection string should be of the format:
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
        :param str connection_string: Kusto connection string should be of the format:
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
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
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
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
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

        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
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
            # noinspection PyUnreachableCode
            params["object_id"] = object_id
            exclusive_pcount += 1

        if msi_res_id is not None:
            # Until we upgrade azure-identity to version 1.4.1, only client_id is excepted as a hint for user managed service identity
            raise ValueError(
                "User Managed Service Identity with msi_res_id is temporarily not supported by azure identity 1.3.1. Please use client_id instead."
            )
            # noinspection PyUnreachableCode
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
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
        :param token_provider: a parameterless function that returns a valid bearer token for the relevant kusto resource as a string
        """

        assert callable(token_provider)

        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb._token_provider = token_provider

        return kcsb

    @classmethod
    def with_async_token_provider(
        cls,
        connection_string: str,
        async_token_provider: Callable[[], Coroutine[None, None, str]],
    ) -> "KustoConnectionStringBuilder":
        """
        Create a KustoConnectionStringBuilder that uses an async callback function to obtain a connection token
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
        :param async_token_provider: a parameterless function that after awaiting returns a valid bearer token for the relevant kusto resource as a string
        """

        assert callable(async_token_provider)

        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb._async_token_provider = async_token_provider

        return kcsb

    @classmethod
    def with_interactive_login(
        cls, connection_string: str, login_hint: Optional[str] = None, domain_hint: Optional[str] = None
    ) -> "KustoConnectionStringBuilder":
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.interactive_login] = True
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        if login_hint is not None:
            kcsb[kcsb.ValidKeywords.login_hint] = login_hint

        if domain_hint is not None:
            kcsb[kcsb.ValidKeywords.domain_hint] = domain_hint

        return kcsb

    @property
    def data_source(self) -> Optional[str]:
        """The URI specifying the Kusto service endpoint.
        For example, https://kuskus.kusto.windows.net or net.tcp://localhost
        """
        return self._internal_dict.get(self.ValidKeywords.data_source)

    @property
    def aad_user_id(self) -> Optional[str]:
        """The username to use for AAD Federated AuthN."""
        return self._internal_dict.get(self.ValidKeywords.aad_user_id)

    @property
    def password(self) -> Optional[str]:
        """The password to use for authentication when username/password authentication is used.
        Must be accompanied by UserID property
        """
        return self._internal_dict.get(self.ValidKeywords.password)

    @property
    def application_client_id(self) -> Optional[str]:
        """The application client id to use for authentication when federated
        authentication is used.
        """
        return self._internal_dict.get(self.ValidKeywords.application_client_id)

    @property
    def application_key(self) -> Optional[str]:
        """The application key to use for authentication when federated authentication is used"""
        return self._internal_dict.get(self.ValidKeywords.application_key)

    @property
    def application_certificate(self) -> Optional[str]:
        """A PEM encoded certificate private key."""
        return self._internal_dict.get(self.ValidKeywords.application_certificate)

    @application_certificate.setter
    def application_certificate(self, value: str):
        self[self.ValidKeywords.application_certificate] = value

    @property
    def application_certificate_thumbprint(self) -> Optional[str]:
        """hex encoded thumbprint of the certificate."""
        return self._internal_dict.get(self.ValidKeywords.application_certificate_thumbprint)

    @application_certificate_thumbprint.setter
    def application_certificate_thumbprint(self, value: str):
        self[self.ValidKeywords.application_certificate_thumbprint] = value

    @property
    def application_public_certificate(self) -> Optional[str]:
        """A public certificate matching the PEM encoded certificate private key."""
        return self._internal_dict.get(self.ValidKeywords.public_application_certificate)

    @application_public_certificate.setter
    def application_public_certificate(self, value: str):
        self[self.ValidKeywords.public_application_certificate] = value

    @property
    def authority_id(self) -> Optional[str]:
        """The ID of the AAD tenant where the application is configured.
        (should be supplied only for non-Microsoft tenant)"""
        return self._internal_dict.get(self.ValidKeywords.authority_id)

    @authority_id.setter
    def authority_id(self, value: str):
        self[self.ValidKeywords.authority_id] = value

    @property
    def aad_federated_security(self) -> Optional[bool]:
        """A Boolean value that instructs the client to perform AAD federated authentication."""
        return self._internal_dict.get(self.ValidKeywords.aad_federated_security)

    @property
    def user_token(self) -> Optional[str]:
        """User token."""
        return self._internal_dict.get(self.ValidKeywords.user_token)

    @property
    def application_token(self) -> Optional[str]:
        """Application token."""
        return self._internal_dict.get(self.ValidKeywords.application_token)

    @property
    def msi_authentication(self) -> Optional[bool]:
        """A value stating the MSI identity type to obtain"""
        return self._internal_dict.get(self.ValidKeywords.msi_auth)

    @property
    def msi_parameters(self) -> Optional[dict]:
        """A user assigned MSI ID to be obtained"""
        return self._internal_dict.get(self.ValidKeywords.msi_params)

    @property
    def az_cli(self) -> Optional[bool]:
        return self._internal_dict.get(self.ValidKeywords.az_cli)

    @property
    def token_provider(self) -> Optional[Callable[[], str]]:
        return self._token_provider

    @property
    def async_token_provider(self) -> Optional[Callable[[], Coroutine[None, None, str]]]:
        return self._async_token_provider

    @property
    def interactive_login(self) -> bool:
        val = self._internal_dict.get(self.ValidKeywords.interactive_login)
        return val is not None and val

    @property
    def login_hint(self) -> Optional[str]:
        return self._internal_dict.get(self.ValidKeywords.login_hint)

    @property
    def domain_hint(self) -> Optional[str]:
        return self._internal_dict.get(self.ValidKeywords.domain_hint)

    def __str__(self) -> str:
        dict_copy = self._internal_dict.copy()
        for key in dict_copy:
            if key.is_secret():
                dict_copy[key] = "****"
        return self._build_connection_string(dict_copy)

    def __repr__(self) -> str:
        return self._build_connection_string(self._internal_dict)

    def _build_connection_string(self, kcsb_as_dict: dict) -> str:
        return ";".join(["{0}={1}".format(word.value, kcsb_as_dict[word]) for word in self.ValidKeywords if word in kcsb_as_dict])


def _assert_value_is_valid(value: str):
    if not value or not value.strip():
        raise ValueError("Should not be empty")


class ClientRequestProperties:
    """This class is a POD used by client making requests to describe specific needs from the service executing the requests.
    For more information please look at: https://docs.microsoft.com/en-us/azure/kusto/api/netfx/request-properties
    """

    results_defer_partial_query_failures_option_name = "deferpartialqueryfailures"
    request_timeout_option_name = "servertimeout"
    no_request_timeout_option_name = "norequesttimeout"

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

    def has_parameter(self, name: str) -> bool:
        """Checks if a parameter is specified."""
        return name in self._parameters

    def get_parameter(self, name: str, default_value: str) -> str:
        """Gets a parameter's value."""
        return self._parameters.get(name, default_value)

    def set_option(self, name: str, value: Any):
        """Sets an option's value"""
        _assert_value_is_valid(name)
        self._options[name] = value

    def has_option(self, name: str) -> bool:
        """Checks if an option is specified."""
        return name in self._options

    def get_option(self, name: str, default_value: Any) -> str:
        """Gets an option's value."""
        return self._options.get(name, default_value)

    def to_json(self) -> str:
        """Safe serialization to a JSON string."""
        return json.dumps({"Options": self._options, "Parameters": self._parameters}, default=str)


class ExecuteRequestParams:
    def __init__(self, database: str, payload: Optional[io.IOBase], properties: ClientRequestProperties, query: str, timeout: timedelta, request_headers: dict):
        request_headers = copy(request_headers)
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

            # Before 3.0 it was KPC.execute_streaming_ingest, but was changed to align with the other SDKs
            client_request_id_prefix = "KPC.executeStreamingIngest;"
            request_headers["Content-Encoding"] = "gzip"
        request_headers["x-ms-client-request-id"] = client_request_id_prefix + str(uuid.uuid4())
        if properties is not None:
            if properties.client_request_id is not None:
                request_headers["x-ms-client-request-id"] = properties.client_request_id
            if properties.application is not None:
                request_headers["x-ms-app"] = properties.application
            if properties.user is not None:
                request_headers["x-ms-user"] = properties.user
            if properties.get_option(ClientRequestProperties.no_request_timeout_option_name, False):
                timeout = KustoClient._mgmt_default_timeout
            else:
                timeout = properties.get_option(ClientRequestProperties.request_timeout_option_name, timeout)

        timeout = (timeout or KustoClient._mgmt_default_timeout) + KustoClient._client_server_delta

        self.json_payload = json_payload
        self.request_headers = request_headers
        self.timeout = timeout


class HTTPAdapterWithSocketOptions(requests.adapters.HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.socket_options = kwargs.pop("socket_options", None)
        self.pool_maxsize = kwargs.pop("pool_maxsize", None)
        self.max_retries = kwargs.pop("max_retries", None)
        super(HTTPAdapterWithSocketOptions, self).__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        if self.socket_options is not None:
            kwargs["socket_options"] = self.socket_options
        super(HTTPAdapterWithSocketOptions, self).init_poolmanager(*args, **kwargs)


class _KustoClientBase(abc.ABC):
    API_VERSION = "2019-02-13"

    _mgmt_default_timeout = timedelta(hours=1, seconds=30)
    _query_default_timeout = timedelta(minutes=4, seconds=30)
    _streaming_ingest_default_timeout = timedelta(minutes=10)

    _aad_helper: _AadHelper

    def __init__(self, kcsb: Union[KustoConnectionStringBuilder, str], is_async):
        self._kcsb = kcsb
        self._proxy_url: Optional[str] = None
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            self._kcsb = KustoConnectionStringBuilder(kcsb)
        self._kusto_cluster = self._kcsb.data_source

        # notice that in this context, federated actually just stands for aad auth, not aad federated auth (legacy code)
        self._aad_helper = _AadHelper(self._kcsb, is_async) if self._kcsb.aad_federated_security else None

        # Create a session object for connection pooling
        self._mgmt_endpoint = "{0}/v1/rest/mgmt".format(self._kusto_cluster)
        self._query_endpoint = "{0}/v2/rest/query".format(self._kusto_cluster)
        self._streaming_ingest_endpoint = "{0}/v1/rest/ingest/".format(self._kusto_cluster)
        self._request_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip,deflate",
            "x-ms-client-version": "Kusto.Python.Client:" + VERSION,
            "x-ms-version": self.API_VERSION,
        }

    def set_proxy(self, proxy_url: str):
        self._proxy_url = proxy_url
        if self._aad_helper:
            self._aad_helper.token_provider.set_proxy(proxy_url)

    @staticmethod
    def _kusto_parse_by_endpoint(endpoint: str, response_json: Any) -> KustoResponseDataSet:
        if endpoint.endswith("v2/rest/query"):
            return KustoResponseDataSetV2(response_json)
        return KustoResponseDataSetV1(response_json)

    @staticmethod
    def _handle_http_error(
        exception: Exception,
        endpoint: Optional[str],
        payload: Optional[io.IOBase],
        response: "Union[Response, aiohttp.ClientResponse]",
        status: int,
        response_json: Any,
        response_text: Optional[str],
    ) -> NoReturn:

        if status == 404:
            if payload:
                raise KustoServiceError("The ingestion endpoint does not exist. Please enable streaming ingestion on your cluster.", response) from exception

            raise KustoServiceError(f"The requested endpoint '{endpoint}' does not exist.", response) from exception

        if payload:
            message = f"An error occurred while trying to ingest: Status: {status}, Reason: {response.reason}, Text: {response_text}."
            if response_json:
                raise KustoApiError(response_json, message, response) from exception

            raise KustoServiceError(message, response) from exception

        if response_json:
            raise KustoApiError(response_json, http_response=response) from exception

        if response_text:
            raise KustoServiceError(response_text, response) from exception

        raise KustoServiceError("Server error response contains no data.", response) from exception


class KustoClient(_KustoClientBase):
    """
    Kusto client for Python.
    The client is a wrapper around the Kusto REST API.
    To read more about it, go to https://docs.microsoft.com/en-us/azure/kusto/api/rest/

    The primary methods are:
    `execute_query`:  executes a KQL query against the Kusto service.
    `execute_mgmt`: executes a KQL control command against the Kusto service.
    """

    _mgmt_default_timeout = timedelta(hours=1)
    _query_default_timeout = timedelta(minutes=4)
    _streaming_ingest_default_timeout = timedelta(minutes=10)
    _client_server_delta = timedelta(seconds=30)

    # The maximum amount of connections to be able to operate in parallel
    _max_pool_size = 100

    def __init__(self, kcsb: Union[KustoConnectionStringBuilder, str]):
        """
        Kusto Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        :type kcsb: azure.kusto.data.KustoConnectionStringBuilder or str
        """
        super().__init__(kcsb, False)

        # Create a session object for connection pooling
        self._session = requests.Session()

        adapter = HTTPAdapterWithSocketOptions(
            socket_options=(HTTPConnection.default_socket_options or []) + self.compose_socket_options(), pool_maxsize=self._max_pool_size
        )
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    def set_proxy(self, proxy_url: str):
        super().set_proxy(proxy_url)
        self._session.proxies = {"http": proxy_url, "https": proxy_url}

    def set_http_retries(self, max_retries: int):
        """
        Set the number of HTTP retries to attempt
        """
        adapter = HTTPAdapterWithSocketOptions(
            socket_options=(HTTPConnection.default_socket_options or []) + self.compose_socket_options(),
            pool_maxsize=self._max_pool_size,
            max_retries=max_retries,
        )
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

    @staticmethod
    def compose_socket_options() -> List[Tuple[int, int, int]]:
        # Sends TCP Keep-Alive after MAX_IDLE_SECONDS seconds of idleness, once every INTERVAL_SECONDS seconds, and closes the connection after MAX_FAILED_KEEPALIVES failed pings (e.g. 20 => 1:00:30)
        MAX_IDLE_SECONDS = 30
        INTERVAL_SECONDS = 180  # Corresponds to Azure Load Balancer Service 4 minute timeout, with 1 minute of slack
        MAX_FAILED_KEEPALIVES = 20

        if (
            sys.platform == "linux"
            and hasattr(socket, "SOL_SOCKET")
            and hasattr(socket, "SO_KEEPALIVE")
            and hasattr(socket, "TCP_KEEPIDLE")
            and hasattr(socket, "TCP_KEEPINTVL")
            and hasattr(socket, "TCP_KEEPCNT")
        ):
            return [
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, MAX_IDLE_SECONDS),
                (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, INTERVAL_SECONDS),
                (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, MAX_FAILED_KEEPALIVES),
            ]
        elif (
            sys.platform == "win32"
            and hasattr(socket, "SOL_SOCKET")
            and hasattr(socket, "SO_KEEPALIVE")
            and hasattr(socket, "TCP_KEEPIDLE")
            and hasattr(socket, "TCP_KEEPCNT")
        ):
            return [
                (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
                (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, MAX_IDLE_SECONDS),
                (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, MAX_FAILED_KEEPALIVES),
            ]
        elif sys.platform == "darwin" and hasattr(socket, "SOL_SOCKET") and hasattr(socket, "SO_KEEPALIVE") and hasattr(socket, "IPPROTO_TCP"):
            TCP_KEEPALIVE = 0x10
            return [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1), (socket.IPPROTO_TCP, TCP_KEEPALIVE, INTERVAL_SECONDS)]
        else:
            return []

    def execute(self, database: str, query: str, properties: Optional[ClientRequestProperties] = None) -> KustoResponseDataSet:
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

    def execute_query(self, database: str, query: str, properties: Optional[ClientRequestProperties] = None) -> KustoResponseDataSet:
        """
        Execute a KQL query.
        To learn more about KQL go to https://docs.microsoft.com/en-us/azure/kusto/query/
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data.response.KustoResponseDataSet
        """
        return self._execute(self._query_endpoint, database, query, None, self._query_default_timeout, properties)

    def execute_mgmt(self, database: str, query: str, properties: Optional[ClientRequestProperties] = None) -> KustoResponseDataSet:
        """
        Execute a KQL control command.
        To learn more about KQL control commands go to  https://docs.microsoft.com/en-us/azure/kusto/management/
        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param azure.kusto.data.ClientRequestProperties properties: Optional additional properties.
        :return: Kusto response data set.
        :rtype: azure.kusto.data.response.KustoResponseDataSet
        """
        return self._execute(self._mgmt_endpoint, database, query, None, self._mgmt_default_timeout, properties)

    def execute_streaming_ingest(
        self,
        database: str,
        table: str,
        stream: IO[AnyStr],
        stream_format: Union[DataFormat, str],
        properties: Optional[ClientRequestProperties] = None,
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
        stream_format = stream_format.kusto_value if isinstance(stream_format, DataFormat) else DataFormat[stream_format.upper()].kusto_value
        endpoint = self._streaming_ingest_endpoint + database + "/" + table + "?streamFormat=" + stream_format
        if mapping_name is not None:
            endpoint = endpoint + "&mappingName=" + mapping_name

        self._execute(endpoint, database, None, stream, self._streaming_ingest_default_timeout, properties)

    def _execute_streaming_query_parsed(
        self, database: str, query: str, timeout: timedelta = _KustoClientBase._query_default_timeout, properties: Optional[ClientRequestProperties] = None
    ) -> StreamingDataSetEnumerator:
        response = self._execute(self._query_endpoint, database, query, None, timeout, properties, stream_response=True)
        response.raw.decode_content = True
        return StreamingDataSetEnumerator(JsonTokenReader(response.raw))

    def execute_streaming_query(
        self, database: str, query: str, timeout: timedelta = _KustoClientBase._query_default_timeout, properties: Optional[ClientRequestProperties] = None
    ) -> KustoStreamingResponseDataSet:
        """
        Execute a KQL query without reading it all to memory.
        The resulting KustoStreamingResponseDataSet will stream one table at a time, and the rows can be retrieved sequentially.

        :param str database: Database against query will be executed.
        :param str query: Query to be executed.
        :param timedelta timeout: timeout for the query to be executed
        :param azure.kusto.data.ClientRequestProperties properties: Optional additional properties.
        :return KustoStreamingResponseDataSet:
        """
        return KustoStreamingResponseDataSet(self._execute_streaming_query_parsed(database, query, timeout, properties))

    def _execute(
        self,
        endpoint: str,
        database: str,
        query: Optional[str],
        payload: Optional[IO[AnyStr]],
        timeout: timedelta,
        properties: Optional[ClientRequestProperties] = None,
        stream_response: bool = False,
    ) -> Union[KustoResponseDataSet, Response]:
        """Executes given query against this client"""
        request_params = ExecuteRequestParams(database, payload, properties, query, timeout, self._request_headers)
        json_payload = request_params.json_payload
        request_headers = request_params.request_headers
        timeout = request_params.timeout
        if self._aad_helper:
            request_headers["Authorization"] = self._aad_helper.acquire_authorization_header()
        response = self._session.post(endpoint, headers=request_headers, json=json_payload, data=payload, timeout=timeout.seconds, stream=stream_response)

        if stream_response:
            try:
                response.raise_for_status()
                return response
            except Exception as e:
                raise self._handle_http_error(e, self._query_endpoint, None, response, response.status_code, response.json(), response.text)

        response_json = None
        try:
            response_json = response.json()
            response.raise_for_status()
        except Exception as e:
            raise self._handle_http_error(e, endpoint, payload, response, response.status_code, response_json, response.text)

        return self._kusto_parse_by_endpoint(endpoint, response_json)

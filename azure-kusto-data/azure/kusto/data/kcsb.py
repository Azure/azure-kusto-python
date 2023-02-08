from enum import unique, Enum
from typing import Union, Callable, Coroutine, Optional, Tuple, List, Any

from ._string_utils import assert_string_is_not_empty
from .client_details import ClientDetails


class KustoConnectionStringBuilder:
    """
    Parses Kusto connection strings.
    For usages, check out the sample at:
        https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py
    """

    kcsb_invalid_item_error = "%s is not supported as an item in KustoConnectionStringBuilder"

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
            if key in ["msi_auth", "msi authentication"]:
                return cls.msi_auth
            if key in ["msi_type", "msi params"]:
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
        assert_string_is_not_empty(connection_string)
        self._internal_dict = {}
        self._token_provider = None
        self._async_token_provider = None
        self.is_token_credential_auth = False
        self.credential: Optional[Any] = None
        self.credential_from_login_endpoint: Optional[Any] = None
        if connection_string is not None and "=" not in connection_string.partition(";")[0]:
            connection_string = "Data Source=" + connection_string

        self._application_for_tracing: Optional[str] = None
        self._user_for_tracing: Optional[str] = None

        self[self.ValidKeywords.authority_id] = "organizations"

        for kvp_string in connection_string.split(";"):
            key, _, value = kvp_string.partition("=")
            try:
                keyword = self.ValidKeywords.parse(key)
            except KeyError:
                raise KeyError(self.kcsb_invalid_item_error % key)
            value_stripped = value.strip()
            if keyword.is_str_type():
                self[keyword] = value_stripped.rstrip("/")
            elif keyword.is_bool_type():
                if value_stripped in ["True", "true"]:
                    self[keyword] = True
                elif value_stripped in ["False", "false"]:
                    self[keyword] = False
                else:
                    raise KeyError("Expected aad federated security to be bool. Recieved %s" % value)

    def __setitem__(self, key: "Union[KustoConnectionStringBuilder.ValidKeywords, str]", value: Union[str, bool, dict]):
        try:
            keyword = key if isinstance(key, self.ValidKeywords) else self.ValidKeywords.parse(key)
        except KeyError:
            raise KeyError(self.kcsb_invalid_item_error % key)

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
        cls, connection_string: str, user_id: str, password: str, authority_id: str = "organizations"
    ) -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD user name and
        password.
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
        :param str user_id: AAD user ID.
        :param str password: Corresponding password of the AAD user.
        :param str authority_id: optional param. defaults to "organizations"
        """
        assert_string_is_not_empty(user_id)
        assert_string_is_not_empty(password)

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
        assert_string_is_not_empty(user_token)

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
        assert_string_is_not_empty(aad_app_id)
        assert_string_is_not_empty(app_key)
        assert_string_is_not_empty(authority_id)

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
        assert_string_is_not_empty(aad_app_id)
        assert_string_is_not_empty(certificate)
        assert_string_is_not_empty(thumbprint)
        assert_string_is_not_empty(authority_id)

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
        assert_string_is_not_empty(aad_app_id)
        assert_string_is_not_empty(private_certificate)
        assert_string_is_not_empty(public_certificate)
        assert_string_is_not_empty(thumbprint)
        assert_string_is_not_empty(authority_id)

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
        assert_string_is_not_empty(application_token)
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb[kcsb.ValidKeywords.application_token] = application_token

        return kcsb

    @classmethod
    def with_aad_device_authentication(cls, connection_string: str, authority_id: str = "organizations") -> "KustoConnectionStringBuilder":
        """
        Creates a KustoConnection string builder that will authenticate with AAD application and
        password.
        :param str connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
        :param str authority_id: optional param. defaults to "organizations"
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

    @classmethod
    def with_azure_token_credential(
        cls,
        connection_string: str,
        credential: Optional[Any] = None,
        credential_from_login_endpoint: Optional[Callable[[str], Any]] = None,
    ) -> "KustoConnectionStringBuilder":
        """
        Create a KustoConnectionStringBuilder that uses an azure token credential to obtain a connection token.
        :param connection_string: Kusto connection string should be of the format: https://<clusterName>.kusto.windows.net
        :param credential: an optional token credential to use for authentication
        :param credential_from_login_endpoint: an optional function that returns a token credential for the relevant kusto resource
        """
        kcsb = cls(connection_string)
        kcsb[kcsb.ValidKeywords.aad_federated_security] = True
        kcsb.is_token_credential_auth = True
        kcsb.credential = credential
        kcsb.credential_from_login_endpoint = credential_from_login_endpoint

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

    @property
    def application_for_tracing(self) -> Optional[str]:
        return self._application_for_tracing

    @application_for_tracing.setter
    def application_for_tracing(self, value: str):
        self._application_for_tracing = value

    @property
    def user_name_for_tracing(self) -> Optional[str]:
        return self._user_for_tracing

    @user_name_for_tracing.setter
    def user_name_for_tracing(self, value: str):
        self._user_for_tracing = value

    @property
    def client_details(self) -> ClientDetails:
        return ClientDetails(self.application_for_tracing, self.user_name_for_tracing)

    def _set_connector_details(
        self,
        name: str,
        version: str,
        app_name: Optional[str] = None,
        app_version: Optional[str] = None,
        send_user: bool = False,
        override_user: Optional[str] = None,
        additional_fields: Optional[List[Tuple[str, str]]] = None,
    ):
        """
        Sets the connector details for tracing purposes.
        :param name:  The name of the connector
        :param version:  The version of the connector
        :param send_user: Whether to send the user name
        :param override_user: Override the user name ( if send_user is True )
        :param app_name: The name of the containing application
        :param app_version: The version of the containing application
        :param additional_fields: Additional fields to add to the header
        """
        client_details = ClientDetails.set_connector_details(name, version, app_name, app_version, send_user, override_user, additional_fields)

        self.application_for_tracing = client_details.application_for_tracing
        self.user_name_for_tracing = client_details.user_name_for_tracing

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

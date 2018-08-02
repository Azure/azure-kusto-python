"""A module contains all kusto security related classes."""

from datetime import timedelta, datetime
from enum import Enum
import webbrowser
import dateutil.parser
from adal import AuthenticationContext
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoClientError


class AuthenticationMethod(Enum):
    """Enum represnting all authentication methods available in Kusto."""

    aad_username_password = "aad_username_password"
    aad_application_key = "aad_application_key"
    aad_device_login = "aad_device_login"


class _AadHelper(object):
    def __init__(self, kcsb):
        try:
            authority = kcsb.authority_id or "microsoft.com"
        except KeyError:
            authority = "microsoft.com"
        self._kusto_cluster = kcsb.data_source
        self._adal_context = AuthenticationContext(
            "https://login.windows.net/{0}".format(authority)
        )
        self._username = None
        if kcsb.aad_user_id is not None:
            self._authentication_method = AuthenticationMethod.aad_username_password
            self._client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
            self._username = kcsb.aad_user_id
            self._password = kcsb.password
        elif kcsb.application_client_id is not None:
            self._authentication_method = AuthenticationMethod.aad_application_key
            self._client_id = kcsb[kcsb._Keywords.application_client_id]
            self._client_secret = kcsb[kcsb._Keywords.application_key]
        else:
            self._authentication_method = AuthenticationMethod.aad_device_login
            self._client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

    def acquire_token(self):
        """A method to acquire tokens from AAD."""
        token_response = self._adal_context.acquire_token(
            self._kusto_cluster, self._username, self._client_id
        )
        if token_response is not None:
            expiration_date = dateutil.parser.parse(token_response["expiresOn"])
            if expiration_date > datetime.utcnow() + timedelta(minutes=5):
                return token_response["accessToken"]

        if self._authentication_method is AuthenticationMethod.aad_username_password:
            token_response = self._adal_context.acquire_token_with_username_password(
                self._kusto_cluster, self._username, self._password, self._client_id
            )
        elif self._authentication_method is AuthenticationMethod.aad_application_key:
            token_response = self._adal_context.acquire_token_with_client_credentials(
                self._kusto_cluster, self._client_id, self._client_secret
            )
        elif self._authentication_method is AuthenticationMethod.aad_device_login:
            code = self._adal_context.acquire_user_code(self._kusto_cluster, self._client_id)
            print(code["message"])
            webbrowser.open(code["verification_url"])
            token_response = self._adal_context.acquire_token_with_device_code(
                self._kusto_cluster, code, self._client_id
            )
        else:
            raise KustoClientError(
                "Please choose authentication method from azure.kusto.data.security.AuthenticationMethod"
            )

        return token_response["accessToken"]

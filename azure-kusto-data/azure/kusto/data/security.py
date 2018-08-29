"""A module to acquire tokens from AAD."""

from enum import Enum, unique
from datetime import timedelta, datetime
import webbrowser
from six.moves.urllib.parse import urlparse
import dateutil.parser

from adal import AuthenticationContext
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters

from .exceptions import KustoClientError


@unique
class AuthenticationMethod(Enum):
    """Enum represnting all authentication methods available in Kusto with Python."""

    aad_username_password = "aad_username_password"
    aad_application_key = "aad_application_key"
    aad_application_certificate = "aad_application_certificate"
    aad_device_login = "aad_device_login"


class _AadHelper(object):
    def __init__(self, kcsb):
        authority = kcsb.authority_id or "common"
        self._kusto_cluster = "{0.scheme}://{0.hostname}".format(urlparse(kcsb.data_source))
        self._adal_context = AuthenticationContext("https://login.microsoftonline.com/{0}".format(authority))
        self._username = None
        if all([kcsb.aad_user_id, kcsb.password]):
            self._authentication_method = AuthenticationMethod.aad_username_password
            self._client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
            self._username = kcsb.aad_user_id
            self._password = kcsb.password
        elif all([kcsb.application_client_id, kcsb.application_key]):
            self._authentication_method = AuthenticationMethod.aad_application_key
            self._client_id = kcsb.application_client_id
            self._client_secret = kcsb.application_key
        elif all([kcsb.application_client_id, kcsb.application_certificate, kcsb.application_certificate_thumbprint]):
            self._authentication_method = AuthenticationMethod.aad_application_certificate
            self._client_id = kcsb.application_client_id
            self._certificate = kcsb.application_certificate
            self._thumbprint = kcsb.application_certificate_thumbprint
        else:
            self._authentication_method = AuthenticationMethod.aad_device_login
            self._client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

    def acquire_token(self):
        """Acquire tokens from AAD."""
        token = self._adal_context.acquire_token(self._kusto_cluster, self._username, self._client_id)
        if token is not None:
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=1):
                return _get_header(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = self._adal_context.acquire_token_with_refresh_token(
                    token[TokenResponseFields.REFRESH_TOKEN], self._client_id, self._kusto_cluster
                )
                if token is not None:
                    return _get_header(token)

        if self._authentication_method is AuthenticationMethod.aad_username_password:
            token = self._adal_context.acquire_token_with_username_password(
                self._kusto_cluster, self._username, self._password, self._client_id
            )
        elif self._authentication_method is AuthenticationMethod.aad_application_key:
            token = self._adal_context.acquire_token_with_client_credentials(
                self._kusto_cluster, self._client_id, self._client_secret
            )
        elif self._authentication_method is AuthenticationMethod.aad_device_login:
            code = self._adal_context.acquire_user_code(self._kusto_cluster, self._client_id)
            print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
            webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
            token = self._adal_context.acquire_token_with_device_code(self._kusto_cluster, code, self._client_id)
        elif self._authentication_method is AuthenticationMethod.aad_application_certificate:
            token = self._adal_context.acquire_token_with_client_certificate(
                self._kusto_cluster, self._client_id, self._certificate, self._thumbprint
            )
        else:
            raise KustoClientError(
                "Please choose authentication method from azure.kusto.data.security.AuthenticationMethod"
            )

        return _get_header(token)


def _get_header(token):
    return "{0} {1}".format(token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN])

"""A module to acquire tokens from AAD."""
import os
import webbrowser
from datetime import timedelta, datetime
from enum import Enum, unique
from urllib.parse import urlparse

import dateutil.parser
from adal import AuthenticationContext, AdalError
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters
from msrestazure.azure_active_directory import MSIAuthentication

from .exceptions import KustoClientError, KustoAuthenticationError


def _get_azure_cli_auth_token() -> dict:
    """
    Try to get the az cli authenticated token
    :return: refresh token
    """
    import os

    try:
        # this makes it cleaner, but in case azure cli is not present on virtual env,
        # but cli exists on computer, we can try and manually get the token from the cache
        from azure.cli.core._profile import Profile
        from azure.cli.core._session import ACCOUNT
        from azure.cli.core._environment import get_config_dir

        azure_folder = get_config_dir()
        ACCOUNT.load(os.path.join(azure_folder, "azureProfile.json"))
        profile = Profile(storage=ACCOUNT)
        token_data = profile.get_raw_token()[0][2]

        return token_data

    except ModuleNotFoundError:
        try:
            import os
            import json

            folder = os.getenv("AZURE_CONFIG_DIR", None) or os.path.expanduser(os.path.join("~", ".azure"))
            token_path = os.path.join(folder, "accessTokens.json")
            with open(token_path) as f:
                data = json.load(f)

            # TODO: not sure I should take the first
            return data[0]
        except Exception:
            pass


@unique
class AuthenticationMethod(Enum):
    """Enum representing all authentication methods available in Kusto with Python."""

    aad_username_password = "aad_username_password"
    aad_application_key = "aad_application_key"
    aad_application_certificate = "aad_application_certificate"
    aad_device_login = "aad_device_login"
    aad_token = "aad_token"
    aad_msi = "aad_msi"
    az_cli_profile = "az_cli_profile"


CLOUD_LOGIN_URL = "https://login.microsoftonline.com/"


class _AadHelper:
    def __init__(self, kcsb):
        self._kusto_cluster = "{0.scheme}://{0.hostname}".format(urlparse(kcsb.data_source))
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
        elif kcsb.msi_authentication:
            self._authentication_method = AuthenticationMethod.aad_msi
            self._msi_params = kcsb.msi_parameters
            return
        elif any([kcsb.user_token, kcsb.application_token]):
            self._token = kcsb.user_token or kcsb.application_token
            self._authentication_method = AuthenticationMethod.aad_token
            return
        elif kcsb.az_cli:
            self._authentication_method = AuthenticationMethod.az_cli_profile
            return
        else:
            self._authentication_method = AuthenticationMethod.aad_device_login
            self._client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

        authority = kcsb.authority_id or "common"
        aad_authority_uri = os.environ.get("AadAuthorityUri", CLOUD_LOGIN_URL)
        full_authority_uri = aad_authority_uri + authority if aad_authority_uri.endswith("/") else aad_authority_uri + "/" + authority
        self._adal_context = AuthenticationContext(full_authority_uri)

    def acquire_authorization_header(self):
        """Acquire tokens from AAD."""
        try:
            return self._acquire_authorization_header()
        except (AdalError, KustoClientError) as error:
            if self._authentication_method is AuthenticationMethod.aad_username_password:
                kwargs = {"username": self._username, "client_id": self._client_id}
            elif self._authentication_method is AuthenticationMethod.aad_application_key:
                kwargs = {"client_id": self._client_id}
            elif self._authentication_method is AuthenticationMethod.aad_device_login:
                kwargs = {"client_id": self._client_id}
            elif self._authentication_method is AuthenticationMethod.aad_application_certificate:
                kwargs = {"client_id": self._client_id, "thumbprint": self._thumbprint}
            elif self._authentication_method is AuthenticationMethod.aad_msi:
                kwargs = self._msi_params
            else:
                raise error

            kwargs["resource"] = self._kusto_cluster

            if self._authentication_method is AuthenticationMethod.aad_msi:
                kwargs["authority"] = AuthenticationMethod.aad_msi.value
            else:
                kwargs["authority"] = self._adal_context.authority.url

            raise KustoAuthenticationError(self._authentication_method.value, error, **kwargs)

    def _acquire_authorization_header(self):
        # Token was provided by caller
        if self._authentication_method is AuthenticationMethod.aad_token:
            return _get_header("Bearer", self._token)

        # Obtain token from MSI endpoint
        if self._authentication_method == AuthenticationMethod.aad_msi:
            token = self.get_token_from_msi()
            return _get_header_from_dict(token)

        user = self._username
        client_id = getattr(self, '_client_id', None)

        token = None

        if self._authentication_method == AuthenticationMethod.az_cli_profile:
            stored_token = _get_azure_cli_auth_token()

            if (TokenResponseFields.REFRESH_TOKEN in stored_token and
                TokenResponseFields._CLIENT_ID in stored_token and
                TokenResponseFields._AUTHORITY in stored_token
            ):
                client_id = stored_token[TokenResponseFields._CLIENT_ID]
                user = stored_token[TokenResponseFields.USER_ID]

            # TODO: this is a hack because the resource requested might be different (loading stored token
            if getattr(self, '_adal_context', None) is None:
                self._adal_context = AuthenticationContext(stored_token[TokenResponseFields._AUTHORITY])
                refresh_token = stored_token[TokenResponseFields.REFRESH_TOKEN]
                token = self._adal_context.acquire_token_with_refresh_token(refresh_token, client_id, self._kusto_cluster)


        if token is None:
            # obtain token from cache and make sure it has not expired
            token = self._adal_context.acquire_token(self._kusto_cluster, user, client_id)
        if token is not None:
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=1):
                return _get_header_from_dict(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = self._adal_context.acquire_token_with_refresh_token(token[TokenResponseFields.REFRESH_TOKEN], client_id, self._kusto_cluster)
                if token is not None:
                    return _get_header_from_dict(token)

        # obtain token from AAD
        if self._authentication_method is AuthenticationMethod.aad_username_password:
            token = self._adal_context.acquire_token_with_username_password(self._kusto_cluster, self._username, self._password, self._client_id)
        elif self._authentication_method is AuthenticationMethod.aad_application_key:
            token = self._adal_context.acquire_token_with_client_credentials(self._kusto_cluster, self._client_id, self._client_secret)
        elif self._authentication_method is AuthenticationMethod.aad_device_login:
            code = self._adal_context.acquire_user_code(self._kusto_cluster, self._client_id)
            print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
            webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
            token = self._adal_context.acquire_token_with_device_code(self._kusto_cluster, code, self._client_id)
        elif self._authentication_method is AuthenticationMethod.aad_application_certificate:
            token = self._adal_context.acquire_token_with_client_certificate(self._kusto_cluster, self._client_id, self._certificate, self._thumbprint)
        else:
            raise KustoClientError("Please choose authentication method from azure.kusto.data.security.AuthenticationMethod")

        return _get_header_from_dict(token)

    def get_token_from_msi(self):
        credentials = None
        try:
            credentials = MSIAuthentication(**self._msi_params)
        except Exception as e:
            raise KustoClientError("Failed to obtain MSI context for [" + str(self._msi_params) + "]\n" + str(e))

        return credentials.token


def _get_header_from_dict(token):
    return _get_header(token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN])


def _get_header(token_type, access_token):
    return "{0} {1}".format(token_type, access_token)

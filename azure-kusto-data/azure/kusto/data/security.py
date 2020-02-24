"""A module to acquire tokens from AAD."""
import json
import os
import webbrowser
from datetime import timedelta, datetime
from enum import Enum, unique
from urllib.parse import urlparse

import dateutil.parser
from adal import AuthenticationContext, AdalError
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters, OAuth2ResponseParameters
from msrestazure.azure_active_directory import MSIAuthentication

from .exceptions import KustoClientError, KustoAuthenticationError


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


class _AadHelperBase:
    authentication_method = None
    auth_context = None
    username = None
    kusto_uri = None
    authority_uri = None
    client_id = None
    password = None
    thumbprint = None
    certificate = None
    msi_params = None

    def __init__(self, kcsb):
        self.kusto_uri = "{0.scheme}://{0.hostname}".format(urlparse(kcsb.data_source))
        self.username = None

        if all([kcsb.aad_user_id, kcsb.password]):
            self.authentication_method = AuthenticationMethod.aad_username_password
            self.client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
            self.username = kcsb.aad_user_id
            self.password = kcsb.password
        elif all([kcsb.application_client_id, kcsb.application_key]):
            self.authentication_method = AuthenticationMethod.aad_application_key
            self.client_id = kcsb.application_client_id
            self.client_secret = kcsb.application_key
        elif all([kcsb.application_client_id, kcsb.application_certificate, kcsb.application_certificate_thumbprint]):
            self.authentication_method = AuthenticationMethod.aad_application_certificate
            self.client_id = kcsb.application_client_id
            self.certificate = kcsb.application_certificate
            self.thumbprint = kcsb.application_certificate_thumbprint
        elif kcsb.msi_authentication:
            self.authentication_method = AuthenticationMethod.aad_msi
            self.msi_params = kcsb.msi_parameters
            return
        elif any([kcsb.user_token, kcsb.application_token]):
            self.token = kcsb.user_token or kcsb.application_token
            self.authentication_method = AuthenticationMethod.aad_token
            return
        elif kcsb.az_cli:
            self.authentication_method = AuthenticationMethod.az_cli_profile
            return
        else:
            self.authentication_method = AuthenticationMethod.aad_device_login
            self.client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

        authority = kcsb.authority_id or "common"
        aad_authority_uri = os.environ.get("AadAuthorityUri", CLOUD_LOGIN_URL)
        self.authority_uri = aad_authority_uri + authority if aad_authority_uri.endswith("/") else aad_authority_uri + "/" + authority

    def _build_kusto_authentication_error(self, error: KustoAuthenticationError) -> KustoAuthenticationError:
        if self.authentication_method is AuthenticationMethod.aad_username_password:
            kwargs = {"username": self.username, "client_id": self.client_id}
        elif self.authentication_method is AuthenticationMethod.aad_application_key:
            kwargs = {"client_id": self.client_id}
        elif self.authentication_method is AuthenticationMethod.aad_device_login:
            kwargs = {"client_id": self.client_id}
        elif self.authentication_method is AuthenticationMethod.aad_application_certificate:
            kwargs = {"client_id": self.client_id, "thumbprint": self.thumbprint}
        elif self.authentication_method is AuthenticationMethod.aad_msi:
            kwargs = self.msi_params
        else:
            return error
        kwargs["resource"] = self.kusto_uri
        if self.authentication_method is AuthenticationMethod.aad_msi:
            kwargs["authority"] = AuthenticationMethod.aad_msi.value
        elif self.auth_context is not None:
            kwargs["authority"] = self.auth_context.authority.url
        return KustoAuthenticationError(self.authentication_method.value, error, **kwargs)

    @staticmethod
    def _get_header(token_type, access_token):
        return "{0} {1}".format(token_type, access_token)

    def _get_header_from_dict(self, token: dict):
        if TokenResponseFields.TOKEN_TYPE in token:
            return self._get_header(token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN])
        elif OAuth2ResponseParameters.TOKEN_TYPE in token:
            # Assume OAuth2 format (e.g. MSI Token)
            return self._get_header(token[OAuth2ResponseParameters.TOKEN_TYPE], token[OAuth2ResponseParameters.ACCESS_TOKEN])
        else:
            raise KustoClientError("Unable to determine the token type. Neither 'tokenType' nor 'token_type' property is present.")

    def _set_from_stored_token(self, stored_token):
        if (
            TokenResponseFields.REFRESH_TOKEN in stored_token
            and TokenResponseFields._CLIENT_ID in stored_token
            and TokenResponseFields._AUTHORITY in stored_token
        ):
            self.client_id = stored_token[TokenResponseFields._CLIENT_ID]
            self.username = stored_token[TokenResponseFields.USER_ID]
            self.authority_uri = stored_token[TokenResponseFields._AUTHORITY]

    @staticmethod
    def _get_expiration_data_from_token(token):
        return dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])

    @staticmethod
    def _authentication_method_missing_exception() -> KustoClientError:
        return KustoClientError("Please choose authentication method from azure.kusto.data.security.AuthenticationMethod")

    @staticmethod
    def _is_expired(expiration_date):
        return expiration_date <= datetime.now() + timedelta(minutes=1)

    def _build_msi_exception(self, e: Exception) -> KustoClientError:
        return KustoClientError("Failed to obtain MSI context for [" + str(self.msi_params) + "]\n" + str(e))

    @staticmethod
    def _load_azure_cli_profile():
        from azure.cli.core._profile import Profile
        from azure.cli.core._session import ACCOUNT
        from azure.cli.core._environment import get_config_dir

        azure_folder = get_config_dir()
        ACCOUNT.load(os.path.join(azure_folder, "azureProfile.json"))
        profile = Profile(storage=ACCOUNT)
        return profile

    @staticmethod
    def _get_env_azure_token_path() -> str:
        folder = os.getenv("AZURE_CONFIG_DIR", None) or os.path.expanduser(os.path.join("~", ".azure"))
        token_path = os.path.join(folder, "accessTokens.json")
        return token_path


class _AadHelper(_AadHelperBase):
    def acquire_authorization_header(self) -> str:
        """Acquire tokens from AAD."""
        try:
            return self._acquire_authorization_header()
        except (AdalError, KustoClientError) as error:
            raise self._build_kusto_authentication_error(error)

    def _acquire_authorization_header(self) -> str:
        # Token was provided by caller
        if self.authentication_method is AuthenticationMethod.aad_token:
            return self._get_header("Bearer", self.token)

        # Obtain token from MSI endpoint
        if self.authentication_method == AuthenticationMethod.aad_msi:
            token = self.get_token_from_msi()
            return self._get_header_from_dict(token)

        refresh_token = None

        if self.authentication_method == AuthenticationMethod.az_cli_profile:
            stored_token = self._get_azure_cli_auth_token()
            self._set_from_stored_token(stored_token)
            refresh_token = stored_token[TokenResponseFields.REFRESH_TOKEN]

        if self.auth_context is None:
            self.auth_context = AuthenticationContext(self.authority_uri)

        if refresh_token is not None:
            token = self.auth_context.acquire_token_with_refresh_token(refresh_token, self.client_id, self.kusto_uri)
        else:
            token = self.auth_context.acquire_token(self.kusto_uri, self.username, self.client_id)

        if token is not None:
            expiration_date = self._get_expiration_data_from_token(token)
            if not self._is_expired(expiration_date):
                return self._get_header_from_dict(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = self.auth_context.acquire_token_with_refresh_token(token[TokenResponseFields.REFRESH_TOKEN], self.client_id, self.kusto_uri)
                if token is not None:
                    return self._get_header_from_dict(token)

        # obtain token from AAD
        if self.authentication_method is AuthenticationMethod.aad_username_password:
            token = self.auth_context.acquire_token_with_username_password(self.kusto_uri, self.username, self.password, self.client_id)
        elif self.authentication_method is AuthenticationMethod.aad_application_key:
            token = self.auth_context.acquire_token_with_client_credentials(self.kusto_uri, self.client_id, self.client_secret)
        elif self.authentication_method is AuthenticationMethod.aad_device_login:
            code = self.auth_context.acquire_user_code(self.kusto_uri, self.client_id)
            print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
            webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
            token = self.auth_context.acquire_token_with_device_code(self.kusto_uri, code, self.client_id)
        elif self.authentication_method is AuthenticationMethod.aad_application_certificate:
            token = self.auth_context.acquire_token_with_client_certificate(self.kusto_uri, self.client_id, self.certificate, self.thumbprint)
        else:
            raise self._authentication_method_missing_exception()

        return self._get_header_from_dict(token)

    def get_token_from_msi(self) -> dict:
        try:
            credentials = MSIAuthentication(**self.msi_params)
        except Exception as e:
            raise self._build_msi_exception(e)

        return credentials.token

    def _get_azure_cli_auth_token(self) -> dict:
        """
        Try to get the az cli authenticated token
        :return: refresh token
        """
        try:
            profile = self._load_azure_cli_profile()
            token_data = profile.get_raw_token()[0][2]

            return token_data
        except ModuleNotFoundError:
            try:
                token_path = self._get_env_azure_token_path()
                with open(token_path) as f:
                    data = json.load(f)

                # TODO: not sure I should take the first
                return data[0]
            except Exception:
                pass

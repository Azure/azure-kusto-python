# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import webbrowser
from datetime import timedelta, datetime
from enum import Enum, unique
from urllib.parse import urlparse

import dateutil.parser
from adal import AuthenticationContext, AdalError
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters, OAuth2ResponseParameters
from azure.identity import ManagedIdentityCredential
from azure.core.credentials import AccessToken

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
        except Exception as e:
            raise KustoClientError("Azure cli token was not found. Please run 'az login' to setup account.", e)


@unique
class AuthenticationMethod(Enum):
    """Enum representing all authentication methods available in Kusto with Python."""

    aad_username_password = "aad_username_password"
    aad_application_key = "aad_application_key"
    aad_application_certificate = "aad_application_certificate"
    aad_application_certificate_sni = "aad_application_certificate_sni"
    aad_device_login = "aad_device_login"
    aad_token = "aad_token"
    managed_service_identity = "managed_service_identity"
    az_cli_profile = "az_cli_profile"
    token_provider = "token_provider"


CLOUD_LOGIN_URL = "https://login.microsoftonline.com/"


class _AadHelper:
    authentication_method = None
    auth_context = None
    msi_auth_context = None
    username = None
    kusto_uri = None
    authority_uri = None
    client_id = None
    password = None
    thumbprint = None
    private_certificate = None
    public_certificate = None
    msi_params = None
    token_provider = None

    def __init__(self, kcsb: "KustoConnectionStringBuilder"):
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
            self.client_id = kcsb.application_client_id
            self.private_certificate = kcsb.application_certificate
            self.thumbprint = kcsb.application_certificate_thumbprint
            if all([kcsb.application_public_certificate]):
                self.public_certificate = kcsb.application_public_certificate
                self.authentication_method = AuthenticationMethod.aad_application_certificate_sni
            else:
                self.authentication_method = AuthenticationMethod.aad_application_certificate

        elif kcsb.msi_authentication:
            self.authentication_method = AuthenticationMethod.managed_service_identity
            self.msi_params = kcsb.msi_parameters
            return
        elif any([kcsb.user_token, kcsb.application_token]):
            self.token = kcsb.user_token or kcsb.application_token
            self.authentication_method = AuthenticationMethod.aad_token
            return
        elif kcsb.az_cli:
            self.authentication_method = AuthenticationMethod.az_cli_profile
            return
        elif kcsb.token_provider:
            self.authentication_method = AuthenticationMethod.token_provider
            self.token_provider = kcsb.token_provider
        else:
            self.authentication_method = AuthenticationMethod.aad_device_login
            self.client_id = "db662dc1-0cfe-4e1c-a843-19a68e65be58"

        authority = kcsb.authority_id or "common"
        aad_authority_uri = os.environ.get("AadAuthorityUri", CLOUD_LOGIN_URL)
        self.authority_uri = aad_authority_uri + authority if aad_authority_uri.endswith("/") else aad_authority_uri + "/" + authority

    def acquire_authorization_header(self):
        """Acquire tokens from AAD."""
        try:
            return self._acquire_authorization_header()
        except (AdalError, KustoClientError) as error:
            if self.authentication_method is AuthenticationMethod.aad_username_password:
                kwargs = {"username": self.username, "client_id": self.client_id}
            elif self.authentication_method is AuthenticationMethod.aad_application_key:
                kwargs = {"client_id": self.client_id}
            elif self.authentication_method is AuthenticationMethod.aad_device_login:
                kwargs = {"client_id": self.client_id}
            elif self.authentication_method in (AuthenticationMethod.aad_application_certificate, AuthenticationMethod.aad_application_certificate_sni):
                kwargs = {"client_id": self.client_id, "thumbprint": self.thumbprint}
            elif self.authentication_method is AuthenticationMethod.managed_service_identity:
                kwargs = self.msi_params
            elif self.authentication_method is AuthenticationMethod.token_provider:
                kwargs = {}
            else:
                raise error

            kwargs["resource"] = self.kusto_uri

            if self.authentication_method is AuthenticationMethod.managed_service_identity:
                kwargs["authority"] = AuthenticationMethod.managed_service_identity.value
            elif self.authentication_method is AuthenticationMethod.token_provider:
                kwargs["authority"] = AuthenticationMethod.token_provider.value
            elif self.auth_context is not None:
                kwargs["authority"] = self.auth_context.authority.url
            elif self.authentication_method is AuthenticationMethod.az_cli_profile:
                kwargs["authority"] = AuthenticationMethod.az_cli_profile.value

            raise KustoAuthenticationError(self.authentication_method.value, error, **kwargs)

    def _acquire_authorization_header(self) -> str:
        # Token was provided by caller
        if self.authentication_method is AuthenticationMethod.aad_token:
            return _get_header("Bearer", self.token)

        if self.authentication_method is AuthenticationMethod.token_provider:
            caller_token = self.token_provider()
            if not isinstance(caller_token, str):
                raise KustoClientError("Token provider returned something that is not a string [" + str(type(caller_token)) + "]")

            return _get_header("Bearer", caller_token)

        # Obtain token from MSI endpoint
        if self.authentication_method == AuthenticationMethod.managed_service_identity:
            msi_token = self.get_token_from_msi()
            return _get_header("Bearer", msi_token.token)

        refresh_token = None

        if self.authentication_method == AuthenticationMethod.az_cli_profile:
            stored_token = _get_azure_cli_auth_token()

            if (
                TokenResponseFields.REFRESH_TOKEN in stored_token
                and TokenResponseFields._CLIENT_ID in stored_token
                and TokenResponseFields._AUTHORITY in stored_token
            ):
                self.client_id = stored_token[TokenResponseFields._CLIENT_ID]
                self.username = stored_token[TokenResponseFields.USER_ID]
                self.authority_uri = stored_token[TokenResponseFields._AUTHORITY]
                refresh_token = stored_token[TokenResponseFields.REFRESH_TOKEN]

        if self.auth_context is None:
            self.auth_context = AuthenticationContext(self.authority_uri)

        if refresh_token is not None:
            token = self.auth_context.acquire_token_with_refresh_token(refresh_token, self.client_id, self.kusto_uri)
        else:
            token = self.auth_context.acquire_token(self.kusto_uri, self.username, self.client_id)

        if token is not None:
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=1):
                return _get_header_from_dict(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = self.auth_context.acquire_token_with_refresh_token(token[TokenResponseFields.REFRESH_TOKEN], self.client_id, self.kusto_uri)
                if token is not None:
                    return _get_header_from_dict(token)

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
        elif self.authentication_method in (AuthenticationMethod.aad_application_certificate, AuthenticationMethod.aad_application_certificate_sni):
            token = self.auth_context.acquire_token_with_client_certificate(
                self.kusto_uri, self.client_id, self.private_certificate, self.thumbprint, self.public_certificate
            )
        else:
            raise KustoClientError("Please choose authentication method from azure.kusto.data.security.AuthenticationMethod")

        return _get_header_from_dict(token)

    def get_token_from_msi(self) -> AccessToken:
        try:
            if self.msi_auth_context is None:
                # Create the MSI Authentication object
                self.msi_auth_context = ManagedIdentityCredential(**self.msi_params)

            return self.msi_auth_context.get_token(self.kusto_uri)

        except Exception as e:
            raise KustoClientError("Failed to obtain MSI context for [" + str(self.msi_params) + "]\n" + str(e))


def _get_header_from_dict(token: dict):
    if TokenResponseFields.TOKEN_TYPE in token:
        return _get_header(token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN])
    elif OAuth2ResponseParameters.TOKEN_TYPE in token:
        # Assume OAuth2 format (e.g. MSI Token)
        return _get_header(token[OAuth2ResponseParameters.TOKEN_TYPE], token[OAuth2ResponseParameters.ACCESS_TOKEN])
    else:
        raise KustoClientError("Unable to determine the token type. Neither 'tokenType' nor 'token_type' property is present.")


def _get_header(token_type: str, access_token: str) -> str:
    return "{0} {1}".format(token_type, access_token)

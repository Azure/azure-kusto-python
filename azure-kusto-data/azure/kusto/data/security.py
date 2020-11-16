# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import webbrowser
from datetime import timedelta, datetime
from enum import Enum, unique
from urllib.parse import urlparse

import dateutil.parser
# Todo  remove Adal
from adal import AuthenticationContext, AdalError
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters, OAuth2ResponseParameters

from msal import ConfidentialClientApplication, PublicClientApplication
from azure.identity import ManagedIdentityCredential
from azure.core.credentials import AccessToken

from .exceptions import KustoClientError, KustoAuthenticationError
from ._cloud_settings import CloudSettings, CloudInfo
from ._token_providers import *


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
    token_provider_callback = "token_provider_callback"


class _AadHelper:
    authentication_method = None
    auth_context = None # Todo remove this
    msi_auth_context = None
    msal_client_application = None
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

        cloud_info = CloudSettings.get_cloud_info(self.kusto_uri)
        if cloud_info is None:
            raise KustoClientError("Unable to detect cloud instance from DNS Suffix of Kusto Connection String [" + self.kusto_uri + "]")

        if all([kcsb.aad_user_id, kcsb.password]):
            self.authentication_method = AuthenticationMethod.aad_username_password
            self.client_id = cloud_info.kusto_client_app_id
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
            self.token_provider = MsiTokenProvider(self.kusto_uri, kcsb.msi_parameters)
            return
        elif any([kcsb.user_token, kcsb.application_token]):
            self.token_provider = BasicTokenProvider(kcsb.user_token or kcsb.application_token)
            self.authentication_method = AuthenticationMethod.aad_token
            return
        elif kcsb.az_cli:
            self.authentication_method = AuthenticationMethod.az_cli_profile
            self.token_provider = AzCliTokenProvider(self.kusto_uri)
            return
        elif kcsb.token_provider:
            self.authentication_method = AuthenticationMethod.token_provider_callback
            self.token_provider = CallbackTokenProvider(kcsb.token_provider)
        else:
            self.authentication_method = AuthenticationMethod.aad_device_login
            self.client_id = cloud_info.kusto_client_app_id

        authority = kcsb.authority_id or "common"
        aad_authority_uri = cloud_info.aad_authority_uri
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
            elif self.authentication_method is AuthenticationMethod.token_provider_callback:
                kwargs = {}
            else:
                raise error

            kwargs["resource"] = self.kusto_uri

            if self.authentication_method is AuthenticationMethod.managed_service_identity:
                kwargs["authority"] = AuthenticationMethod.managed_service_identity.value
            elif self.authentication_method is AuthenticationMethod.token_provider_callback:
                kwargs["authority"] = AuthenticationMethod.token_provider_callback.value
            elif self.authentication_method is AuthenticationMethod.az_cli_profile:
                kwargs["authority"] = AuthenticationMethod.az_cli_profile.value
            elif self.authority_uri is not None:
                kwargs["authority"] = self.authority_uri

            raise KustoAuthenticationError(self.authentication_method.value, error, **kwargs)

    def _acquire_authorization_header(self) -> str:
        # Token was provided by caller
        if self.authentication_method is AuthenticationMethod.aad_token:
            return _get_header("Bearer", self.token_provider.get_token())

        # Token provider callback was provided by caller
        if self.authentication_method is AuthenticationMethod.token_provider_callback:
            return _get_header("Bearer", self.token_provider.get_token())

        # Obtain token from MSI endpoint
        if self.authentication_method == AuthenticationMethod.managed_service_identity:
            return _get_header("Bearer", self.token_provider.get_token())

        # Obtain token from AzCli
        if self.authentication_method == AuthenticationMethod.az_cli_profile:
            return _get_header_from_dict(self.token_provider.get_token())

        if self.auth_context is None: # todo remove this
            self.auth_context = AuthenticationContext(self.authority_uri)

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

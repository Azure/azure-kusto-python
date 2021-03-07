# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from urllib.parse import urlparse
from .exceptions import KustoAuthenticationError
from ._token_providers import *


class _AadHelper:
    kusto_uri = None
    authority_uri = None
    token_provider = None

    def __init__(self, kcsb: "KustoConnectionStringBuilder"):
        self.kusto_uri = "{0.scheme}://{0.hostname}".format(urlparse(kcsb.data_source))
        self.username = None

        cloud_info = CloudSettings.get_cloud_info()
        authority = kcsb.authority_id or "organizations"
        aad_authority_uri = cloud_info.aad_authority_uri
        self.authority_uri = aad_authority_uri + authority if aad_authority_uri.endswith("/") else aad_authority_uri + "/" + authority

        if kcsb.interactive_login:
            self.token_provider = InteractiveLoginTokenProvider(self.kusto_uri, self.authority_uri, kcsb.login_hint, kcsb.domain_hint)
        elif all([kcsb.aad_user_id, kcsb.password]):
            self.token_provider = UserPassTokenProvider(self.kusto_uri, self.authority_uri, kcsb.aad_user_id, kcsb.password)
        elif all([kcsb.application_client_id, kcsb.application_key]):
            self.token_provider = ApplicationKeyTokenProvider(self.kusto_uri, self.authority_uri, kcsb.application_client_id, kcsb.application_key)
        elif all([kcsb.application_client_id, kcsb.application_certificate, kcsb.application_certificate_thumbprint]):
            # kcsb.application_public_certificate can be None if SNI is not used
            self.token_provider = ApplicationCertificateTokenProvider(
                self.kusto_uri,
                kcsb.application_client_id,
                self.authority_uri,
                kcsb.application_certificate,
                kcsb.application_certificate_thumbprint,
                kcsb.application_public_certificate,
            )
        elif kcsb.msi_authentication:
            self.token_provider = MsiTokenProvider(self.kusto_uri, kcsb.msi_parameters)
        elif kcsb.user_token:
            self.token_provider = BasicTokenProvider(kcsb.user_token)
        elif kcsb.application_token:
            self.token_provider = BasicTokenProvider(kcsb.application_token)
        elif kcsb.az_cli:
            self.token_provider = AzCliTokenProvider(self.kusto_uri)
        elif kcsb.token_provider:
            self.token_provider = CallbackTokenProvider(kcsb.token_provider)
        else:
            self.token_provider = DeviceLoginTokenProvider(self.kusto_uri, self.authority_uri)

    def acquire_authorization_header(self):
        try:
            return _get_header_from_dict(self.token_provider.get_token())
        except Exception as error:
            kwargs = self.token_provider.context()
            kwargs["kusto_uri"] = self.kusto_uri
            raise KustoAuthenticationError(self.token_provider.name(), error, **kwargs)

    async def acquire_authorization_header_async(self):
        try:
            return _get_header_from_dict(await self.token_provider.get_token_async())
        except Exception as error:
            kwargs = self.token_provider.context()
            kwargs["resource"] = self.kusto_uri
            raise KustoAuthenticationError(self.token_provider.name(), error, **kwargs)


def _get_header_from_dict(token: dict):
    if TokenConstants.MSAL_ACCESS_TOKEN in token:
        return _get_header(token[TokenConstants.MSAL_TOKEN_TYPE], token[TokenConstants.MSAL_ACCESS_TOKEN])
    elif TokenConstants.AZ_ACCESS_TOKEN in token:
        return _get_header(token[TokenConstants.AZ_TOKEN_TYPE], token[TokenConstants.AZ_ACCESS_TOKEN])
    else:
        raise KustoClientError("Unable to determine the token type. Neither 'tokenType' nor 'token_type' property is present.")


def _get_header(token_type: str, access_token: str) -> str:
    return "{0} {1}".format(token_type, access_token)

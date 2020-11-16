# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import webbrowser
from datetime import timedelta, datetime
from enum import Enum, unique
from urllib.parse import urlparse
from typing import Callable
import abc

import dateutil.parser
# Todo  remove Adal
from adal import AuthenticationContext, AdalError
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters, OAuth2ResponseParameters

from msal import ConfidentialClientApplication, PublicClientApplication
from azure.identity import ManagedIdentityCredential
from azure.core.credentials import AccessToken

from .exceptions import KustoClientError, KustoAuthenticationError
from ._cloud_settings import CloudSettings, CloudInfo


class TokenProviderBase(abc.ABC):
    """
    This base class abstracts token acquisition for all implementations.
    The class is build for Lazy initialization, so that the first call, take on instantiation of 'heavy' long-lived class members
    """
    _initialized = False
    _kusto_uri = None
    _cloud_info = None
    _scopes = None

    def __init__(self, kusto_uri: str):
        self._kusto_uri = kusto_uri
        if kusto_uri is not None:
            self._scopes = [kusto_uri + ".defult" if kusto_uri.endswith("/") else kusto_uri + "/.default"]
            self._cloud_info = CloudSettings.get_cloud_info(kusto_uri)

    def get_token(self):
        if not self._initialized:
            self._init_impl()

        token = self._get_token_silent_impl()
        if token is None:
            self._get_token_impl()

        return token

    @abc.abstractmethod
    def _init_impl(self):
        pass

    @abc.abstractmethod
    def _get_token_impl(self) -> str:
        pass

    @abc.abstractmethod
    def _get_token_silent_impl(self) -> str:
        pass

class BasicTokenProvider(TokenProviderBase):
    """ Basic Token Provider keeps and returns a token received on construction """
    def __init__(self, token: str):
        super().__init__(None)
        self._token = token

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> str:
        return self._token

    def _get_token_silent_impl(self) -> str:
        return self._token


class CallbackTokenProvider(TokenProviderBase):
    """ Callback Token Provider generates a token based on a callback function provided by the caller """
    def __init__(self, token_callback: Callable[[], str]):
        super().__init__(None)
        self._token_callback = token_callback

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> str:
        caller_token = self._token_callback()
        if not isinstance(caller_token, str):
            raise KustoClientError("Token provider returned something that is not a string [" + str(type(caller_token)) + "]")

        return caller_token

    def _get_token_silent_impl(self) -> str:
        return self._get_token_impl()


class MsiTokenProvider(TokenProviderBase):
    """
    MSI Token Provider obtains a token from the MSI endpoint
    The args parameter is a dictionary conforming with the ManagedIdentityCredential initializer API arguments
    """
    def __init__(self, kusto_uri: str, msi_args):
        super().__init__(kusto_uri)
        self._msi_args = msi_args
        self._msi_auth_context = None

    def _init_impl(self):
        try:
            self.msi_auth_context = ManagedIdentityCredential(**self._msi_args)
        except Exception as e:
            raise KustoClientError("Failed to initialize MSI ManagedIdentityCredential with [" + str(self._msi_params) + "]\n" + str(e))

    def _get_token_impl(self) -> str:
        try:
            return self.msi_auth_context.get_token(self._kusto_uri)
        except Exception as e:
            raise KustoClientError("Failed to obtain MSI token for '" + self._kusto_uri + "' with [" + str(self._msi_params) + "]\n" + str(e))

    def _get_token_silent_impl(self) -> str:
        return self._get_token_impl()


class AzCliTokenProvider(TokenProviderBase):
    """ Todo Write Something Here """
    def __init__(self, kusto_uri: str):
        super().__init__(kusto_uri)
        self._msal_client = None
        self._client_id = None
        self._authority_id = None

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> str:
        # try and obtain the refresh token from AzCli
        refresh_token = None
        stored_token = _get_azure_cli_auth_token()
        if (
                TokenResponseFields.REFRESH_TOKEN in stored_token
                and TokenResponseFields._CLIENT_ID in stored_token
                and TokenResponseFields._AUTHORITY in stored_token
        ):
            refresh_token = stored_token[TokenResponseFields.REFRESH_TOKEN]
            self._client_id = stored_token[TokenResponseFields._CLIENT_ID]
            self._authority_uri = stored_token[TokenResponseFields._AUTHORITY]
            # todo delete if not used
            # username = stored_token[TokenResponseFields.USER_ID]
        else:
            raise KustoClientError("Unable to obtain a refresh token from Az-Cli")

        if self._msal_client is None:
            self._msal_client = PublicClientApplication(client_id=self._client_id, authority=self._authority_uri)

        if refresh_token is not None:
            self._msal_client.acquire_token_by_refresh_token(refresh_token, self._scopes)

    def _get_token_silent_impl(self) -> str:
        if self._msal_client is not None:
            self._msal_client.acquire_token_silent(scopes=self._scopes, account=None, client_id=self._client_id, authority=self._authority_uri)


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


# todo do not change this
class NextTokenProvider(TokenProviderBase):
    """ Todo Write Something Here """
    def __init__(self, kusto_uri: str):
        self._kusto_uri = kusto_uri

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> str:
        pass

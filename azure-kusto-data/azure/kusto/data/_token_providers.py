# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import abc
import webbrowser
from typing import Callable
from msal import ConfidentialClientApplication, PublicClientApplication
from azure.identity import ManagedIdentityCredential
from .exceptions import KustoClientError
from ._cloud_settings import CloudSettings


# constant key names and values used throughout the code
class TokenConstants:
    BEARER_TYPE = "Bearer"
    MSAL_TOKEN_TYPE = "token_type"
    MSAL_ACCESS_TOKEN = "access_token"
    MSAL_ERROR = "error"
    MSAL_ERROR_DESCRIPTION = "error_description"
    MSAL_PRIVATE_CERT = "private_key"
    MSAL_THUMBPRINT = "thumbprint"
    MSAL_PUBLIC_CERT = "public_certificate"
    MSAL_DEVICE_MSG = "message"
    MSAL_DEVICE_URI = "verification_uri"
    AZ_TOKEN_TYPE = "tokenType"
    AZ_ACCESS_TOKEN = "accessToken"
    AZ_REFRESH_TOKEN = "refreshToken"
    AZ_USER_ID = "userId"
    AZ_AUTHORITY = "_authority"
    AZ_CLIENT_ID = "_clientId"


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
            self._cloud_info = CloudSettings.get_cloud_info()

    def get_token(self):
        """ Get a token silently from cache or authenticate if cached token is not found """
        if not self._initialized:
            self._init_impl()
            self._initialized = True

        token = self._get_token_from_cache_impl()
        if token is None:
            token = self._get_token_impl()

        return self._valid_token_or_throw(token)

    @staticmethod
    @abc.abstractmethod
    def name() -> str:
        """ return the provider class name """
        pass

    @abc.abstractmethod
    def context(self) -> dict:
        """ return a secret-free context for error reporting """
        pass

    @abc.abstractmethod
    def _init_impl(self):
        """ Implement any "heavy" first time initializations here """
        pass

    @abc.abstractmethod
    def _get_token_impl(self) -> dict:
        """ implement actual token acquisition here """
        pass

    @abc.abstractmethod
    def _get_token_from_cache_impl(self) -> dict:
        """ Implement cache checks here, return None if cache check fails """
        pass

    @staticmethod
    def _valid_token_or_none(token: dict) -> dict:
        if token is None or TokenConstants.MSAL_ERROR in token:
            return None

        return token

    def _valid_token_or_throw(self, token: dict, context: str = "") -> dict:
        if token is None:
            raise KustoClientError(self.name() + " - failed to obtain a token. " + context)

        if TokenConstants.MSAL_ERROR in token:
            message = self.name() + " - failed to obtain a token. " + context + "\n" + token[TokenConstants.MSAL_ERROR]
            if TokenConstants.MSAL_ERROR_DESCRIPTION in token:
                message = message + "\n" + token[TokenConstants.MSAL_ERROR_DESCRIPTION]

            raise KustoClientError(message)

        return token


class BasicTokenProvider(TokenProviderBase):
    """ Basic Token Provider keeps and returns a token received on construction """

    def __init__(self, token: str):
        super().__init__(None)
        self._token = token

    @staticmethod
    def name() -> str:
        return "BasicTokenProvider"

    def context(self) -> dict:
        return {"authority": self.name()}

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> dict:
        return None

    def _get_token_from_cache_impl(self) -> dict:
        return {TokenConstants.MSAL_TOKEN_TYPE: TokenConstants.BEARER_TYPE, TokenConstants.MSAL_ACCESS_TOKEN: self._token}


class CallbackTokenProvider(TokenProviderBase):
    """ Callback Token Provider generates a token based on a callback function provided by the caller """

    def __init__(self, token_callback: Callable[[], str]):
        super().__init__(None)
        self._token_callback = token_callback

    @staticmethod
    def name() -> str:
        return "CallbackTokenProvider"

    def context(self) -> dict:
        return {"authority": self.name()}

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> dict:
        return None

    def _get_token_from_cache_impl(self) -> dict:
        caller_token = self._token_callback()
        if not isinstance(caller_token, str):
            raise KustoClientError("Token provider returned something that is not a string [" + str(type(caller_token)) + "]")

        return {TokenConstants.MSAL_TOKEN_TYPE: TokenConstants.BEARER_TYPE, TokenConstants.MSAL_ACCESS_TOKEN: caller_token}


class MsiTokenProvider(TokenProviderBase):
    """
    MSI Token Provider obtains a token from the MSI endpoint
    The args parameter is a dictionary conforming with the ManagedIdentityCredential initializer API arguments
    """

    def __init__(self, kusto_uri: str, msi_args):
        super().__init__(kusto_uri)
        self._msi_args = msi_args
        self._msi_auth_context = None

    @staticmethod
    def name() -> str:
        return "MsiTokenProvider"

    def context(self) -> dict:
        context = self._msi_args.copy()
        context["authority"] = self.name()
        return context

    def _init_impl(self):
        try:
            self.msi_auth_context = ManagedIdentityCredential(**self._msi_args)
        except Exception as e:
            raise KustoClientError("Failed to initialize MSI ManagedIdentityCredential with [" + str(self._msi_params) + "]\n" + str(e))

    def _get_token_impl(self) -> dict:
        return None

    def _get_token_from_cache_impl(self) -> dict:
        try:
            msi_token = self.msi_auth_context.get_token(self._kusto_uri)
            return {TokenConstants.MSAL_TOKEN_TYPE: TokenConstants.BEARER_TYPE, TokenConstants.MSAL_ACCESS_TOKEN: msi_token.token}
        except Exception as e:
            raise KustoClientError("Failed to obtain MSI token for '" + self._kusto_uri + "' with [" + str(self._msi_params) + "]\n" + str(e))


class AzCliTokenProvider(TokenProviderBase):
    """ AzCli Token Provider obtains a refresh token from the AzCli cache and uses it to authenticate with MSAL """

    def __init__(self, kusto_uri: str):
        super().__init__(kusto_uri)
        self._msal_client = None
        self._client_id = None
        self._authority_uri = None
        self._username = None

    @staticmethod
    def name() -> str:
        return "AzCliTokenProvider"

    def context(self) -> dict:
        return {"authority:": self.name()}

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> dict:
        # try and obtain the refresh token from AzCli
        refresh_token = None
        token = None
        stored_token = self._get_azure_cli_auth_token()
        if TokenConstants.AZ_REFRESH_TOKEN in stored_token and TokenConstants.AZ_CLIENT_ID in stored_token and TokenConstants.AZ_AUTHORITY in stored_token:
            refresh_token = stored_token[TokenConstants.AZ_REFRESH_TOKEN]
            self._client_id = stored_token[TokenConstants.AZ_CLIENT_ID]
            self._authority_uri = stored_token[TokenConstants.AZ_AUTHORITY]
            self._username = stored_token[TokenConstants.AZ_USER_ID]
        else:
            raise KustoClientError("Unable to obtain a refresh token from Az-Cli. Calling 'az login' may fix this issue.")

        if self._msal_client is None:
            self._msal_client = PublicClientApplication(client_id=self._client_id, authority=self._authority_uri)

        try:
            token = self._msal_client.acquire_token_by_refresh_token(refresh_token, self._scopes)
        except Exception as ex:
            raise KustoClientError("Unable to obtain with Az-Cli refresh token. Calling 'az login' may fix this issue.\n" + str(ex))

        return self._valid_token_or_throw(token, "Calling 'az login' may fix this issue.")

    def _get_token_from_cache_impl(self) -> dict:
        token = None
        if self._msal_client is not None:
            account = None
            if self._username is not None:
                accounts = self._msal_client.get_accounts(self._username)
                if len(accounts) > 0:
                    account = accounts[0]

            token = self._msal_client.acquire_token_silent(scopes=self._scopes, account=account, client_id=self._client_id, authority=self._authority_uri)

        return self._valid_token_or_none(token)

    @staticmethod
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


class UserPassTokenProvider(TokenProviderBase):
    """ Acquire a token from MSAL with username and password """

    def __init__(self, kusto_uri: str, authority_uri: str, username: str, password: str):
        super().__init__(kusto_uri)
        self._msal_client = None
        self._auth = authority_uri
        self._user = username
        self._pass = password

    @staticmethod
    def name() -> str:
        return "UserPassTokenProvider"

    def context(self) -> dict:
        return {"authority": self._auth, "client_id": self._cloud_info.kusto_client_app_id, "username": self._user}

    def _init_impl(self):
        self._msal_client = PublicClientApplication(client_id=self._cloud_info.kusto_client_app_id, authority=self._auth)

    def _get_token_impl(self) -> dict:
        token = self._msal_client.acquire_token_by_username_password(username=self._user, password=self._pass, scopes=self._scopes)
        return self._valid_token_or_throw(token)

    def _get_token_from_cache_impl(self) -> dict:
        account = None
        if self._user is not None:
            accounts = self._msal_client.get_accounts(self._user)
            if len(accounts) > 0:
                account = accounts[0]

        token = self._msal_client.acquire_token_silent(scopes=self._scopes, account=account)
        return self._valid_token_or_none(token)


class DeviceLoginTokenProvider(TokenProviderBase):
    """ Acquire a token from MSAL with Device Login flow """

    def __init__(self, kusto_uri: str, authority_uri: str, device_code_callback=None):
        super().__init__(kusto_uri)
        self._msal_client = None
        self._auth = authority_uri
        self._account = None
        self._device_code_callback = device_code_callback

    @staticmethod
    def name() -> str:
        return "DeviceLoginTokenProvider"

    def context(self) -> dict:
        return {"authority": self._auth, "client_id": self._cloud_info.kusto_client_app_id}

    def _init_impl(self):
        self._msal_client = PublicClientApplication(client_id=self._cloud_info.kusto_client_app_id, authority=self._auth)

    def _get_token_impl(self) -> dict:
        flow = self._msal_client.initiate_device_flow(scopes=self._scopes)
        try:
            if self._device_code_callback:
                self._device_code_callback(flow[TokenConstants.MSAL_DEVICE_MSG])
            else:
                print(flow[TokenConstants.MSAL_DEVICE_MSG])

            webbrowser.open(flow[TokenConstants.MSAL_DEVICE_URI])
        except KeyError:
            raise KustoClientError("Failed to initiate device code flow")

        token = self._msal_client.acquire_token_by_device_flow(flow)

        # Keep the account for slient login
        if self._valid_token_or_none(token) is not None:
            accounts = self._msal_client.get_accounts()
            if len(accounts) == 1:
                self._account = accounts[0]

        return self._valid_token_or_throw(token)

    def _get_token_from_cache_impl(self) -> dict:
        token = self._msal_client.acquire_token_silent(scopes=self._scopes, account=self._account)
        return self._valid_token_or_none(token)


class ApplicationKeyTokenProvider(TokenProviderBase):
    """ Acquire a token from MSAL with application Id and Key """

    def __init__(self, kusto_uri: str, authority_uri: str, app_client_id: str, app_key: str):
        super().__init__(kusto_uri)
        self._msal_client = None
        self._app_client_id = app_client_id
        self._app_key = app_key
        self._auth = authority_uri

    @staticmethod
    def name() -> str:
        return "ApplicationKeyTokenProvider"

    def context(self) -> dict:
        return {"authority": self._auth, "client_id": self._app_client_id}

    def _init_impl(self):
        self._msal_client = ConfidentialClientApplication(client_id=self._app_client_id, client_credential=self._app_key, authority=self._auth)

    def _get_token_impl(self) -> dict:
        token = self._msal_client.acquire_token_for_client(scopes=self._scopes)
        return self._valid_token_or_throw(token)

    def _get_token_from_cache_impl(self) -> dict:
        token = self._msal_client.acquire_token_silent(scopes=self._scopes, account=None)
        return self._valid_token_or_none(token)


class ApplicationCertificateTokenProvider(TokenProviderBase):
    """
    Acquire a token from MSAL using application certificate
    Passing the public certificate is optional and will result in Subject Name & Issuer Authentication
    """

    def __init__(self, kusto_uri: str, client_id: str, authority_uri: str, private_cert: str, thumbprint: str, public_cert: str = None):
        super().__init__(kusto_uri)
        self._msal_client = None
        self._auth = authority_uri
        self._client_id = client_id
        self._cert_credentials = {TokenConstants.MSAL_PRIVATE_CERT: private_cert, TokenConstants.MSAL_THUMBPRINT: thumbprint}
        if public_cert is not None:
            self._cert_credentials[TokenConstants.MSAL_PUBLIC_CERT] = public_cert

    @staticmethod
    def name() -> str:
        return "ApplicationCertificateTokenProvider"

    def context(self) -> dict:
        return {"authority": self._auth, "client_id": self._app_client_id, "thumbprint": self._cert_credentials[TokenConstants.MSAL_THUMBPRINT]}

    def _init_impl(self):
        self._msal_client = ConfidentialClientApplication(client_id=self._client_id, client_credential=self._cert_credentials, authority=self._auth)

    def _get_token_impl(self) -> dict:
        token = self._msal_client.acquire_token_for_client(scopes=self._scopes)
        return self._valid_token_or_throw(token)

    def _get_token_from_cache_impl(self) -> dict:
        token = self._msal_client.acquire_token_silent(scopes=self._scopes, account=None)
        return self._valid_token_or_none(token)

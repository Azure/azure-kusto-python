# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import abc
import asyncio
from threading import Lock
from typing import Callable, Coroutine, List, Optional, Type

from azure.core.credentials import TokenCredential
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import (
    AzureCliCredential,
    ManagedIdentityCredential,
    UsernamePasswordCredential,
    DeviceCodeCredential,
    InteractiveBrowserCredential,
    ClientSecretCredential,
    CertificateCredential,
    DefaultAzureCredential,
)

from ._cloud_settings import CloudInfo, CloudSettings
from .exceptions import KustoAioSyntaxError, KustoAsyncUsageError, KustoClientError

try:
    from asgiref.sync import sync_to_async
except ImportError:

    def sync_to_async(f):
        raise KustoAioSyntaxError()


try:
    from azure.identity.aio import (
        ManagedIdentityCredential as AsyncManagedIdentityCredential,
        AzureCliCredential as AsyncAzureCliCredential,
        ClientSecretCredential as AsyncClientSecretCredential,
        CertificateCredential as AsyncCertificateCredential,
        DefaultAzureCredential as AsyncDefaultAzureCredential,
    )
except ImportError:

    # These are here in case the user doesn't have the aio optional dependency installed, but still tries to use async.
    # They will give them a useful error message, and will appease linters.
    class AsyncManagedIdentityCredential:
        def __init__(self):
            raise KustoAioSyntaxError()

    class AsyncAzureCliCredential:
        def __init__(self):
            raise KustoAioSyntaxError()

    class AsyncClientSecretCredential:
        def __init__(self):
            raise KustoAioSyntaxError()

    class AsyncCertificateCredential:
        def __init__(self):
            raise KustoAioSyntaxError()

    class AsyncDefaultAzureCredential:
        def __init__(self):
            raise KustoAioSyntaxError()


class TokenConstants:
    BEARER_TYPE = "Bearer"
    TOKEN_TYPE = "token_type"
    ACCESS_TOKEN = "access_token"


class TokenProviderBase(abc.ABC):
    """
    This base class abstracts token acquisition for all implementations.
    The class is build for Lazy initialization, so that the first call, take on instantiation of 'heavy' long-lived class members
    """

    _initialized: bool = False
    _resources_initialized: bool = False

    def __init__(self, is_async: bool = False):
        self._proxy_dict: Optional[str, str] = None
        self.is_async = is_async

        if is_async:
            self._async_lock = asyncio.Lock()
        else:
            self._lock = Lock()

    def close(self):
        pass

    def _init_once(self, init_only_resources=False):
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            if not self._resources_initialized:
                self._init_resources()
                self._resources_initialized = True

            if init_only_resources:
                return

            self._init_impl()
            self._initialized = True

    async def _init_once_async(self, init_only_resources=False):
        if self._initialized:
            return

        async with self._async_lock:
            if self._initialized:
                return

            if not self._resources_initialized:
                await (sync_to_async(self._init_resources)())
                self._resources_initialized = True

            if init_only_resources:
                return

            self._init_impl()
            self._initialized = True

    def _init_resources(self):
        pass

    def get_token(self):
        """Get a token silently from cache or authenticate if cached token is not found"""
        if self.is_async:
            raise KustoAsyncUsageError("get_token", self.is_async)
        self._init_once()

        token = self._get_token_from_cache_impl()
        if token is None:
            with self._lock:
                token = self._get_token_impl()

        return token

    def context(self) -> dict:
        if self.is_async:
            raise KustoAsyncUsageError("context", self.is_async)
        self._init_once(init_only_resources=True)
        return self._context_impl()

    async def context_async(self) -> dict:
        if not self.is_async:
            raise KustoAsyncUsageError("context_async", self.is_async)

        await self._init_once_async(init_only_resources=True)
        return self._context_impl()

    async def get_token_async(self):
        """Get a token asynchronously silently from cache or authenticate if cached token is not found"""

        if not self.is_async:
            raise KustoAsyncUsageError("get_token_async", self.is_async)

        await self._init_once_async()

        token = self._get_token_from_cache_impl()

        if token is None:
            async with self._async_lock:
                token = await self._get_token_impl_async()

        return token

    @staticmethod
    @abc.abstractmethod
    def name() -> str:
        """return the provider class name"""
        pass

    @abc.abstractmethod
    def _context_impl(self) -> dict:
        """return a secret-free context for error reporting"""
        pass

    @abc.abstractmethod
    def _init_impl(self):
        """Implement any "heavy" first time initializations here"""
        pass

    @abc.abstractmethod
    def _get_token_impl(self) -> Optional[dict]:
        """implement actual token acquisition here"""
        pass

    async def _get_token_impl_async(self) -> Optional[dict]:
        """implement actual token acquisition here"""
        return await sync_to_async(self._get_token_impl)()

    @abc.abstractmethod
    def _get_token_from_cache_impl(self) -> Optional[dict]:
        """Implement cache checks here, return None if cache check fails"""
        pass

    def set_proxy(self, proxy_url: str):
        self._proxy_dict = {"http": proxy_url, "https": proxy_url}


class AzureIdentityTokenProvider(TokenProviderBase, abc.ABC):
    _cloud_info: Optional[CloudInfo]
    _scopes = List[str]
    _kusto_uri: str
    _tenant_id: str
    _sync_type: Type
    _async_type: Type

    def __init__(self, sync_type: Type, async_type: Optional[Type], kusto_uri: str, tenant_id: str = None, is_async: bool = False):
        super().__init__(is_async)
        self._sync_type = sync_type
        self._async_type = async_type
        self._kusto_uri = kusto_uri
        self._sync_context: Optional[TokenCredential] = None
        self._async_context: Optional[TokenCredential] = None
        self._tenant_id = tenant_id

    def _init_impl(self):
        pass

    def _init_resources(self):
        self._cloud_info = CloudSettings.get_cloud_info_for_cluster(self._kusto_uri, self._proxy_dict)
        resource_uri = self._cloud_info.kusto_service_resource_id
        if self._cloud_info.login_mfa_required:
            resource_uri = resource_uri.replace(".kusto.", ".kustomfa.")

        self._scopes = [resource_uri + "/.default"]

    @abc.abstractmethod
    def _get_options(self, censored: bool) -> dict:
        pass

    def get_options_full(self, censored: bool) -> dict:
        return {"proxies": self._proxy_dict, **self._get_options(censored)}

    def _context_impl(self) -> dict:
        return {"authority": self.name(), "options": self.get_options_full(True)}

    def _get_token_impl(self) -> Optional[dict]:
        try:
            if self._sync_context is None:
                self._sync_context = self._sync_type(**self.get_options_full(False))

            token = self._sync_context.get_token(self._scopes[0], tenant_id=self._tenant_id)
            return {TokenConstants.TOKEN_TYPE: TokenConstants.BEARER_TYPE, TokenConstants.ACCESS_TOKEN: token.token}
        except Exception as e:
            raise KustoClientError(f"Failed to authenticate using {self.name()}, {self.context()} {self._additional_error_message()}", e)

    async def _get_token_impl_async(self) -> Optional[dict]:
        try:
            if self._async_context is None:
                if self._async_type is None:
                    self._async_context = self._sync_type(**self.get_options_full(False))
                else:
                    self._async_context = self._async_type(**self.get_options_full(False))

            if self._async_type is None:
                token = await sync_to_async(self._async_context.get_token)(self._scopes[0], tenant_id=self._tenant_id)
            else:
                token = await self._async_context.get_token(self._scopes[0], tenant_id=self._tenant_id)
            return {TokenConstants.TOKEN_TYPE: TokenConstants.BEARER_TYPE, TokenConstants.ACCESS_TOKEN: token.token}
        except ClientAuthenticationError as e:
            raise KustoClientError(f"Failed to authenticate using {self.name()}, {self.context()} {self._additional_error_message()}", e)

    def _get_token_from_cache_impl(self) -> Optional[dict]:
        return None

    def close(self):
        if self._sync_context is not None:
            self._sync_context.close()
        if self._async_context is not None:
            self._async_context.close()

    def _additional_error_message(self) -> str:
        pass


class BasicTokenProvider(TokenProviderBase):
    """Basic Token Provider keeps and returns a token received on construction"""

    def __init__(self, token: str, is_async: bool = False):
        super().__init__(is_async)
        self._token = token

    @staticmethod
    def name() -> str:
        return "BasicTokenProvider"

    def _context_impl(self) -> dict:
        return {"authority": self.name()}

    def _init_impl(self):
        pass

    def _get_token_impl(self) -> Optional[dict]:
        return None

    def _get_token_from_cache_impl(self) -> dict:
        return {TokenConstants.TOKEN_TYPE: TokenConstants.BEARER_TYPE, TokenConstants.ACCESS_TOKEN: self._token}


class CallbackTokenProvider(TokenProviderBase):
    """Callback Token Provider generates a token based on a callback function provided by the caller"""

    def __init__(
        self, token_callback: Optional[Callable[[], str]], async_token_callback: Optional[Callable[[], Coroutine[None, None, str]]], is_async: bool = False
    ):
        super().__init__(is_async)
        self._token_callback = token_callback
        self._async_token_callback = async_token_callback

    @staticmethod
    def name() -> str:
        return "CallbackTokenProvider"

    def _context_impl(self) -> dict:
        return {"authority": self.name()}

    def _init_impl(self):
        pass

    @staticmethod
    def _build_response(caller_token) -> dict:
        if not isinstance(caller_token, str):
            raise KustoClientError("Token provider returned something that is not a string [" + str(type(caller_token)) + "]")

        return {TokenConstants.TOKEN_TYPE: TokenConstants.BEARER_TYPE, TokenConstants.ACCESS_TOKEN: caller_token}

    def _get_token_impl(self) -> Optional[dict]:
        if self._token_callback is None:
            raise KustoClientError("token_callback is None, can't retrieve token")
        return self._build_response(self._token_callback())

    async def _get_token_impl_async(self) -> Optional[dict]:
        if self._async_token_callback is None:
            return await super()._get_token_impl_async()
        return self._build_response(await self._async_token_callback())

    def _get_token_from_cache_impl(self) -> Optional[dict]:
        return None


class MsiTokenProvider(AzureIdentityTokenProvider):
    """
    MSI Token Provider obtains a token from the MSI endpoint
    The args parameter is a dictionary conforming with the ManagedIdentityCredential initializer API arguments
    """

    def __init__(self, kusto_uri: str, msi_args: dict = None, is_async: bool = False):
        super().__init__(ManagedIdentityCredential, AsyncManagedIdentityCredential, kusto_uri, None, is_async)
        self._msi_args = msi_args

    @staticmethod
    def name() -> str:
        return "MsiTokenProvider"

    def _get_options(self, _) -> dict:
        return self._msi_args


class AzCliTokenProvider(AzureIdentityTokenProvider):
    """AzCli Token Provider obtains a refresh token from the AzCli cache and uses it to authenticate with Az identity"""

    def __init__(self, kusto_uri: str, is_async: bool = False):
        super().__init__(AzureCliCredential, AsyncAzureCliCredential, kusto_uri, None, is_async)

    @staticmethod
    def name() -> str:
        return "AzCliTokenProvider"

    def _get_options(self, _) -> dict:
        return {}

    def _additional_error_message(self) -> str:
        return "Please make sure you are logged in using 'az login', and have the latest version of the Azure CLI " "installed. "


class UserPassTokenProvider(AzureIdentityTokenProvider):
    """Acquire a token from Azure Identity using the username and password flow"""

    def __init__(self, kusto_uri: str, tenant_id: str, username: str, password: str, is_async: bool = False):
        super().__init__(UsernamePasswordCredential, None, kusto_uri, tenant_id, is_async)
        self._auth = tenant_id
        self._user = username
        self._pass = password

    @staticmethod
    def name() -> str:
        return "UserPassTokenProvider"

    def _get_options(self, censored: bool) -> dict:
        d = {"authority": self._cloud_info.login_endpoint, "client_id": self._cloud_info.kusto_client_app_id, "username": self._user, "password": self._pass}
        if censored:
            d["password"] = "********"
        return d


class DeviceLoginTokenProvider(AzureIdentityTokenProvider):
    """Acquire a token from Azure Identity using device login"""

    def __init__(self, kusto_uri: str, tenant_id: str, device_code_callback=None, is_async: bool = False):
        super().__init__(DeviceCodeCredential, None, kusto_uri, tenant_id, is_async)
        self._auth = tenant_id
        self._device_code_callback = lambda x, _1, _2: device_code_callback(x) if device_code_callback is not None else None

    @staticmethod
    def name() -> str:
        return "DeviceLoginTokenProvider"

    def _get_options(self, _) -> dict:
        return {"authority": self._cloud_info.login_endpoint, "client_id": self._cloud_info.kusto_client_app_id, "prompt_callback": self._device_code_callback}


class InteractiveLoginTokenProvider(AzureIdentityTokenProvider):
    """Acquire a token from azure-identity with Interactive Login flow"""

    def __init__(
        self,
        kusto_uri: str,
        tenant_id: str,
        login_hint: Optional[str] = None,
        domain_hint: Optional[str] = None,
        is_async: bool = False,
    ):
        super().__init__(InteractiveBrowserCredential, None, kusto_uri, tenant_id, is_async)
        self._login_hint = login_hint
        self._domain_hint = domain_hint

    @staticmethod
    def name() -> str:
        return "InteractiveLoginTokenProvider"

    def _get_options(self, _) -> dict:
        return {
            "authority": self._cloud_info.login_endpoint,
            "client_id": self._cloud_info.kusto_client_app_id,
            "login_hint": self._login_hint,
            "domain_hint": self._domain_hint,
        }


class ApplicationKeyTokenProvider(AzureIdentityTokenProvider):
    """Acquire a token from Azure Identity with a client secret"""

    def __init__(self, kusto_uri: str, tenant_id: str, client_id: str, client_secret: str, is_async: bool = False):
        super().__init__(ClientSecretCredential, AsyncClientSecretCredential, kusto_uri, tenant_id, is_async)
        self.options = {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
        }

    @staticmethod
    def name() -> str:
        return "ApplicationKeyTokenProvider"

    def _get_options(self, censored: bool) -> dict:
        d = self.options.copy()
        d["authority"] = self._cloud_info.login_endpoint
        if censored:
            d["client_secret"] = "********"
        return d


class ApplicationCertificateTokenProvider(AzureIdentityTokenProvider):
    """
    Acquire a token from Azure Identity with application Id and certificate.
    The certificate needs to be a PEM file with the private key.
    """

    def __init__(
        self,
        kusto_uri: str,
        client_id: str,
        tenant_id: str,
        certificate_path: str,
        is_async: bool = False,
    ):
        super().__init__(CertificateCredential, AsyncCertificateCredential, kusto_uri, tenant_id, is_async)
        self._client_id = client_id
        self._certificate_path = certificate_path

    @staticmethod
    def name() -> str:
        return "ApplicationCertificateTokenProvider"

    def _get_options(self, _) -> dict:
        return {
            "authority": self._cloud_info.login_endpoint,
            "client_id": self._client_id,
            "tenant_id": self._tenant_id,
            "certificate_path": self._certificate_path,
        }


class DefaultTokenProvider(AzureIdentityTokenProvider):
    """Acquire a token from Azure Identity with default credentials"""

    def __init__(self, kusto_uri: str, is_async: bool = False):
        super().__init__(DefaultAzureCredential, AsyncDefaultAzureCredential, kusto_uri, None, is_async)

    @staticmethod
    def name() -> str:
        return "DefaultTokenProvider"

    def _get_options(self, _) -> dict:
        return {"authority": self._cloud_info.login_endpoint}

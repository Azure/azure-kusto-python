# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import unittest
from threading import Thread
from typing import Optional

import pytest
from _cloud_settings import CloudInfo
from asgiref.sync import async_to_sync
from azure.identity import ClientSecretCredential, DefaultAzureCredential

from azure.kusto.data._token_providers import (
    UserPassTokenProvider,
    BasicTokenProvider,
    MsiTokenProvider,
    CallbackTokenProvider,
    InteractiveLoginTokenProvider,
    ApplicationKeyTokenProvider,
    AzCliTokenProvider,
    ApplicationCertificateTokenProvider,
    AzureIdentityTokenCredentialProvider,
    TokenConstants,
    KustoClientError,
    CloudSettings,
    DeviceLoginTokenProvider,
    TokenProviderBase,
)
from azure.kusto.data.exceptions import KustoNetworkError, KustoAsyncUsageError
from azure.kusto.data.env_utils import get_env, get_app_id, get_auth_id, prepare_app_key_auth

TOKEN_VALUE = "little miss sunshine"

TEST_AZ_AUTH = False  # enable this in environments with az cli installed, and make sure to call 'az login' first
TEST_MSI_AUTH = False  # enable this in environments with MSI enabled and make sure to set the relevant environment variables
TEST_DEVICE_AUTH = False  # User interaction required, enable this when running test manually
TEST_INTERACTIVE_AUTH = False  # User interaction required, enable this when running test manually


class MockProvider(TokenProviderBase):
    def __init__(self, is_async: bool = False):
        super().__init__(is_async)
        self._silent_token = False
        self.init_count = 0

    @staticmethod
    def name() -> str:
        return "MockProvider"

    def _context_impl(self) -> dict:
        return {"authority": "MockProvider"}

    def _init_impl(self):
        self.init_count = self.init_count + 1

    def _get_token_impl(self) -> Optional[dict]:
        self._silent_token = True
        return {TokenConstants.MSAL_ACCESS_TOKEN: "token"}

    def _get_token_from_cache_impl(self) -> Optional[dict]:
        if self._silent_token:
            return {TokenConstants.MSAL_ACCESS_TOKEN: "token"}

        return None


KUSTO_URI = get_env("ENGINE_CONNECTION_STRING", None)


class TokenProviderTests(unittest.TestCase):
    @staticmethod
    def test_base_provider():
        # test init with no URI
        with MockProvider():
            pass

        # Test provider with URI, No silent token
        with MockProvider() as provider:
            token = provider._get_token_from_cache_impl()
            assert provider.init_count == 0
            assert token is None

            token = provider.get_token()
            assert provider.init_count == 1
            assert TokenConstants.MSAL_ACCESS_TOKEN in token

            token = provider._get_token_from_cache_impl()
            assert TokenConstants.MSAL_ACCESS_TOKEN in token

            token = provider.get_token()
            assert provider.init_count == 1

            good_token = {TokenConstants.MSAL_ACCESS_TOKEN: TOKEN_VALUE}
            bad_token1 = None
            bad_token2 = {"error": "something bad occurred"}

            assert provider._valid_token_or_none(good_token) == good_token
            assert provider._valid_token_or_none(bad_token1) is None
            assert provider._valid_token_or_none(bad_token2) is None

            assert provider._valid_token_or_throw(good_token) == good_token

            exception_occurred = False
            try:
                provider._valid_token_or_throw(bad_token1)
            except KustoClientError:
                exception_occurred = True
            finally:
                assert exception_occurred

            exception_occurred = False
            try:
                provider._valid_token_or_throw(bad_token2)
            except KustoClientError:
                exception_occurred = True
            finally:
                assert exception_occurred

    @staticmethod
    def get_token_value(token: dict):
        assert token is not None
        assert TokenConstants.MSAL_ERROR not in token

        if TokenConstants.MSAL_ACCESS_TOKEN in token:
            return token[TokenConstants.MSAL_ACCESS_TOKEN]
        elif TokenConstants.AZ_ACCESS_TOKEN in token:
            return token[TokenConstants.AZ_ACCESS_TOKEN]
        else:
            assert False

    @staticmethod
    def test_fail_async_call():
        with BasicTokenProvider(token=TOKEN_VALUE) as provider:
            try:
                async_to_sync(provider.get_token_async)()
                assert False, "Expected KustoAsyncUsageError to occur"
            except KustoAsyncUsageError as e:
                assert (
                    str(e) == "Method get_token_async can't be called from a synchronous client"
                    or str(e) == "Method context_async can't be called from a synchronous client"
                )
                # context_async is called for tracing purposes

            try:
                async_to_sync(provider.context_async)()
                assert False, "Expected KustoAsyncUsageError to occur"
            except KustoAsyncUsageError as e:
                assert str(e) == "Method context_async can't be called from a synchronous client"

    @staticmethod
    def test_basic_provider():
        with BasicTokenProvider(token=TOKEN_VALUE) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) == TOKEN_VALUE

    @staticmethod
    def test_basic_provider_in_thread():
        exc = []

        def inner(exc):
            try:
                TokenProviderTests.test_basic_provider()
            except Exception as e:
                exc.append(e)

        pass
        t = Thread(target=inner, args=(exc,))
        t.start()
        t.join()
        if exc:
            raise exc[0]

    @staticmethod
    def test_callback_token_provider():
        with CallbackTokenProvider(token_callback=lambda: TOKEN_VALUE, async_token_callback=None) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) == TOKEN_VALUE

        with CallbackTokenProvider(token_callback=lambda: 0, async_token_callback=None) as provider:  # token is not a string
            exception_occurred = False
            try:
                provider.get_token()
            except KustoClientError:
                exception_occurred = True
            finally:
                assert exception_occurred

    @staticmethod
    def test_az_provider():
        if not TEST_AZ_AUTH:
            pytest.skip(" *** Skipped Az-Cli Provider Test ***")

        print("Note!\nThe test 'test_az_provider' will fail if 'az login' was not called.")
        with AzCliTokenProvider(KUSTO_URI) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

            # another run to pass through the cache
            token = provider._get_token_from_cache_impl()
            assert TokenProviderTests.get_token_value(token) is not None

    @staticmethod
    def test_msi_provider():
        if not TEST_MSI_AUTH:
            pytest.skip(" *** Skipped MSI Provider Test ***")

        user_msi_object_id = get_env("MSI_OBJECT_ID", optional=True)
        user_msi_client_id = get_env("MSI_CLIENT_ID", optional=True)

        # system MSI
        with MsiTokenProvider(KUSTO_URI) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

        if user_msi_object_id is not None:
            args = {"object_id": user_msi_object_id}
            with MsiTokenProvider(KUSTO_URI, args) as provider:
                token = provider.get_token()
                assert TokenProviderTests.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped MSI Provider Client Id Test ***")

        if user_msi_client_id is not None:
            args = {"client_id": user_msi_client_id}
            with MsiTokenProvider(KUSTO_URI, args) as provider:
                token = provider.get_token()
                assert TokenProviderTests.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped MSI Provider Object Id Test ***")

    @staticmethod
    def test_user_pass_provider():
        username = get_env("USER_NAME", optional=True)
        password = get_env("USER_PASS", optional=True)
        auth = get_env("USER_AUTH_ID", default="organizations")

        if username and password and auth:
            with UserPassTokenProvider(KUSTO_URI, auth, username, password) as provider:
                token = provider.get_token()
                assert TokenProviderTests.get_token_value(token) is not None

                # Again through cache
                token = provider._get_token_from_cache_impl()
                assert TokenProviderTests.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped User & Pass Provider Test ***")

    @staticmethod
    def test_device_auth_provider():
        if not TEST_DEVICE_AUTH:
            pytest.skip(" *** Skipped User Device Flow Test ***")

        def callback(x, x2, x3):
            # break here if you debug this test, and get the code from 'x'
            print(f"Please go to {x} and enter code {x2} to authenticate, expires in {x3}")

        with DeviceLoginTokenProvider(KUSTO_URI, "organizations", callback) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert TokenProviderTests.get_token_value(token) is not None

    @staticmethod
    def test_interactive_login():
        if not TEST_INTERACTIVE_AUTH:
            pytest.skip(" *** Skipped interactive login Test ***")

        auth_id = get_auth_id()
        with InteractiveLoginTokenProvider(KUSTO_URI, auth_id) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert TokenProviderTests.get_token_value(token) is not None

    @staticmethod
    def test_app_key_provider():
        app_auth = prepare_app_key_auth(optional=True)

        if app_auth:
            with ApplicationKeyTokenProvider(KUSTO_URI, app_auth.auth_id, app_auth.app_id, app_auth.app_key) as provider:
                token = provider.get_token()
                assert TokenProviderTests.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped App Id & Key Provider Test ***")

    @staticmethod
    def test_app_key_provider_when_url_not_valid():
        app_auth = prepare_app_key_auth(optional=True)

        if app_auth:
            with pytest.raises(KustoNetworkError):
                with ApplicationKeyTokenProvider("NoURI", app_auth.auth_id, app_auth.app_id, app_auth.app_key) as provider:
                    provider.get_token()

    @staticmethod
    def test_app_cert_provider():
        cert_app_id = get_app_id(optional=True)
        cert_auth = get_auth_id(optional=True)
        thumbprint = get_env("CERT_THUMBPRINT", optional=True)
        public_cert_path = get_env("CERT_PUBLIC_CERT_PATH", optional=True)
        pem_key_path = get_env("CERT_PEM_KEY_PATH", optional=True)

        if pem_key_path and thumbprint and cert_app_id and cert_auth:
            with open(pem_key_path, "r") as file:
                pem_key = file.read()

            with ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, cert_auth, pem_key, thumbprint) as provider:
                token = provider.get_token()
                assert TokenProviderTests.get_token_value(token) is not None

                if public_cert_path:
                    with open(public_cert_path, "r") as file:
                        public_cert = file.read()

                    with ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, cert_auth, pem_key, thumbprint, public_cert) as provider:
                        token = provider.get_token()
                        assert TokenProviderTests.get_token_value(token) is not None
                else:
                    pytest.skip(" *** Skipped App Cert SNI Provider Test ***")

        else:
            pytest.skip(" *** Skipped App Cert Provider Test ***")

    @staticmethod
    def test_cloud_mfa_off():
        FAKE_URI = "https://fake_cluster_for_login_mfa_test.kusto.windows.net"
        cloud = CloudInfo(
            login_endpoint="https://login_endpoint",
            login_mfa_required=False,
            kusto_client_app_id="1234",
            kusto_client_redirect_uri="",
            kusto_service_resource_id="https://fakeurl.kusto.windows.net",
            first_party_authority_url="",
        )
        CloudSettings.add_to_cache(FAKE_URI, cloud)
        authority = "auth_test"

        with UserPassTokenProvider(FAKE_URI, authority, "a", "b") as provider:
            provider._init_once(init_only_resources=True)
            context = provider.context()
            assert context["authority"] == "https://login_endpoint/auth_test"
            assert context["client_id"] == cloud.kusto_client_app_id
            assert provider._scopes == ["https://fakeurl.kusto.windows.net/.default"]

    @staticmethod
    def test_cloud_mfa_on():
        FAKE_URI = "https://fake_cluster_for_login_mfa_test.kusto.windows.net"
        cloud = CloudInfo(
            login_endpoint="https://login_endpoint",
            login_mfa_required=True,
            kusto_client_app_id="1234",
            kusto_client_redirect_uri="",
            kusto_service_resource_id="https://fakeurl.kusto.windows.net",
            first_party_authority_url="",
        )
        CloudSettings.add_to_cache(FAKE_URI, cloud)
        authority = "auth_test"

        with UserPassTokenProvider(FAKE_URI, authority, "a", "b") as provider:
            provider._init_once(init_only_resources=True)
            context = provider.context()
            assert context["authority"] == "https://login_endpoint/auth_test"
            assert context["client_id"] == "1234"
            assert provider._scopes == ["https://fakeurl.kustomfa.windows.net/.default"]

    @staticmethod
    def test_azure_identity_default_token_provider():
        auth = prepare_app_key_auth(optional=True)
        if not auth:
            pytest.skip(" *** Skipped Azure Identity Provider Test ***")

        app_id, app_key, auth_id = auth

        with AzureIdentityTokenCredentialProvider(KUSTO_URI, credential=DefaultAzureCredential()) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

        with AzureIdentityTokenCredentialProvider(
            KUSTO_URI,
            credential_from_login_endpoint=lambda login_endpoint: ClientSecretCredential(
                authority=login_endpoint, client_id=app_id, client_secret=app_key, tenant_id=auth_id
            ),
        ) as provider:
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

    @staticmethod
    def test_sanity_close():
        DUMMY_URI = "https://dummy_uri.kusto.windows.net"
        providers = [
            MsiTokenProvider(DUMMY_URI),
            AzCliTokenProvider(DUMMY_URI),
            UserPassTokenProvider(DUMMY_URI, "organizations", "a", "b"),
            ApplicationKeyTokenProvider(DUMMY_URI, "a", "b", "c"),
            ApplicationCertificateTokenProvider(DUMMY_URI, "a", "b", "c", "d"),
            CallbackTokenProvider(lambda: "a", None),
            AzureIdentityTokenCredentialProvider(DUMMY_URI, credential=DefaultAzureCredential()),
            DeviceLoginTokenProvider(DUMMY_URI, "organizations", lambda x, x2, x3: None),
        ]

        for provider in providers:
            with provider:
                pass

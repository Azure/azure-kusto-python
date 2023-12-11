# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import pytest
from azure.identity.aio import ClientSecretCredential as AsyncClientSecretCredential

from azure.kusto.data._decorators import aio_documented_by
from azure.kusto.data._token_providers import *
from azure.kusto.data.env_utils import get_env, get_app_id, get_auth_id, prepare_app_key_auth
from .test_kusto_client import run_aio_tests
from ..test_token_providers import KUSTO_URI, TOKEN_VALUE, TEST_AZ_AUTH, TEST_MSI_AUTH, TEST_DEVICE_AUTH, TokenProviderTests, MockProvider


@pytest.mark.skipif(not run_aio_tests, reason="requires aio")
@aio_documented_by(TokenProviderTests)
class TestTokenProvider:
    @aio_documented_by(TokenProviderTests.test_base_provider)
    @pytest.mark.asyncio
    async def test_base_provider(self):
        # test init with no URI
        with MockProvider(is_async=True):
            pass

        # Test provider with URI, No silent token
        with MockProvider(is_async=True) as provider:
            token = provider._get_token_from_cache_impl()
            assert provider.init_count == 0
            assert token is None

            token = await provider.get_token_async()
            assert provider.init_count == 1
            assert TokenConstants.MSAL_ACCESS_TOKEN in token

            token = provider._get_token_from_cache_impl()
            assert TokenConstants.MSAL_ACCESS_TOKEN in token

            token = await provider.get_token_async()
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

    @aio_documented_by(TokenProviderTests.get_token_value)
    def get_token_value(self, token: dict):
        assert token is not None
        assert TokenConstants.MSAL_ERROR not in token

        value = None
        if TokenConstants.MSAL_ACCESS_TOKEN in token:
            return token[TokenConstants.MSAL_ACCESS_TOKEN]
        elif TokenConstants.AZ_ACCESS_TOKEN in token:
            return token[TokenConstants.AZ_ACCESS_TOKEN]
        else:
            assert False

    @staticmethod
    def test_fail_sync_call():
        with BasicTokenProvider(token=TOKEN_VALUE, is_async=True) as provider:
            try:
                provider.get_token()
                assert False, "Expected KustoAsyncUsageError to occur"
            except KustoAsyncUsageError as e:
                assert (
                    str(e) == "Method get_token can't be called from an asynchronous client"
                    or str(e) == "Method context can't be called from an asynchronous client"
                )
                # context is called for tracing purposes

            try:
                provider.context()
                assert False, "Expected KustoAsyncUsageError to occur"
            except KustoAsyncUsageError as e:
                assert str(e) == "Method context can't be called from an asynchronous client"

    @aio_documented_by(TokenProviderTests.test_basic_provider)
    @pytest.mark.asyncio
    async def test_basic_provider(self):
        with BasicTokenProvider(token=TOKEN_VALUE, is_async=True) as provider:
            token = await provider.get_token_async()
            assert self.get_token_value(token) == TOKEN_VALUE

    @aio_documented_by(TokenProviderTests.test_callback_token_provider)
    @pytest.mark.asyncio
    async def test_callback_token_provider(self):
        with CallbackTokenProvider(token_callback=lambda: TOKEN_VALUE, async_token_callback=None, is_async=True) as provider:
            token = await provider.get_token_async()
            assert self.get_token_value(token) == TOKEN_VALUE

        with CallbackTokenProvider(token_callback=lambda: 0, async_token_callback=None, is_async=True) as provider:  # token is not a string
            exception_occurred = False
            try:
                await provider.get_token_async()
            except KustoClientError:
                exception_occurred = True
            finally:
                assert exception_occurred

    @pytest.mark.asyncio
    async def test_callback_token_provider_with_async_method(self):
        async def callback():
            return TOKEN_VALUE

        with CallbackTokenProvider(token_callback=None, async_token_callback=callback, is_async=True) as provider:
            token = await provider.get_token_async()
            assert self.get_token_value(token) == TOKEN_VALUE

        async def fail_callback():
            return 0

        with CallbackTokenProvider(token_callback=None, async_token_callback=fail_callback, is_async=True) as provider:  # token is not a string
            exception_occurred = False
            try:
                await provider.get_token_async()
            except KustoClientError:
                exception_occurred = True
            finally:
                assert exception_occurred

    @aio_documented_by(TokenProviderTests.test_az_provider)
    @pytest.mark.asyncio
    async def test_az_provider(self):
        if not TEST_AZ_AUTH:
            pytest.skip(" *** Skipped Az-Cli Provider Test ***")

        print("Note!\nThe test 'test_az_provider' will fail if 'az login' was not called.")
        async with AzCliTokenProvider(KUSTO_URI, is_async=True) as provider:
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None

            # another run to pass through the cache
            token = provider._get_token_from_cache_impl()
            assert self.get_token_value(token) is not None

    @aio_documented_by(TokenProviderTests.test_msi_provider)
    @pytest.mark.asyncio
    async def test_msi_provider(self):
        if not TEST_MSI_AUTH:
            pytest.skip(" *** Skipped MSI Provider Test ***")

        user_msi_object_id = get_env("MSI_OBJECT_ID", optional=True)
        user_msi_client_id = get_env("MSI_CLIENT_ID", optional=True)

        # system MSI
        async with MsiTokenProvider(KUSTO_URI, is_async=True) as provider:
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None

        if user_msi_object_id is not None:
            args = {"object_id": user_msi_object_id}
            async with MsiTokenProvider(KUSTO_URI, args, is_async=True) as provider:
                token = await provider.get_token_async()
                assert self.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped MSI Provider Client Id Test ***")

        if user_msi_client_id is not None:
            args = {"client_id": user_msi_client_id}
            async with MsiTokenProvider(KUSTO_URI, args, is_async=True) as provider:
                token = await provider.get_token_async()
                assert self.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped MSI Provider Object Id Test ***")

    @aio_documented_by(TokenProviderTests.test_user_pass_provider)
    @pytest.mark.asyncio
    async def test_user_pass_provider(self):
        username = get_env("USER_NAME", optional=True)
        password = get_env("USER_PASS", optional=True)
        auth = get_env("USER_AUTH_ID", default="organizations")

        if username and password and auth:
            with UserPassTokenProvider(KUSTO_URI, auth, username, password, is_async=True) as provider:
                token = await provider.get_token_async()
                assert self.get_token_value(token) is not None

                # Again through cache
                token = provider._get_token_from_cache_impl()
                assert self.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped User & Pass Provider Test ***")

    @aio_documented_by(TokenProviderTests.test_device_auth_provider)
    @pytest.mark.asyncio
    async def test_device_auth_provider(self):
        if not TEST_DEVICE_AUTH:
            pytest.skip(" *** Skipped User Device Flow Test ***")

        def callback(x, x2, x3):
            # break here if you debug this test, and get the code from 'x'
            print(f"Please go to {x} and enter code {x2} to authenticate, expires in {x3}")

        with DeviceLoginTokenProvider(KUSTO_URI, "organizations", callback, is_async=True) as provider:
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert self.get_token_value(token) is not None

    @aio_documented_by(TokenProviderTests.test_app_key_provider)
    @pytest.mark.asyncio
    async def test_app_key_provider(self):
        app_auth = prepare_app_key_auth(optional=True)

        if app_auth:
            with ApplicationKeyTokenProvider(KUSTO_URI, app_auth.auth_id, app_auth.app_id, app_auth.app_key, is_async=True) as provider:
                token = await provider.get_token_async()
                assert self.get_token_value(token) is not None
        else:
            pytest.skip(" *** Skipped App Id & Key Provider Test ***")

    @aio_documented_by(TokenProviderTests.test_app_cert_provider)
    @pytest.mark.asyncio
    async def test_app_cert_provider(self):
        # default details are for kusto-client-e2e-test-app
        # to invoke the test download the certs from Azure Portal
        cert_app_id = get_app_id(optional=True)
        cert_auth = get_auth_id(optional=True)
        thumbprint = get_env("CERT_THUMBPRINT", optional=True)
        public_cert_path = get_env("CERT_PUBLIC_CERT_PATH", optional=True)
        pem_key_path = get_env("CERT_PEM_KEY_PATH", optional=True)

        if pem_key_path and thumbprint and cert_app_id and cert_auth:
            with open(pem_key_path, "rb") as file:
                pem_key = file.read()

            with ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, cert_auth, pem_key, thumbprint, is_async=True) as provider:
                token = await provider.get_token_async()
                assert self.get_token_value(token) is not None

                if public_cert_path:
                    with open(public_cert_path, "r") as file:
                        public_cert = file.read()

                    with ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, cert_auth, pem_key, thumbprint, public_cert, is_async=True) as provider:
                        token = await provider.get_token_async()
                        assert self.get_token_value(token) is not None

                        # Again through cache
                        token = provider._get_token_from_cache_impl()
                        assert self.get_token_value(token) is not None
                else:
                    pytest.skip(" *** Skipped App Cert SNI Provider Test ***")

        else:
            pytest.skip(" *** Skipped App Cert Provider Test ***")

    @aio_documented_by(TokenProviderTests.test_cloud_mfa_off)
    @pytest.mark.asyncio
    async def test_cloud_mfa_off(self):
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

        with UserPassTokenProvider(FAKE_URI, authority, "a", "b", is_async=True) as provider:
            await provider._init_once_async(init_only_resources=True)
            context = await provider.context_async()
            assert context["authority"] == "https://login_endpoint/auth_test"
            assert context["client_id"] == "1234"
            assert provider._scopes == ["https://fakeurl.kusto.windows.net/.default"]

    @aio_documented_by(TokenProviderTests.test_cloud_mfa_off)
    @pytest.mark.asyncio
    async def test_cloud_mfa_on(self):
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

        with UserPassTokenProvider(FAKE_URI, authority, "a", "b", is_async=True) as provider:
            await provider._init_once_async(init_only_resources=True)
            context = await provider.context_async()
            assert context["authority"] == "https://login_endpoint/auth_test"
            assert context["client_id"] == "1234"
            assert provider._scopes == ["https://fakeurl.kustomfa.windows.net/.default"]

    def test_async_lock(self):
        """
        This test makes sure that the lock inside of a TokenProvider, is created within the correct event loop.
        Before this, the Lock was created once per class.
        This meant that if someone created a new event loop, and created a provider in it, awaiting on the lock would cause an exception because it belongs to
        a different loop.
        Now the lock is instantiated for every class instance, avoiding this issue.
        """

        async def start():
            async def inner():
                await asyncio.sleep(0.1)
                return ""

            with CallbackTokenProvider(token_callback=None, async_token_callback=inner, is_async=True) as provider:
                await asyncio.gather(provider.get_token_async(), provider.get_token_async(), provider.get_token_async())

        loop = asyncio.events.new_event_loop()
        asyncio.events.set_event_loop(loop)
        loop.run_until_complete(start())

    @aio_documented_by(TokenProviderTests.test_azure_identity_default_token_provider)
    @pytest.mark.asyncio
    async def test_azure_identity_token_provider(self):
        app_id, app_key, auth_id = prepare_app_key_auth(optional=True)
        if not app_id or not app_key or not auth_id:
            pytest.skip(" *** Skipped Azure Identity Provider Test ***")

        async with AzureIdentityTokenCredentialProvider(KUSTO_URI, is_async=True, credential=AsyncDefaultAzureCredential()) as provider:
            token = await provider.get_token_async()
            assert TokenProviderTests.get_token_value(token) is not None

        async with AzureIdentityTokenCredentialProvider(
            KUSTO_URI,
            is_async=True,
            credential_from_login_endpoint=lambda login_endpoint: AsyncClientSecretCredential(
                authority=login_endpoint, client_id=app_id, client_secret=app_key, tenant_id=auth_id
            ),
        ) as provider:
            token = await provider.get_token_async()
            assert TokenProviderTests.get_token_value(token) is not None

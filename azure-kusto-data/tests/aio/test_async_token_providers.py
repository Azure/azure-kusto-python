# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import pytest

from azure.kusto.data._decorators import aio_documented_by
from azure.kusto.data._token_providers import *
from .test_kusto_client import run_aio_tests
from ..test_token_providers import KUSTO_URI, PUBLIC_AUTH_URI, TOKEN_VALUE, TEST_AZ_AUTH, TEST_MSI_AUTH, TEST_DEVICE_AUTH, TokenProviderTests


class MockProvider(TokenProviderBase):
    def __init__(self, uri: str):
        super().__init__(uri)
        self._silent_token = False
        self.init_count = 0

    @staticmethod
    def name() -> str:
        return "MockProvider"

    def context(self) -> dict:
        return {"authority": "MockProvider"}

    def _init_impl(self):
        self.init_count = self.init_count + 1

    def _get_token_impl(self) -> dict:
        self._silent_token = True
        return {TokenConstants.MSAL_ACCESS_TOKEN: "token"}

    def _get_token_from_cache_impl(self) -> dict:
        if self._silent_token:
            return {TokenConstants.MSAL_ACCESS_TOKEN: "token"}

        return None


@pytest.mark.skipif(not run_aio_tests, reason="requires aio")
@aio_documented_by(TokenProviderTests)
class TestTokenProvider:
    @aio_documented_by(TokenProviderTests.test_base_provider)
    @pytest.mark.asyncio
    async def test_base_provider(self):
        # test init with no URI
        provider = MockProvider(None)

        # Test provider with URI, No silent token
        provider = MockProvider(KUSTO_URI)

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

    @aio_documented_by(TokenProviderTests.test_basic_provider)
    @pytest.mark.asyncio
    async def test_basic_provider(self):
        provider = BasicTokenProvider(token=TOKEN_VALUE)
        token = await provider.get_token_async()
        assert self.get_token_value(token) == TOKEN_VALUE

    @aio_documented_by(TokenProviderTests.test_callback_token_provider)
    @pytest.mark.asyncio
    async def test_callback_token_provider(self):
        provider = CallbackTokenProvider(lambda: TOKEN_VALUE)
        token = await provider.get_token_async()
        assert self.get_token_value(token) == TOKEN_VALUE

        provider = CallbackTokenProvider(lambda: 0)  # token is not a string
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
            print(" *** Skipped Az-Cli Provider Test ***")
            return

        print("Note!\nThe test 'test_az_provider' will fail if 'az login' was not called.")
        provider = AzCliTokenProvider(KUSTO_URI)
        token = await provider.get_token_async()
        assert self.get_token_value(token) is not None

        # another run to pass through the cache
        token = provider._get_token_from_cache_impl()
        assert self.get_token_value(token) is not None

    @aio_documented_by(TokenProviderTests.test_msi_provider)
    @pytest.mark.asyncio
    async def test_msi_provider(self):
        if not TEST_MSI_AUTH:
            print(" *** Skipped MSI Provider Test ***")
            return

        user_msi_object_id = os.environ.get("MSI_OBJECT_ID")
        user_msi_client_id = os.environ.get("MSI_CLIENT_ID")

        # system MSI
        provider = MsiTokenProvider(KUSTO_URI)
        token = await provider.get_token_async()
        assert self.get_token_value(token) is not None

        if user_msi_object_id is not None:
            args = {"object_id": user_msi_object_id}
            provider = MsiTokenProvider(KUSTO_URI, args)
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None
        else:
            print(" *** Skipped MSI Provider Client Id Test ***")

        if user_msi_client_id is not None:
            args = {"client_id": user_msi_client_id}
            provider = MsiTokenProvider(KUSTO_URI, args)
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None
        else:
            print(" *** Skipped MSI Provider Object Id Test ***")

    @aio_documented_by(TokenProviderTests.test_user_pass_provider)
    @pytest.mark.asyncio
    async def test_user_pass_provider(self):
        username = os.environ.get("USER_NAME")
        password = os.environ.get("USER_PASS")
        auth = os.environ.get("USER_AUTH_ID", "organizations")

        if username and password and auth:
            provider = UserPassTokenProvider(KUSTO_URI, PUBLIC_AUTH_URI + auth, username, password)
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert self.get_token_value(token) is not None
        else:
            print(" *** Skipped User & Pass Provider Test ***")

    @aio_documented_by(TokenProviderTests.test_device_auth_provider)
    @pytest.mark.asyncio
    async def test_device_auth_provider(self):
        if not TEST_DEVICE_AUTH:
            print(" *** Skipped User Device Flow Test ***")
            return

        def callback(x):
            # break here if you debug this test, and get the code from 'x'
            print(x)

        provider = DeviceLoginTokenProvider(KUSTO_URI, PUBLIC_AUTH_URI + "organizations", callback)
        token = await provider.get_token_async()
        assert self.get_token_value(token) is not None

        # Again through cache
        token = provider._get_token_from_cache_impl()
        assert self.get_token_value(token) is not None

    @aio_documented_by(TokenProviderTests.test_app_key_provider)
    @pytest.mark.asyncio
    async def test_app_key_provider(self):
        # default details are for kusto-client-e2e-test-app
        # to run the test, get the key from Azure portal
        app_id = os.environ.get("APP_ID", "b699d721-4f6f-4320-bc9a-88d578dfe68f")
        auth_id = os.environ.get("APP_AUTH_ID", "72f988bf-86f1-41af-91ab-2d7cd011db47")
        app_key = os.environ.get("APP_KEY")

        if app_id and app_key and auth_id:
            provider = ApplicationKeyTokenProvider(KUSTO_URI, PUBLIC_AUTH_URI + auth_id, app_id, app_key)
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert self.get_token_value(token) is not None
        else:
            print(" *** Skipped App Id & Key Provider Test ***")

    @aio_documented_by(TokenProviderTests.test_app_cert_provider)
    @pytest.mark.asyncio
    async def test_app_cert_provider(self):
        # default details are for kusto-client-e2e-test-app
        # to run the test download the certs from Azure Portal
        cert_app_id = os.environ.get("CERT_APP_ID", "b699d721-4f6f-4320-bc9a-88d578dfe68f")
        cert_auth = os.environ.get("CERT_AUTH", "72f988bf-86f1-41af-91ab-2d7cd011db47")
        thumbprint = os.environ.get("CERT_THUMBPRINT")
        public_cert_path = os.environ.get("PUBLIC_CERT_PATH")
        pem_key_path = os.environ.get("CERT_PEM_KEY_PATH")

        if pem_key_path and thumbprint and cert_app_id:
            with open(pem_key_path, "rb") as file:
                pem_key = file.read()

            provider = ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, PUBLIC_AUTH_URI + cert_auth, pem_key, thumbprint)
            token = await provider.get_token_async()
            assert self.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert self.get_token_value(token) is not None

            if public_cert_path:
                with open(public_cert_path, "r") as file:
                    public_cert = file.read()

                provider = ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, PUBLIC_AUTH_URI + cert_auth, pem_key, thumbprint, public_cert)
                token = await provider.get_token_async()
                assert self.get_token_value(token) is not None

                # Again through cache
                token = provider._get_token_from_cache_impl()
                assert self.get_token_value(token) is not None
            else:
                print(" *** Skipped App Cert SNI Provider Test ***")

        else:
            print(" *** Skipped App Cert Provider Test ***")

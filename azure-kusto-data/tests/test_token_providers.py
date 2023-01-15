# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import unittest
from threading import Thread

from asgiref.sync import async_to_sync

from azure.identity import ClientSecretCredential
from azure.kusto.data._token_providers import *

KUSTO_URI = "https://sdkse2etest.eastus.kusto.windows.net"
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


class TokenProviderTests(unittest.TestCase):
    @staticmethod
    def test_base_provider():
        # test init with no URI
        provider = MockProvider()

        # Test provider with URI, No silent token
        provider = MockProvider()

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

        value = None
        if TokenConstants.MSAL_ACCESS_TOKEN in token:
            return token[TokenConstants.MSAL_ACCESS_TOKEN]
        elif TokenConstants.AZ_ACCESS_TOKEN in token:
            return token[TokenConstants.AZ_ACCESS_TOKEN]
        else:
            assert False

    @staticmethod
    def test_fail_async_call():
        provider = BasicTokenProvider(token=TOKEN_VALUE)
        try:
            async_to_sync(provider.get_token_async)()
            assert False, "Expected KustoAsyncUsageError to occur"
        except KustoAsyncUsageError as e:
            assert str(e) == "Method get_token_async can't be called from a synchronous client"
        try:
            async_to_sync(provider.context_async)()
            assert False, "Expected KustoAsyncUsageError to occur"
        except KustoAsyncUsageError as e:
            assert str(e) == "Method context_async can't be called from a synchronous client"

    @staticmethod
    def test_basic_provider():
        provider = BasicTokenProvider(token=TOKEN_VALUE)
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
        provider = CallbackTokenProvider(token_callback=lambda: TOKEN_VALUE, async_token_callback=None)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) == TOKEN_VALUE

        provider = CallbackTokenProvider(token_callback=lambda: 0, async_token_callback=None)  # token is not a string
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
            print(" *** Skipped Az-Cli Provider Test ***")
            return

        print("Note!\nThe test 'test_az_provider' will fail if 'az login' was not called.")
        provider = AzCliTokenProvider(KUSTO_URI)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) is not None

        # another run to pass through the cache
        token = provider._get_token_from_cache_impl()
        assert TokenProviderTests.get_token_value(token) is not None

    @staticmethod
    def test_msi_provider():
        if not TEST_MSI_AUTH:
            print(" *** Skipped MSI Provider Test ***")
            return

        user_msi_object_id = os.environ.get("MSI_OBJECT_ID")
        user_msi_client_id = os.environ.get("MSI_CLIENT_ID")

        # system MSI
        provider = MsiTokenProvider(KUSTO_URI)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) is not None

        if user_msi_object_id is not None:
            args = {"object_id": user_msi_object_id}
            provider = MsiTokenProvider(KUSTO_URI, args)
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None
        else:
            print(" *** Skipped MSI Provider Client Id Test ***")

        if user_msi_client_id is not None:
            args = {"client_id": user_msi_client_id}
            provider = MsiTokenProvider(KUSTO_URI, args)
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None
        else:
            print(" *** Skipped MSI Provider Object Id Test ***")

    @staticmethod
    def test_user_pass_provider():
        username = os.environ.get("USER_NAME")
        password = os.environ.get("USER_PASS")
        auth = os.environ.get("USER_AUTH_ID", "organizations")

        if username and password and auth:
            provider = UserPassTokenProvider(KUSTO_URI, auth, username, password)
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert TokenProviderTests.get_token_value(token) is not None
        else:
            print(" *** Skipped User & Pass Provider Test ***")

    @staticmethod
    def test_device_auth_provider():
        if not TEST_DEVICE_AUTH:
            print(" *** Skipped User Device Flow Test ***")
            return

        def callback(x):
            # break here if you debug this test, and get the code from 'x'
            print(x)

        provider = DeviceLoginTokenProvider(KUSTO_URI, "organizations", callback)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) is not None

        # Again through cache
        token = provider._get_token_from_cache_impl()
        assert TokenProviderTests.get_token_value(token) is not None

    @staticmethod
    def test_interactive_login():
        if not TEST_INTERACTIVE_AUTH:
            print(" *** Skipped interactive login Test ***")
            return

        auth_id = os.environ.get("APP_AUTH_ID", "72f988bf-86f1-41af-91ab-2d7cd011db47")
        provider = InteractiveLoginTokenProvider(KUSTO_URI, auth_id)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) is not None

        # Again through cache
        token = provider._get_token_from_cache_impl()
        assert TokenProviderTests.get_token_value(token) is not None

    @staticmethod
    def test_app_key_provider():
        # default details are for kusto-client-e2e-test-app
        # to run the test, get the key from Azure portal
        app_id = os.environ.get("APP_ID", "b699d721-4f6f-4320-bc9a-88d578dfe68f")
        auth_id = os.environ.get("APP_AUTH_ID", "72f988bf-86f1-41af-91ab-2d7cd011db47")
        app_key = os.environ.get("APP_KEY")

        if app_id and app_key and auth_id:
            provider = ApplicationKeyTokenProvider(KUSTO_URI, auth_id, app_id, app_key)
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert TokenProviderTests.get_token_value(token) is not None
        else:
            print(" *** Skipped App Id & Key Provider Test ***")

    @staticmethod
    def test_app_cert_provider():
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

            provider = ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, cert_auth, pem_key, thumbprint)
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

            # Again through cache
            token = provider._get_token_from_cache_impl()
            assert TokenProviderTests.get_token_value(token) is not None

            if public_cert_path:
                with open(public_cert_path, "r") as file:
                    public_cert = file.read()

                provider = ApplicationCertificateTokenProvider(KUSTO_URI, cert_app_id, cert_auth, pem_key, thumbprint, public_cert)
                token = provider.get_token()
                assert TokenProviderTests.get_token_value(token) is not None

                # Again through cache
                token = provider._get_token_from_cache_impl()
                assert TokenProviderTests.get_token_value(token) is not None
            else:
                print(" *** Skipped App Cert SNI Provider Test ***")

        else:
            print(" *** Skipped App Cert Provider Test ***")

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
        CloudSettings._cloud_cache[FAKE_URI] = cloud
        authority = "auth_test"

        provider = UserPassTokenProvider(FAKE_URI, authority, "a", "b")
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
        CloudSettings._cloud_cache[FAKE_URI] = cloud
        authority = "auth_test"

        provider = UserPassTokenProvider(FAKE_URI, authority, "a", "b")
        provider._init_once(init_only_resources=True)
        context = provider.context()
        assert context["authority"] == "https://login_endpoint/auth_test"
        assert context["client_id"] == "1234"
        assert provider._scopes == ["https://fakeurl.kustomfa.windows.net/.default"]

    @staticmethod
    def test_azure_identity_default_token_provider():
        app_id = os.environ.get("APP_ID", "b699d721-4f6f-4320-bc9a-88d578dfe68f")
        os.environ["AZURE_CLIENT_ID"] = app_id
        auth_id = os.environ.get("APP_AUTH_ID", "72f988bf-86f1-41af-91ab-2d7cd011db47")
        os.environ["AZURE_TENANT_ID"] = auth_id
        app_key = os.environ.get("APP_KEY")
        os.environ["AZURE_CLIENT_SECRET"] = app_key

        provider = AzureIdentityTokenCredentialProvider(KUSTO_URI)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) is not None

        provider = AzureIdentityTokenCredentialProvider(
            KUSTO_URI, cred_builder=ClientSecretCredential, additional_params={"tenant_id": auth_id, "client_id": app_id, "client_secret": app_key}
        )
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) is not None

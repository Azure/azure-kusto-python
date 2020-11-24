# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import unittest
import os
from abc import ABC

from azure.kusto.data._token_providers import *


KUSTO_URI = "https://thisclusterdoesnotexist.kusto.windows.net"
TOKEN_VALUE = "little miss sunshine"
TEST_AZ_AUTH = False  # enable this in environments with az cli installed, and make sure to call 'az login' first
TEST_MSI_AUTH = False  # enable this in environments with MSI enabled and make sure to set the relevant environment variables


class MockProvider(TokenProviderBase, ABC):
    def __init__(self, uri: str):
        super().__init__(uri)
        self._silent_token = False
        self.init_count = 0

    @staticmethod
    def name() -> str:
        return "MockProvider"

    def context(self) -> dict:
        return {'authority": '"MockProvider"}

    def _init_impl(self):
        self.init_count = self.init_count + 1

    def _get_token_impl(self) -> dict:
        self._silent_token = True
        return {TokenConstants.MSAL_ACCESS_TOKEN: "token"}

    def _get_token_from_cache_impl(self) -> dict:
        if self._silent_token:
            return {TokenConstants.MSAL_ACCESS_TOKEN: "token"}

        return None


class TokenProviderTests(unittest.TestCase):
    @staticmethod
    def test_base_provider():
        # test init with no URI
        provider = MockProvider(None)

        # Test provider with URI, No silent token
        provider = MockProvider(KUSTO_URI)

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
    def test_basic_provider():
        provider = BasicTokenProvider(token=TOKEN_VALUE)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) == TOKEN_VALUE

    @staticmethod
    def test_callback_token_provider():
        provider = CallbackTokenProvider(lambda: TOKEN_VALUE)
        token = provider.get_token()
        assert TokenProviderTests.get_token_value(token) == TOKEN_VALUE

        provider = CallbackTokenProvider(lambda: 0)  # token is not a string
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

        if user_msi_client_id is not None:
            args = {"client_id": user_msi_client_id}
            provider = MsiTokenProvider(KUSTO_URI, args)
            token = provider.get_token()
            assert TokenProviderTests.get_token_value(token) is not None

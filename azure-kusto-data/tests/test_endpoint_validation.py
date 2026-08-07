# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from unittest.mock import AsyncMock, Mock, patch

import pytest

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data._cloud_settings import DEFAULT_PUBLIC_LOGIN_URL
from azure.kusto.data.aio import KustoClient as AsyncKustoClient
from azure.kusto.data.exceptions import KustoClientInvalidConnectionStringException
from azure.kusto.data.kusto_trusted_endpoints import KustoTrustedEndpoints, MatchRule


UNTRUSTED_HOST = "https://kusto.attacker.example.com"
TRUSTED_HOST = "https://somecluster.kusto.windows.net"
TOKEN = "a token that must not be transmitted"


def _sync_kcsb(authentication_method, token_callback):
    if authentication_method == "user_token":
        return KustoConnectionStringBuilder.with_aad_user_token_authentication(UNTRUSTED_HOST, TOKEN)
    if authentication_method == "application_token":
        return KustoConnectionStringBuilder.with_aad_application_token_authentication(UNTRUSTED_HOST, TOKEN)
    return KustoConnectionStringBuilder.with_token_provider(UNTRUSTED_HOST, token_callback)


def _async_kcsb(authentication_method, token_callback):
    if authentication_method == "user_token":
        return KustoConnectionStringBuilder.with_aad_user_token_authentication(UNTRUSTED_HOST, TOKEN)
    if authentication_method == "application_token":
        return KustoConnectionStringBuilder.with_aad_application_token_authentication(UNTRUSTED_HOST, TOKEN)
    return KustoConnectionStringBuilder.with_async_token_provider(UNTRUSTED_HOST, token_callback)


@pytest.mark.parametrize("authentication_method", ["user_token", "application_token", "callback"])
def test_sync_authentication_rejects_untrusted_host_before_token_use(authentication_method):
    token_callback = Mock(return_value=TOKEN)
    kcsb = _sync_kcsb(authentication_method, token_callback)

    with (
        patch("azure.kusto.data.client_base.CloudSettings.get_cloud_info_for_cluster") as cloud_info,
        patch("azure.kusto.data.security._get_header_from_dict") as build_header,
        patch("requests.get") as requests_get,
        patch("requests.Session.get") as session_get,
        patch("requests.Session.post") as session_post,
    ):
        with KustoClient(kcsb) as client:
            with pytest.raises(KustoClientInvalidConnectionStringException):
                client.execute_query("database", "print 1")

    token_callback.assert_not_called()
    cloud_info.assert_not_called()
    build_header.assert_not_called()
    requests_get.assert_not_called()
    session_get.assert_not_called()
    session_post.assert_not_called()


def test_cloud_login_endpoint_is_resolved_only_for_builtin_candidate():
    trusted_endpoints = KustoTrustedEndpoints()
    login_endpoint = Mock(return_value=DEFAULT_PUBLIC_LOGIN_URL)

    trusted_endpoints.validate_trusted_endpoint(TRUSTED_HOST, login_endpoint)
    login_endpoint.assert_called_once_with()

    login_endpoint.reset_mock()
    with pytest.raises(KustoClientInvalidConnectionStringException):
        trusted_endpoints.validate_trusted_endpoint(UNTRUSTED_HOST, login_endpoint)
    login_endpoint.assert_not_called()


def test_additional_trusted_host_does_not_resolve_cloud_login_endpoint():
    trusted_endpoints = KustoTrustedEndpoints()
    trusted_endpoints.add_trusted_hosts([MatchRule(".example.com", False)], False)
    login_endpoint = Mock()

    trusted_endpoints.validate_trusted_endpoint(UNTRUSTED_HOST, login_endpoint)

    login_endpoint.assert_not_called()


def test_override_policy_preserves_additional_trusted_host_fallback():
    trusted_endpoints = KustoTrustedEndpoints()
    trusted_endpoints.set_override_policy(lambda hostname: False)
    trusted_endpoints.add_trusted_hosts([MatchRule(".example.com", False)], False)
    login_endpoint = Mock()

    trusted_endpoints.validate_trusted_endpoint(UNTRUSTED_HOST, login_endpoint)

    login_endpoint.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("authentication_method", ["user_token", "application_token", "callback"])
async def test_async_authentication_rejects_untrusted_host_before_token_use(authentication_method):
    token_callback = AsyncMock(return_value=TOKEN)
    kcsb = _async_kcsb(authentication_method, token_callback)

    with (
        patch("azure.kusto.data.client_base.CloudSettings.get_cloud_info_for_cluster") as cloud_info,
        patch("azure.kusto.data.security._get_header_from_dict") as build_header,
        patch("requests.get") as requests_get,
        patch("requests.Session.get") as session_get,
        patch("azure.kusto.data.aio.client.ClientSession.post") as session_post,
    ):
        async with AsyncKustoClient(kcsb) as client:
            with pytest.raises(KustoClientInvalidConnectionStringException):
                await client.execute_query("database", "print 1")

    token_callback.assert_not_awaited()
    cloud_info.assert_not_called()
    build_header.assert_not_called()
    requests_get.assert_not_called()
    session_get.assert_not_called()
    session_post.assert_not_called()
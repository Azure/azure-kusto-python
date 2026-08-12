# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
"""Trusted-endpoint validation must be enforced for every authentication method.

Token-based and callback-based authentication used to bypass validation entirely, which let a
connection string send its Authorization header to an arbitrary host.
"""

import asyncio
from unittest.mock import patch

import pytest

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoClientInvalidConnectionStringException
from azure.kusto.data.kusto_trusted_endpoints import MatchRule, well_known_kusto_endpoints

UNTRUSTED_HOST = "https://kusto.attacker.example.com"
TRUSTED_HOST = "https://somecluster.kusto.windows.net"
TOKEN = "a token that must never leave the machine"


def _bypassing_auth_kcsbs(cluster: str):
    """Connection strings whose token providers do not derive from CloudInfoTokenProvider."""
    return {
        "user_token": KustoConnectionStringBuilder.with_aad_user_token_authentication(cluster, TOKEN),
        "application_token": KustoConnectionStringBuilder.with_aad_application_token_authentication(cluster, TOKEN),
        "token_provider": KustoConnectionStringBuilder.with_token_provider(cluster, lambda: TOKEN),
        "async_token_provider": KustoConnectionStringBuilder.with_async_token_provider(cluster, lambda: asyncio.sleep(0, result=TOKEN)),
    }


@pytest.fixture(params=["user_token", "application_token", "token_provider", "async_token_provider"])
def bypassing_auth_name(request):
    return request.param


class TestEndpointValidation:
    def test_untrusted_host_is_rejected_for_token_based_auth(self, bypassing_auth_name):
        kcsb = _bypassing_auth_kcsbs(UNTRUSTED_HOST)[bypassing_auth_name]
        with KustoClient(kcsb) as client:
            with pytest.raises(KustoClientInvalidConnectionStringException):
                client.execute_query("PythonTest", "Deft")

    def test_untrusted_host_is_rejected_before_any_network_call(self, bypassing_auth_name):
        """Validation must not contact the untrusted host, otherwise it is an SSRF primitive."""
        kcsb = _bypassing_auth_kcsbs(UNTRUSTED_HOST)[bypassing_auth_name]
        with patch("requests.get") as mock_get, patch("requests.Session.get") as mock_session_get, patch("requests.Session.post") as mock_post:
            with KustoClient(kcsb) as client:
                with pytest.raises(KustoClientInvalidConnectionStringException):
                    client.execute_query("PythonTest", "Deft")
            assert not mock_get.called
            assert not mock_session_get.called
            assert not mock_post.called

    def test_explicitly_trusted_host_needs_no_cloud_metadata(self):
        """Hosts trusted via add_trusted_hosts must not require the metadata endpoint."""
        try:
            well_known_kusto_endpoints.add_trusted_hosts([MatchRule("kusto.attacker.example.com", True)], False)
            resolved = []

            def resolver():
                resolved.append(True)
                return "https://login.microsoftonline.com"

            well_known_kusto_endpoints.validate_trusted_endpoint(UNTRUSTED_HOST, resolver)
            assert not resolved
        finally:
            well_known_kusto_endpoints.add_trusted_hosts(None, True)

    def test_login_endpoint_resolved_only_for_allow_listed_hosts(self):
        resolved = []

        def resolver():
            resolved.append(True)
            return "https://login.microsoftonline.com"

        well_known_kusto_endpoints.validate_trusted_endpoint(TRUSTED_HOST, resolver)
        assert resolved

        resolved.clear()
        with pytest.raises(KustoClientInvalidConnectionStringException):
            well_known_kusto_endpoints.validate_trusted_endpoint(UNTRUSTED_HOST, resolver)
        assert not resolved

    def test_plain_string_login_endpoint_still_supported(self):
        well_known_kusto_endpoints.validate_trusted_endpoint(TRUSTED_HOST, "https://login.microsoftonline.com")
        with pytest.raises(KustoClientInvalidConnectionStringException):
            well_known_kusto_endpoints.validate_trusted_endpoint(UNTRUSTED_HOST, "https://login.microsoftonline.com")

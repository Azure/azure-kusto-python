# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import pytest

from azure.kusto.data._cloud_settings import CloudSettings, CloudInfo


@pytest.fixture
def clear_cache():
    """Fixture to clear the CloudSettings cache before each test"""
    with CloudSettings._cloud_cache_lock:
        CloudSettings._cloud_cache.clear()
    yield
    # Clean up after test if needed
    with CloudSettings._cloud_cache_lock:
        CloudSettings._cloud_cache.clear()


def test_normalize_uri_extracts_authority():
    """Test that _normalize_uri extracts only the authority part (schema, host, port) from a URI."""
    # Test with various URI formats
    test_cases = [
        ("https://cluster.kusto.windows.net", "https://cluster.kusto.windows.net"),
        ("https://cluster.kusto.windows.net/", "https://cluster.kusto.windows.net"),
        ("https://cluster.kusto.windows.net/v1/rest", "https://cluster.kusto.windows.net"),
        ("https://cluster.kusto.windows.net:443/v1/rest", "https://cluster.kusto.windows.net:443"),
        ("http://localhost:8080/v1/rest/query", "http://localhost:8080"),
        ("https://cluster.kusto.windows.net/database", "https://cluster.kusto.windows.net"),
    ]

    for input_uri, expected_authority in test_cases:
        assert CloudSettings._normalize_uri(input_uri) == expected_authority


def test_cloud_info_cached_by_authority(clear_cache):
    """Test that CloudInfo is cached by authority part of the URI (schema, host, port)."""
    # Create a test CloudInfo object
    test_cloud_info = CloudInfo(
        login_endpoint="https://login.test.com",
        login_mfa_required=False,
        kusto_client_app_id="test-app-id",
        kusto_client_redirect_uri="http://localhost/redirect",
        kusto_service_resource_id="https://test.kusto.windows.net",
        first_party_authority_url="https://login.test.com/tenant-id",
    )

    # Add to cache with a specific URL
    base_url = "https://cluster.kusto.windows.net"
    CloudSettings.add_to_cache(base_url, test_cloud_info)

    # Test that it can be retrieved with different path variations but same authority
    variations = [
        base_url + "/",
        base_url + "/database",
        base_url + "/v1/rest/query",
        base_url + "/some/other/path",
    ]

    for url in variations:
        # Use the internal _normalize_uri to get the cache key
        normalized_url = CloudSettings._normalize_uri(url)
        assert normalized_url == "https://cluster.kusto.windows.net"
        assert normalized_url in CloudSettings._cloud_cache

        # Verify the retrieved CloudInfo is the same instance
        retrieved_info = CloudSettings._cloud_cache[normalized_url]
        assert retrieved_info is test_cloud_info


def test_cloud_info_cached_with_port(clear_cache):
    """Test that URIs with ports are cached separately from those without."""
    # Create two different CloudInfo objects
    cloud_info_default = CloudInfo(
        login_endpoint="https://login.default.com",
        login_mfa_required=False,
        kusto_client_app_id="default-app-id",
        kusto_client_redirect_uri="http://localhost/redirect",
        kusto_service_resource_id="https://default.kusto.windows.net",
        first_party_authority_url="https://login.default.com/tenant-id",
    )

    cloud_info_with_port = CloudInfo(
        login_endpoint="https://login.withport.com",
        login_mfa_required=True,
        kusto_client_app_id="port-app-id",
        kusto_client_redirect_uri="http://localhost/redirect",
        kusto_service_resource_id="https://port.kusto.windows.net",
        first_party_authority_url="https://login.withport.com/tenant-id",
    )

    # Add both to cache with different authorities
    CloudSettings.add_to_cache("https://cluster.kusto.windows.net", cloud_info_default)
    CloudSettings.add_to_cache("https://cluster.kusto.windows.net:443", cloud_info_with_port)

    # Verify they are cached separately
    assert "https://cluster.kusto.windows.net" in CloudSettings._cloud_cache
    assert "https://cluster.kusto.windows.net:443" in CloudSettings._cloud_cache

    # Verify each URI gets the correct CloudInfo
    assert CloudSettings._cloud_cache["https://cluster.kusto.windows.net"] is cloud_info_default
    assert CloudSettings._cloud_cache["https://cluster.kusto.windows.net:443"] is cloud_info_with_port

    # Additional verification with variations
    assert CloudSettings._cloud_cache[CloudSettings._normalize_uri("https://cluster.kusto.windows.net/database")] is cloud_info_default
    assert CloudSettings._cloud_cache[CloudSettings._normalize_uri("https://cluster.kusto.windows.net:443/database")] is cloud_info_with_port

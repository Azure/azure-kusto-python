#!/usr/bin/env python

from urllib.parse import urlparse

def normalize_uri(uri):
    """Extracts and returns the authority part of the URI (schema, host, port)"""
    url_parts = urlparse(uri)
    # Return only the scheme and netloc (which contains host and port if present)
    return f"{url_parts.scheme}://{url_parts.netloc}"

def test_normalize_uri():
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
        actual = normalize_uri(input_uri)
        assert actual == expected_authority, f"Failed for {input_uri}: expected {expected_authority}, got {actual}"

if __name__ == "__main__":
    test_normalize_uri()
    print("All tests passed!")
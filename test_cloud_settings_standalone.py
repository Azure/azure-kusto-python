import sys
from urllib.parse import urlparse


def normalize_uri(kusto_uri):
    """Extracts and returns the authority part of the URI (schema, host, port)"""
    url_parts = urlparse(kusto_uri)
    # Return only the scheme and netloc (which contains host and port if present)
    return f"{url_parts.scheme}://{url_parts.netloc}"


test_cases = [
    ("https://cluster.kusto.windows.net", "https://cluster.kusto.windows.net"),
    ("https://cluster.kusto.windows.net/", "https://cluster.kusto.windows.net"),
    ("https://cluster.kusto.windows.net/v1/rest", "https://cluster.kusto.windows.net"),
    ("https://cluster.kusto.windows.net:443/v1/rest", "https://cluster.kusto.windows.net:443"),
    ("http://localhost:8080/v1/rest/query", "http://localhost:8080"),
    ("https://cluster.kusto.windows.net/database", "https://cluster.kusto.windows.net"),
]

success = True
for input_uri, expected_authority in test_cases:
    result = normalize_uri(input_uri)
    if result != expected_authority:
        print(f"FAIL: For input '{input_uri}', expected '{expected_authority}', got '{result}'")
        success = False
    else:
        print(f"PASS: '{input_uri}' → '{result}'")

if success:
    print("\nAll tests PASSED!")
    sys.exit(0)
else:
    print("\nFAILED tests detected.")
    sys.exit(1)

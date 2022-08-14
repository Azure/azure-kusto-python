from azure.kusto.data.kustoTrustedEndpoints import _FastSuffixMatcher,_wellKnownKustoEndpointsData

def test_well_known_kusto_endpoints_data():
    """Test the data load."""
    locate_suffix = False
    locate_host = False
    for k, v in _wellKnownKustoEndpointsData["AllowedEndpointsByLogin"].items():
        for suffix in v["AllowedKustoSuffixes"]:
            assert suffix.startswith(".")

        # Check for duplicates
        suffixSet = set(v["AllowedKustoSuffixes"])
        hostSet = set(v["AllowedKustoHostnames"])
        diff =  len(v["AllowedKustoSuffixes"]) - len(suffixSet)
        assert  0 == diff

        # Search for one expected URL in one of the lists
        locate_suffix |= ".kusto.windows.net" in suffixSet
        locate_host |= "kusto.aria.microsoft.com" in hostSet
    assert locate_suffix
    assert locate_host


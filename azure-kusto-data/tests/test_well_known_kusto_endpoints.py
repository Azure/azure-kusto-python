from azure.kusto.data._cloud_settings import DEFAULT_PUBLIC_LOGIN_URL
from azure.kusto.data.exceptions import KustoClientInvalidConnectionStringException
from azure.kusto.data.kusto_trusted_endpoints import _well_known_kusto_endpoints_data, well_known_kusto_endpoints, MatchRule

CHINA_CLOUD_LOGIN = "https://login.partner.microsoftonline.cn"


def test_well_known_kusto_endpoints_data():
    """Test the data load."""
    locate_suffix = False
    locate_host = False
    for k, v in _well_known_kusto_endpoints_data["AllowedEndpointsByLogin"].items():
        for suffix in v["AllowedKustoSuffixes"]:
            assert suffix.startswith(".")

        # Check for duplicates
        suffixSet = set(v["AllowedKustoSuffixes"])
        hostSet = set(v["AllowedKustoHostnames"])
        diff = len(v["AllowedKustoSuffixes"]) - len(suffixSet)
        assert 0 == diff

        # Search for one expected URL in one of the lists
        locate_suffix |= ".kusto.windows.net" in suffixSet
        locate_host |= "kusto.aria.microsoft.com" in hostSet
    assert locate_suffix
    assert locate_host


def test_well_known_kusto_endpoints_random_kusto_clusters():
    for c in [
        "https://127.0.0.1",
        "https://127.1.2.3",
        "https://kustozszokb5yrauyq.westeurope.kusto.windows.net",
        "https://kustofrbwrznltavls.centralus.kusto.windows.net",
        "https://kusto7j53clqswr4he.germanywestcentral.kusto.windows.net",
        "https://rpe2e0422132101fct2.eastus2euap.kusto.windows.net",
        "https://kustooq2gdfraeaxtq.westcentralus.kusto.windows.net",
        "https://kustoesp3ewo4s5cow.westcentralus.kusto.windows.net",
        "https://kustowmd43nx4ihnjs.southeastasia.kusto.windows.net",
        "https://createt210723t0601.westus2.kusto.windows.net",
        "https://kusto2rkgmaskub3fy.eastus2.kusto.windows.net",
        "https://kustou7u32pue4eij4.australiaeast.kusto.windows.net",
        "https://kustohme3e2jnolxys.northeurope.kusto.windows.net",
        "https://kustoas7cx3achaups.southcentralus.kusto.windows.net",
        "https://rpe2e0104160100act.westus2.kusto.windows.net",
        "https://kustox5obddk44367y.southcentralus.kusto.windows.net",
        "https://kustortnjlydpe5l6u.canadacentral.kusto.windows.net",
        "https://kustoz74sj7ikkvftk.southeastasia.kusto.windows.net",
        "https://rpe2e1004182350fctf.westus2.kusto.windows.net",
        "https://rpe2e1115095448act.westus2.kusto.windows.net",
        "https://kustoxenx32x3tuznw.southafricawest.kusto.windows.net",
        "https://kustowc3m5jpqtembw.canadacentral.kusto.windows.net",
        "https://rpe2e1011182056fctf.westus2.kusto.windows.net",
        "https://kusto3ge6xthiafqug.eastus.kusto.windows.net",
        "https://teamsauditservice.westus.kusto.windows.net",
        "https://kustooubnzekmh4doy.canadacentral.kusto.windows.net",
        "https://rpe2e1206081632fct2f.westus2.kusto.windows.net",
        "https://stopt402211020t0606.automationtestworkspace402.kusto.azuresynapse.net",
        "https://delt402210818t2309.automationtestworkspace402.kusto.azuresynapse.net",
        "https://kusto42iuqj4bejjxq.koreacentral.kusto.windows.net",
        "https://kusto3rv75hibmg6vu.southeastasia.kusto.windows.net",
        "https://kustogmhxb56nqjrje.westus2.kusto.windows.net",
        "https://kustozu5wg2p3aw3um.koreasouth.kusto.windows.net",
        "https://kustos36f2amn2agwk.australiaeast.kusto.windows.net",
        "https://kustop4htq3k676jau.eastus.kusto.windows.net",
        "https://kustojdny5lga53cts.southcentralus.kusto.windows.net",
        "https://customerportalprodeast.kusto.windows.net",
        "https://rpe2e0730231650und.westus2.kusto.windows.net",
        "https://kusto7lxdbebadivjw.southeastasia.kusto.windows.net",
        "https://alprd2neu000003s.northeurope.kusto.windows.net",
        "https://kustontnwqy3eler5g.northeurope.kusto.windows.net",
        "https://kustoap2wpozj7qpio.eastus.kusto.windows.net",
        "https://kustoajnxslghxlee4.japaneast.kusto.windows.net",
        "https://oiprdseau234x.australiasoutheast.kusto.windows.net",
        "https://kusto7yevbo7ypsnx4.germanywestcentral.kusto.windows.net",
        "https://kustoagph5odbqyquq.westus3.kusto.windows.net",
        "https://kustovs2hxo3ftud5e.westeurope.kusto.windows.net",
        "https://kustorzuk2dgiwdryc.uksouth.kusto.windows.net",
        "https://kustovsb4ogsdniwqk.eastus2.kusto.windows.net",
        "https://kusto3g3mpmkm3p3xc.switzerlandnorth.kusto.windows.net",
        "https://kusto2e2o7er7ypx2o.westus2.kusto.windows.net",
        "https://kustoa3qqlh23yksim.southafricawest.kusto.windows.net",
        "https://rpe2evnt11021711comp.rpe2evnt11021711-wksp.kusto.azuresynapse.net",
        "https://cdpkustoausas01.australiasoutheast.kusto.windows.net",
        "https://testinge16cluster.uksouth.kusto.windows.net",
        "https://testkustopoolbs6ond.workspacebs6ond.kusto.azuresynapse.net",
        "https://offnodereportingbcdr1.southcentralus.kusto.windows.net",
        "https://mhstorage16red.westus.kusto.windows.net",
        "https://kusto7kza5q2fmnh2w.northeurope.kusto.windows.net",
        "https://tvmquerycanc.centralus.kusto.windows.net",
        "https://kustowrcde4olp4zho.eastus.kusto.windows.net",
        "https://delt403210910t0727.automationtestworkspace403.kusto.azuresynapse.net",
        "https://foprdcq0004.brazilsouth.kusto.windows.net",
        "https://rpe2e0827133746fctf.eastus2euap.kusto.windows.net",
        "https://kustoz7yrvoaoa2yaa.australiaeast.kusto.windows.net",
        "https://rpe2e1203125809und.westus2.kusto.windows.net",
        "https://kustoywilbpggrltk4.francecentral.kusto.windows.net",
        "https://stopt402210825t0408.automationtestworkspace402.kusto.azuresynapse.net",
        "https://kustonryfjo5klvrh4.westeurope.kusto.windows.net",
        "https://kustowwqgogzpseg6o.eastus2.kusto.windows.net",
        "https://kustor3gjpwqum3olw.canadacentral.kusto.windows.net",
    ]:
        _validate_endpoint(c, DEFAULT_PUBLIC_LOGIN_URL)

        # Test case sensitivity
        clusterName = c.upper()
        _validate_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL)

        # Test MFA endpoints
        if not "synapse" in c:
            clusterName = c.replace(".kusto.", ".kustomfa.")
            _validate_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL)

        # Test dev endpoints
        if not "synapse" in c:
            clusterName = c.replace(".kusto.", ".kustodev.")
            _validate_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL)


def test_well_known_kusto_endpoints_national_clouds():
    for c in [
        "https://kustozszokb5yrauyq.kusto.chinacloudapi.cn,{0}".format(CHINA_CLOUD_LOGIN),
        "https://kustofrbwrznltavls.kusto.usgovcloudapi.net,https://login.microsoftonline.us",
        "https://kusto7j53clqswr4he.kusto.core.eaglex.ic.gov,https://login.microsoftonline.eaglex.ic.gov",
        "https://rpe2e0422132101fct2.kusto.core.microsoft.scloud,https://login.microsoftonline.microsoft.scloud",
        "https://kustozszokb5yrauyq.kusto.chinacloudapi.cn,{0}".format(CHINA_CLOUD_LOGIN),
        "https://kustofrbwrznltavls.kusto.usgovcloudapi.net,https://login.microsoftonline.us",
        "https://kusto7j53clqswr4he.kusto.core.eaglex.ic.gov,https://login.microsoftonline.eaglex.ic.gov",
        "https://rpe2e0422132101fct2.kusto.core.microsoft.scloud,https://login.microsoftonline.microsoft.scloud",
    ]:
        clusterAndLoginEndpoint = c.split(",")
        _validate_endpoint(clusterAndLoginEndpoint[0], clusterAndLoginEndpoint[1])
        # Test case sensitivity
        _validate_endpoint(clusterAndLoginEndpoint[0].upper(), clusterAndLoginEndpoint[1].upper())


def test_well_known_kusto_endpoints_proxy_test():
    for c in [
        "https://kusto.aria.microsoft.com,{0}".format(DEFAULT_PUBLIC_LOGIN_URL),
        "https://ade.loganalytics.io,{0}".format(DEFAULT_PUBLIC_LOGIN_URL),
        "https://ade.applicationinsights.io,{0}".format(DEFAULT_PUBLIC_LOGIN_URL),
        "https://kusto.aria.microsoft.com,{0}".format(DEFAULT_PUBLIC_LOGIN_URL),
        "https://adx.monitor.azure.com,{0}".format(DEFAULT_PUBLIC_LOGIN_URL),
        "https://cluster.playfab.com,{0}".format(DEFAULT_PUBLIC_LOGIN_URL),
        "https://cluster.playfabapi.com,{0}".format(DEFAULT_PUBLIC_LOGIN_URL),
        "https://cluster.playfab.cn,{0}".format(CHINA_CLOUD_LOGIN),
    ]:
        clusterAndLoginEndpoint = c.split(",")
        _validate_endpoint(clusterAndLoginEndpoint[0], clusterAndLoginEndpoint[1])
        # Test case sensitivity
        _validate_endpoint(clusterAndLoginEndpoint[0].upper(), clusterAndLoginEndpoint[1].upper())


def test_well_known_kusto_endpoints_proxy_negative_test():
    for clusterName in [
        "https://cluster.kusto.aria.microsoft.com",
        "https://cluster.eu.kusto.aria.microsoft.com",
        "https://cluster.ade.loganalytics.io",
        "https://cluster.ade.applicationinsights.io",
        "https://cluster.adx.monitor.azure.com",
        "https://cluster.adx.applicationinsights.azure.cn",
        "https://cluster.adx.monitor.azure.eaglex.ic.gov",
    ]:
        _check_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, True)


def test_well_known_kusto_endpoints_negative():
    for clusterName in [
        "https://localhostess",
        "https://127.0.0.1.a",
        "https://some.azurewebsites.net",
        "https://kusto.azurewebsites.net",
        "https://test.kusto.core.microsoft.scloud",
        "https://cluster.kusto.azuresynapse.azure.cn",
    ]:
        _check_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, True)


def test_well_known_kusto_endpoints_override():
    try:
        well_known_kusto_endpoints.set_override_policy(lambda hostname: True)
        _check_endpoint("https://kusto.kusto.windows.net", "", False)
        _check_endpoint("https://bing.com", "", False)

        well_known_kusto_endpoints.set_override_policy(lambda hostname: False)
        _check_endpoint("https://kusto.kusto.windows.net", "", True)
        _check_endpoint("https://bing.com", "", True)

        well_known_kusto_endpoints.set_override_policy(None)
        _check_endpoint("https://kusto.kusto.windows.net", DEFAULT_PUBLIC_LOGIN_URL, False)
        _check_endpoint("https://bing.com", DEFAULT_PUBLIC_LOGIN_URL, True)
    finally:
        well_known_kusto_endpoints.set_override_policy(None)


def test_wellKnownKustoEndpoints_AdditionalWebsites():
    well_known_kusto_endpoints.add_trusted_hosts([MatchRule(".someotherdomain1.net", False)], True)

    # 2nd call - to validate that addition works
    well_known_kusto_endpoints.add_trusted_hosts([MatchRule("www.someotherdomain2.net", True)], False)
    well_known_kusto_endpoints.add_trusted_hosts([MatchRule("www.someotherdomain3.net", True)], False)

    for clusterName in ["https://some.someotherdomain1.net", "https://www.someotherdomain2.net"]:
        _check_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, False)

    for clusterName in ["https://some.someotherdomain2.net"]:
        _check_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, True)

    # Reset additional hosts
    well_known_kusto_endpoints.add_trusted_hosts(None, True)
    # Validate that hosts are not allowed anymore
    for clusterName in ["https://some.someotherdomain1.net", "https://www.someotherdomain2.net"]:
        _check_endpoint(clusterName, DEFAULT_PUBLIC_LOGIN_URL, True)


def _check_endpoint(clusterName, defaultPublicLoginUrl, expectFail):
    if expectFail:
        try:
            _validate_endpoint(clusterName, defaultPublicLoginUrl)
        except KustoClientInvalidConnectionStringException:
            pass
    else:
        _validate_endpoint(clusterName, defaultPublicLoginUrl)


def _validate_endpoint(address, login_endpoint):
    well_known_kusto_endpoints.validate_trusted_endpoint(address, login_endpoint)

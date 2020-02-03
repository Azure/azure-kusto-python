"""Tests for security module."""

from azure.kusto.data.exceptions import KustoAuthenticationError
from azure.kusto.data.request import KustoConnectionStringBuilder
from azure.kusto.data.security import _AadHelper, AuthenticationMethod


def test_unauthorized_exception():
    """Test the exception thrown when authorization fails."""
    cluster = "https://somecluster.kusto.windows.net"
    username = "username@microsoft.com"
    kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, username, "StrongestPasswordEver", "authorityName")
    aad_helper = _AadHelper(kcsb)

    try:
        aad_helper.acquire_authorization_header()
    except KustoAuthenticationError as error:
        assert error.authentication_method == AuthenticationMethod.aad_username_password.value
        assert error.authority == "https://login.microsoftonline.com/authorityName"
        assert error.kusto_cluster == cluster
        assert error.kwargs["username"] == username


def test_msi_auth():
    """
    * * * Note * * *
    Each connection test takes about 15-20 seconds which is the time it takes TCP to fail connecting to the nonexistent MSI endpoint
    The timeout option does not seem to affect this behavior. Could be it only affects the waiting time fora response in successful connections.
    Please be prudent in adding any future tests!
    """
    client_guid = "kjhjk"
    object_guid = "87687687"
    res_guid = "kajsdghdijewhag"

    kcsb = [
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", timeout=1),
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", client_id=client_guid, timeout=1),
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", object_id=object_guid, timeout=1),
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", msi_res_id=res_guid, timeout=1),
    ]

    helpers = [_AadHelper(kcsb[0]), _AadHelper(kcsb[1]), _AadHelper(kcsb[2]), _AadHelper(kcsb[3])]

    
    try:
        helpers[0].acquire_authorization_header()
    except KustoAuthenticationError as e:
        assert e.authentication_method == AuthenticationMethod.aad_msi.value
        assert "client_id" not in e.kwargs
        assert "object_id" not in e.kwargs
        assert "msi_res_id" not in e.kwargs

    try:
        helpers[1].acquire_authorization_header()
    except KustoAuthenticationError as e:
        assert e.authentication_method == AuthenticationMethod.aad_msi.value
        assert e.kwargs["client_id"] == client_guid
        assert "object_id" not in e.kwargs
        assert "msi_res_id" not in e.kwargs
        assert str(e.exception).index("client_id") > -1
        assert str(e.exception).index(client_guid) > -1

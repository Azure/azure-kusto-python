"""Tests for security module."""

from azure.kusto.data.exceptions import KustoAuthenticationError
from azure.kusto.data.request import KustoConnectionStringBuilder
from azure.kusto.data.security import _AadHelper, AuthenticationMethod

def test_unauthorized_exception():
    """Test the exception thrown when authorization fails."""
    cluster = "https://somecluster.kusto.windows.net"
    username = "username@microsoft.com"
    kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(
        cluster, username, "StrongestPasswordEver", "authorityName"
    )
    aad_helper = _AadHelper(kcsb)

    try:
        aad_helper.acquire_authorization_header()
    except KustoAuthenticationError as error:
        assert error.authentication_method == AuthenticationMethod.aad_username_password.value
        assert error.authority == "https://login.microsoftonline.com/authorityName"
        assert error.kusto_cluster == cluster
        assert error.kwargs["username"] == username


def test_msi_auth():
    client_guid = "kjhjk"
    object_guid = "87687687"
    res_guid = "kajsdghdijewhag"

    kcsb = [
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost"),
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", AuthenticationMethod.msi_client_id_type, client_guid),
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", AuthenticationMethod.msi_object_id_type, object_guid),
        KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication("localhost", AuthenticationMethod.msi_res_id_type, res_guid)
    ]

    helpers = [
        _AadHelper(kcsb[0]),
        _AadHelper(kcsb[1]),
        _AadHelper(kcsb[2]),
        _AadHelper(kcsb[3]),
    ]

    try:
        helpers[0].acquire_authorization_header()
    except KustoAuthenticationError as e:
        assert e.authentication_method == AuthenticationMethod.aad_msi.value
        assert e.kwargs["msi_type"] == AuthenticationMethod.msi_default_type.value
        assert e.kwargs["msi_id"] == ""

    try:
        helpers[1].acquire_authorization_header()
    except KustoAuthenticationError as e:
        assert e.authentication_method == AuthenticationMethod.aad_msi.value
        assert e.kwargs["msi_type"] == AuthenticationMethod.msi_client_id_type.value
        assert e.kwargs["msi_id"] == client_guid
        assert str(e.exception).index(AuthenticationMethod.msi_client_id_type.value) > -1
        assert str(e.exception).index(client_guid) > -1

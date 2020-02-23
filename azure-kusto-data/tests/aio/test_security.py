"""Tests for security module."""
import pytest
from azure.kusto.data._decorators import aio_documented_by
from azure.kusto.data.aio.security import _AadHelper
from azure.kusto.data.exceptions import KustoAuthenticationError
from azure.kusto.data.request import KustoConnectionStringBuilder
from azure.kusto.data.security import AuthenticationMethod

from ..security_common import _prepare_msi_auth, _assert_msi_auth_0, _assert_msi_auth_1
from ..test_security import test_msi_auth as test_msi_auth_sync, test_unauthorized_exception as test_unauthorized_exception_sync

aio_installed = False
try:
    import asgiref

    aio_installed = True
except:
    pass


@pytest.mark.skipif(not aio_installed, reason="requires aio")
@pytest.mark.asyncio
@aio_documented_by(test_unauthorized_exception_sync)
async def test_unauthorized_exception():
    cluster = "https://somecluster.kusto.windows.net"
    username = "username@microsoft.com"
    kcsb = KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, username, "StrongestPasswordEver", "authorityName")
    aad_helper = _AadHelper(kcsb)

    try:
        await aad_helper.acquire_authorization_header()
    except KustoAuthenticationError as error:
        assert error.authentication_method == AuthenticationMethod.aad_username_password.value
        assert error.authority == "https://login.microsoftonline.com/authorityName"
        assert error.kusto_cluster == cluster
        assert error.kwargs["username"] == username


@pytest.mark.skipif(not aio_installed, reason="requires aio")
@pytest.mark.asyncio
@aio_documented_by(test_msi_auth_sync)
async def test_msi_auth():
    client_guid, helpers = _prepare_msi_auth()

    try:
        await helpers[0].acquire_authorization_header()
    except KustoAuthenticationError as e:
        _assert_msi_auth_0(e)

    try:
        await helpers[1].acquire_authorization_header()
    except KustoAuthenticationError as e:
        _assert_msi_auth_1(client_guid, e)

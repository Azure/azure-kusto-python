"""Tests for security module."""
from azure.kusto.data.exceptions import KustoAuthenticationError

from .security_common import _prepare_unauthorized_exception, _assert_unauthorized_exception, _prepare_msi_auth, _assert_msi_auth_0, _assert_msi_auth_1


def test_unauthorized_exception():
    """Test the exception thrown when authorization fails."""
    aad_helper, cluster, username = _prepare_unauthorized_exception()

    try:
        aad_helper.acquire_authorization_header()
    except KustoAuthenticationError as error:
        _assert_unauthorized_exception(cluster, error, username)


def test_msi_auth():
    """
    * * * Note * * *
    Each connection test takes about 15-20 seconds which is the time it takes TCP to fail connecting to the nonexistent MSI endpoint
    The timeout option does not seem to affect this behavior. Could be it only affects the waiting time fora response in successful connections.
    Please be prudent in adding any future tests!
    """
    client_guid, helpers = _prepare_msi_auth()

    try:
        helpers[0].acquire_authorization_header()
    except KustoAuthenticationError as e:
        _assert_msi_auth_0(e)

    try:
        helpers[1].acquire_authorization_header()
    except KustoAuthenticationError as e:
        _assert_msi_auth_1(client_guid, e)

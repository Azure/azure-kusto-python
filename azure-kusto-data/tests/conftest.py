from typing import Tuple

import pytest

from azure.kusto.data import KustoConnectionStringBuilder
from .kusto_client_common import KustoClientTestsMixin


@pytest.fixture(
    params=[
        "user_password",
        "application_key",
        "application_token",
        "device",
        "user_token",
        "managed_identity",
        "token_provider",
        "async_token_provider",
        "az_cli",
        "interactive_login",
    ]
)
def proxy_kcsb(request) -> Tuple[KustoConnectionStringBuilder, bool]:
    cluster = KustoClientTestsMixin.HOST
    user = "test2"
    password = "Pa$$w0rd2"
    authority_id = "13456"
    uuid = "11111111-1111-1111-1111-111111111111"
    key = "key of application"
    token = "The app hardest token ever"

    return {
        "user_password": (KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, user, password, authority_id), True),
        "application_key": (KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, uuid, key, "microsoft.com"), True),
        "application_token": (KustoConnectionStringBuilder.with_aad_application_token_authentication(cluster, application_token=token), False),
        "device": (KustoConnectionStringBuilder.with_aad_device_authentication(cluster), True),
        "user_token": (KustoConnectionStringBuilder.with_aad_user_token_authentication(cluster, user_token=token), False),
        "managed_identity": (KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster), False),
        "token_provider": (KustoConnectionStringBuilder.with_token_provider(cluster, lambda x: x), False),
        "async_token_provider": (KustoConnectionStringBuilder.with_async_token_provider(cluster, lambda x: x), False),
        "az_cli": (KustoConnectionStringBuilder.with_az_cli_authentication(cluster), True),
        "interactive_login": (KustoConnectionStringBuilder.with_interactive_login(cluster), True),
    }[request.param]

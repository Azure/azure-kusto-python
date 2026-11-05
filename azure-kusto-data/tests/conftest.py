import asyncio
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Tuple

import pytest
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.data._cloud_settings import CloudSettings

from .kusto_client_common import KustoClientTestsMixin


@pytest.fixture(params=[301, 302, 307, 308])
def redirect_server(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[str, int, list[str]]]:
    code: int = request.param
    requests_seen: list[str] = []

    class RedirectHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self._respond()

        def do_POST(self) -> None:
            self._respond()

        def _respond(self) -> None:
            requests_seen.append(self.path)
            self.send_response(200 if self.path == "/redirected" else code)
            if self.path != "/redirected":
                self.send_header("Location", "/redirected")
            self.send_header("Content-Length", "0")
            self.end_headers()

    with ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler) as server:
        cluster = f"http://127.0.0.1:{server.server_port}"
        monkeypatch.setattr(CloudSettings, "_cloud_cache", {cluster: CloudSettings.DEFAULT_CLOUD})
        monkeypatch.setenv("NO_PROXY", "127.0.0.1")
        monkeypatch.setenv("no_proxy", "127.0.0.1")
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield cluster, code, requests_seen
        finally:
            server.shutdown()
            thread.join()


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

    kcsb_dict = {
        "user_password": (KustoConnectionStringBuilder.with_aad_user_password_authentication(cluster, user, password, authority_id), True),
        "application_key": (KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, uuid, key, "microsoft.com"), True),
        "application_token": (KustoConnectionStringBuilder.with_aad_application_token_authentication(cluster, token), False),
        "device": (KustoConnectionStringBuilder.with_aad_device_authentication(cluster), True),
        "user_token": (KustoConnectionStringBuilder.with_aad_user_token_authentication(cluster, token), False),
        "managed_identity": (KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster), False),
        "token_provider": (KustoConnectionStringBuilder.with_token_provider(cluster, lambda: ""), False),
        "async_token_provider": (KustoConnectionStringBuilder.with_async_token_provider(cluster, lambda: asyncio.sleep(0, result="")), False),
        "az_cli": (KustoConnectionStringBuilder.with_az_cli_authentication(cluster), True),
        "interactive_login": (KustoConnectionStringBuilder.with_interactive_login(cluster), True),
    }

    return kcsb_dict[request.param]

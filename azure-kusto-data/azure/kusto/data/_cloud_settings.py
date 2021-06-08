import os
from functools import lru_cache
from typing import Optional
from urllib.parse import urljoin

import requests

from azure.kusto.data.exceptions import KustoServiceError

AUTH_ENV_VAR_NAME = "AadAuthorityUri"
KUSTO_CLIENT_APP_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
PUBLIC_LOGIN_URL = "https://login.microsoftonline.com"
REDIRECT_URI = "https://microsoft/kustoclient"
KUSTO_SERVICE_RESOURCE_ID = "https://kusto.kusto.windows.net"
FIRST_PARTY_AUTHORITY_URL = "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a"


class CloudInfo:
    """This class holds the data for a specific cloud instance."""

    def __init__(
        self,
        login_endpoint: str,
        login_mfa_required: bool,
        kusto_client_app_id: str,
        kusto_client_redirect_uri: str,
        kusto_service_resource_id: str,
        first_party_authority_url: str,
    ):
        self.login_endpoint = login_endpoint
        self.login_mfa_required = login_mfa_required
        self.kusto_client_app_id = kusto_client_app_id
        self.kusto_client_redirect_uri = kusto_client_redirect_uri  # will be used for interactive login
        self.kusto_service_resource_id = kusto_service_resource_id
        self.first_party_authority_url = first_party_authority_url

    def authority_uri(self, authority_id: Optional[str]):
        return self.login_endpoint + "/" + (authority_id or "organizations")


class CloudSettings:
    """This class holds data for all cloud instances, and returns the specific data instance by parsing the dns suffix from a URL"""

    METADATA_ENDPOINT = "v1/rest/auth/metadata"

    _cloud_info = None

    DEFAULT_CLOUD = CloudInfo(
        login_endpoint=os.environ.get(AUTH_ENV_VAR_NAME, PUBLIC_LOGIN_URL),
        login_mfa_required=False,
        kusto_client_app_id=KUSTO_CLIENT_APP_ID,
        kusto_client_redirect_uri=REDIRECT_URI,
        kusto_service_resource_id=KUSTO_SERVICE_RESOURCE_ID,
        first_party_authority_url=FIRST_PARTY_AUTHORITY_URL,
    )

    @classmethod
    @lru_cache(maxsize=None)
    def get_cloud_info_for_cluster(cls, kusto_uri: str) -> CloudInfo:
        result = requests.get(urljoin(kusto_uri, cls.METADATA_ENDPOINT))

        if result.status_code == 200:
            content = result.json()
            if content is None or content == {}:
                raise KustoServiceError("Kusto returned an invalid cloud metadata response", result)
            root = content["AzureAD"]
            return CloudInfo(
                login_endpoint=root["LoginEndpoint"],
                login_mfa_required=root["LoginMfaRequired"],
                kusto_client_app_id=root["KustoClientAppId"],
                kusto_client_redirect_uri=root["KustoClientRedirectUri"],
                kusto_service_resource_id=root["KustoServiceResourceId"],
                first_party_authority_url=root["FirstPartyAuthorityUrl"],
            )
        elif result.status_code == 404:
            # For now as long not all proxies implement the metadata endpoint, if no endpoint exists return public cloud data
            return cls.DEFAULT_CLOUD
        else:
            raise KustoServiceError("Kusto returned an invalid cloud metadata response", result)

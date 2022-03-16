import dataclasses
import logging
import os
from threading import Lock
from typing import Optional, Dict
from urllib.parse import urljoin

import requests

from azure.kusto.data.exceptions import KustoServiceError

METADATA_ENDPOINT = "v1/rest/auth/metadata"

DEFAULT_AUTH_ENV_VAR_NAME = "AadAuthorityUri"
DEFAULT_KUSTO_CLIENT_APP_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
DEFAULT_PUBLIC_LOGIN_URL = "https://login.microsoftonline.com"
DEFAULT_REDIRECT_URI = "https://microsoft/kustoclient"
DEFAULT_KUSTO_SERVICE_RESOURCE_ID = "https://kusto.kusto.windows.net"
DEFAULT_FIRST_PARTY_AUTHORITY_URL = "https://login.microsoftonline.com/f8cdef31-a31e-4b4a-93e4-5f571e91255a"

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class CloudInfo:
    """This class holds the data for a specific cloud instance."""

    login_endpoint: str
    login_mfa_required: bool
    kusto_client_app_id: str
    kusto_client_redirect_uri: str  # will be used for interactive login
    kusto_service_resource_id: str
    first_party_authority_url: str

    def authority_uri(self, authority_id: Optional[str]):
        return self.login_endpoint + "/" + (authority_id or "organizations")


class CloudSettings:
    """This class holds data for all cloud instances, and returns the specific data instance by parsing the dns suffix from a URL"""

    _cloud_info = None
    _cloud_cache = {}
    _cloud_cache_lock = Lock()

    DEFAULT_CLOUD = CloudInfo(
        login_endpoint=os.environ.get(DEFAULT_AUTH_ENV_VAR_NAME, DEFAULT_PUBLIC_LOGIN_URL),
        login_mfa_required=False,
        kusto_client_app_id=DEFAULT_KUSTO_CLIENT_APP_ID,
        kusto_client_redirect_uri=DEFAULT_REDIRECT_URI,
        kusto_service_resource_id=DEFAULT_KUSTO_SERVICE_RESOURCE_ID,
        first_party_authority_url=DEFAULT_FIRST_PARTY_AUTHORITY_URL,
    )

    @classmethod
    def get_cloud_info_for_cluster(cls, kusto_uri: str, proxies: Optional[Dict[str, str]] = None) -> CloudInfo:
        logger.info("Getting cloud info for cluster '%s'", kusto_uri)
        if kusto_uri in cls._cloud_cache:  # Double-checked locking to avoid unnecessary lock access
            logger.info("Found cloud for cluster '%s' info in cache", kusto_uri)
            return cls._cloud_cache[kusto_uri]

        with cls._cloud_cache_lock:
            if kusto_uri in cls._cloud_cache:
                logger.info("Found cloud for cluster '%s' info in cache", kusto_uri)
                return cls._cloud_cache[kusto_uri]

            result = requests.get(urljoin(kusto_uri, METADATA_ENDPOINT), proxies=proxies)

            if result.status_code == 200:
                content = result.json()
                if content is None or content == {}:
                    raise KustoServiceError("Kusto returned an invalid cloud metadata response", result)
                root = content["AzureAD"]
                if root is not None:
                    cls._cloud_cache[kusto_uri] = CloudInfo(
                        login_endpoint=root["LoginEndpoint"],
                        login_mfa_required=root["LoginMfaRequired"],
                        kusto_client_app_id=root["KustoClientAppId"],
                        kusto_client_redirect_uri=root["KustoClientRedirectUri"],
                        kusto_service_resource_id=root["KustoServiceResourceId"],
                        first_party_authority_url=root["FirstPartyAuthorityUrl"],
                    )
                    logger.debug("Retrieved cloud info for cluster %s from web - %s", kusto_uri, cls._cloud_cache[kusto_uri])
                else:
                    cls._cloud_cache[kusto_uri] = cls.DEFAULT_CLOUD
                    logger.warning("Received unknown json('%s') when trying to retrieve cloud info for cluster '%s', using default", root, kusto_uri)
            elif result.status_code == 404:
                # For now as long not all proxies implement the metadata endpoint, if no endpoint exists return public cloud data
                logger.warning("Received 404 error when trying to retrieve cloud info for cluster '%s', using default", kusto_uri)
                cls._cloud_cache[kusto_uri] = cls.DEFAULT_CLOUD
            else:
                raise KustoServiceError("Kusto returned an invalid cloud metadata response", result)
            logger.info("Retrieved cloud info for cluster %s", kusto_uri)
            return cls._cloud_cache[kusto_uri]

from urllib.parse import urlparse


class CloudInfo:
    """ This class holds the data for a specific cloud instance. """

    def __init__(self, auth_endpoint: str, kusto_client_app_id: str, first_party_tenant_id: str, redirect_uri: str):
        self.aad_authority_uri = auth_endpoint
        self.kusto_client_app_id = kusto_client_app_id
        self.first_party_tenant_id = first_party_tenant_id
        self.login_redirect_uri = redirect_uri


class CloudSettings:
    """ This class holds data for all cloud instances, and returns the specific data instance by parsing the dns suffix from a URL """

    public_cloud_suffix = "windows.net"
    moon_cake_cloud_suffix = "chinacloudapi.cn"
    black_forest_cloud_suffix = "cloudapi.de"
    fairfax_cloud_suffix = "usgovcloudapi.net"
    us_nat_cloud_suffix = "core.eaglex.ic.gov"
    us_sec_cloud_suffix = "core.microsoft.scloud"

    _initialized = False
    _cloud_info = {}
    _suffix_list = [public_cloud_suffix, moon_cake_cloud_suffix, black_forest_cloud_suffix, fairfax_cloud_suffix, us_nat_cloud_suffix, us_sec_cloud_suffix]

    @classmethod
    def _init_once(cls):
        if cls._initialized:
            return

        cls._initialized = True

        cls._cloud_info[cls.public_cloud_suffix] = CloudInfo(
            "https://login.microsoftonline.com", "db662dc1-0cfe-4e1c-a843-19a68e65be58", "f8cdef31-a31e-4b4a-93e4-5f571e91255a", "https://microsoft/kustoclient"
        )

        cls._cloud_info[cls.moon_cake_cloud_suffix] = CloudInfo(
            "https://login.chinacloudapi.cn",
            "db662dc1-0cfe-4e1c-a843-19a68e65be58",
            "0b4a31a2-c1a0-475d-b363-5f26668660a3",
            "https://ChinaGovCloud.partner.onmschina.cn/kustoclient",
        )

        cls._cloud_info[cls.black_forest_cloud_suffix] = CloudInfo(
            "https://login.microsoftonline.de", "db662dc1-0cfe-4e1c-a843-19a68e65be58", "f577cd82-810c-43f9-a1f6-0cc532871050", "https://microsoft/kustoclient"
        )

        cls._cloud_info[cls.fairfax_cloud_suffix] = CloudInfo(
            "https://login.microsoftonline.us", "730ea9e6-1e1d-480c-9df6-0bb9a90e1a0f", "f8cdef31-a31e-4b4a-93e4-5f571e91255a", "https://microsoft/kustoclient"
        )

        cls._cloud_info[cls.us_nat_cloud_suffix] = CloudInfo(
            "https://login.microsoftonline.eaglex.ic.gov",
            "db662dc1-0cfe-4e1c-a843-19a68e65be58",
            "320923b5-8c1a-49c2-b475-542ab7b15e6b",
            "https://microsoft/kustoclient",
        )

        cls._cloud_info[cls.us_sec_cloud_suffix] = CloudInfo(
            "https://login.microsoftonline.microsoft.scloud",
            "730ea9e6-1e1d-480c-9df6-0bb9a90e1a0f",
            "bd0f7f1b-1c2e-40ad-8910-90dc3af83073",
            "https://microsoft/kustoclient",
        )

    @classmethod
    def _extract_dns_suffix(cls, connection_string) -> str:
        try:
            u = urlparse(connection_string)
            hostname = u.hostname
            if hostname is not None:
                for suffix in cls._suffix_list:
                    if hostname.endswith(suffix):
                        return suffix
        except:
            pass

        return None

    @classmethod
    def get_cloud_info(cls, connection_string: str) -> CloudInfo:
        """ Get the details of a cloud according to the DNS suffix of the provided connection string """
        cls._init_once()

        cloud = cls._extract_dns_suffix(connection_string)
        if cloud is None:
            return None

        return cls._cloud_info[cloud]
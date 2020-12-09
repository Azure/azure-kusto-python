import os

AUTH_ENV_VAR_NAME = "AadAuthorityUri"
KUSTO_CLIENT_APP_ID = "db662dc1-0cfe-4e1c-a843-19a68e65be58"
PUBLIC_LOGIN_URL = "https://login.microsoftonline.com"


class CloudInfo:
    """ This class holds the data for a specific cloud instance. """

    def __init__(self, auth_endpoint: str, kusto_client_app_id: str, redirect_uri: str):
        self.aad_authority_uri = auth_endpoint
        self.kusto_client_app_id = kusto_client_app_id
        self.login_redirect_uri = redirect_uri  # will be used for interactive login


class CloudSettings:
    """ This class holds data for all cloud instances, and returns the specific data instance by parsing the dns suffix from a URL """

    _cloud_info = None

    @classmethod
    def _init_once(cls):
        if cls._cloud_info is not None:
            return

        # todo: replace this with a call to the auth metadata endpoint
        aad_authority_uri = os.environ.get(AUTH_ENV_VAR_NAME, PUBLIC_LOGIN_URL)
        cls._cloud_info = CloudInfo(aad_authority_uri, KUSTO_CLIENT_APP_ID, None)

    @classmethod
    def get_cloud_info(cls) -> CloudInfo:
        """ Get the details of a cloud according to the DNS suffix of the provided connection string """
        cls._init_once()
        return cls._cloud_info

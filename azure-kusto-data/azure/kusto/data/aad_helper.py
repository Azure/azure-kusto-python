"""A module to acquire tokens from AAD."""

from datetime import timedelta, datetime
import webbrowser
import dateutil.parser
from adal import AuthenticationContext


class _AadHelper(object):
    def __init__(
        self,
        kusto_cluster,
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        authority=None,
    ):
        self.adal_context = AuthenticationContext(
            "https://login.windows.net/{0}".format(authority or "microsoft.com")
        )
        self.kusto_cluster = kusto_cluster
        self.client_id = client_id or "db662dc1-0cfe-4e1c-a843-19a68e65be58"
        self.client_secret = client_secret
        self.username = username
        self.password = password

    def acquire_token(self):
        """A method to acquire tokens from AAD."""
        token = self.adal_context.acquire_token(
            self.kusto_cluster, self.username, self.client_id
        )
        if token is not None:
            expiration_date = dateutil.parser.parse(token["expiresOn"])
            if expiration_date < datetime.now() + timedelta(minutes=5):
                token = self.adal_context.acquire_token_with_refresh_token(token["refreshToken"], self.client_id, self.kusto_cluster)
            return "{0} {1}".format(token["tokenType"], token["accessToken"])
        
        
        if self.client_secret is not None and self.client_id is not None:
            token = self.adal_context.acquire_token_with_client_credentials(
                self.kusto_cluster, self.client_id, self.client_secret
            )
        elif self.username is not None and self.password is not None:
            token = self.adal_context.acquire_token_with_username_password(
                self.kusto_cluster, self.username, self.password, self.client_id
            )
        else:
            code = self.adal_context.acquire_user_code(self.kusto_cluster, self.client_id)
            print(code["message"])
            webbrowser.open(code["verification_url"])
            token = self.adal_context.acquire_token_with_device_code(
                self.kusto_cluster, code, self.client_id
            )

        return "{0} {1}".format(token["tokenType"], token["accessToken"])

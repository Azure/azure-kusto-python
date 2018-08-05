"""A module to acquire tokens from AAD."""

from datetime import timedelta, datetime
import webbrowser
import dateutil.parser

from adal import AuthenticationContext
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters, AADConstants


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
            "https://{0}/{1}".format(
                AADConstants.WORLD_WIDE_AUTHORITY, authority or "microsoft.com"
            )
        )
        self.kusto_cluster = kusto_cluster
        self.client_id = client_id or "db662dc1-0cfe-4e1c-a843-19a68e65be58"
        self.client_secret = client_secret
        self.username = username
        self.password = password

    def acquire_token(self):
        """A method to acquire tokens from AAD."""
        token = self.adal_context.acquire_token(self.kusto_cluster, self.username, self.client_id)
        if token is not None:
            expiration_date = dateutil.parser.parse(token[TokenResponseFields.EXPIRES_ON])
            if expiration_date > datetime.now() + timedelta(minutes=5):
                return _get_header(token)
            elif TokenResponseFields.REFRESH_TOKEN in token:
                token = self.adal_context.acquire_token_with_refresh_token(
                    token[TokenResponseFields.REFRESH_TOKEN], self.client_id, self.kusto_cluster
                )
                if token is not None:
                    return _get_header(token)

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
            print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
            webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
            token = self.adal_context.acquire_token_with_device_code(
                self.kusto_cluster, code, self.client_id
            )
        return _get_header(token)


@staticmethod
def _get_header(token):
    return "{0} {1}".format(
        token[TokenResponseFields.TOKEN_TYPE], token[TokenResponseFields.ACCESS_TOKEN]
    )

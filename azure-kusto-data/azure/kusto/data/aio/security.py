import json
import webbrowser

from adal import AdalError, AuthenticationContext
from adal.constants import TokenResponseFields, OAuth2DeviceCodeResponseParameters
from msrestazure.azure_active_directory import MSIAuthentication

from .._decorators import aio_documented_by
from ..exceptions import KustoClientError, KustoAioSyntaxError
from ..security import _AadHelperBase, _AadHelper as _AadHelperSync, AuthenticationMethod

try:
    from asgiref.sync import sync_to_async
    from aiofile import AIOFile
except ImportError:
    raise KustoAioSyntaxError()


class _AadHelper(_AadHelperBase):
    @aio_documented_by(_AadHelperSync._acquire_authorization_header)
    async def _acquire_authorization_header(self) -> str:
        if self.authentication_method is AuthenticationMethod.aad_token:
            return self._get_header("Bearer", self.token)

        # Obtain token from MSI endpoint
        if self.authentication_method == AuthenticationMethod.aad_msi:
            token = await self.get_token_from_msi()
            return self._get_header_from_dict(token)

        refresh_token = None

        if self.authentication_method == AuthenticationMethod.az_cli_profile:
            stored_token = await self._get_azure_cli_auth_token()
            self._set_from_stored_token(stored_token)
            refresh_token = stored_token[TokenResponseFields.REFRESH_TOKEN]

        if self.auth_context is None:
            self.auth_context = AuthenticationContext(self.authority_uri)

        if refresh_token is not None:
            token = await sync_to_async(self.auth_context.acquire_token_with_refresh_token)(refresh_token, self.client_id, self.kusto_uri)
        else:
            token = await sync_to_async(self.auth_context.acquire_token)(self.kusto_uri, self.username, self.client_id)

        if token is not None:
            expiration_date = self._get_expiration_data_from_token(token)
            if not self._is_expired(expiration_date):
                return self._get_header_from_dict(token)
            if TokenResponseFields.REFRESH_TOKEN in token:
                token = await sync_to_async(self.auth_context.acquire_token_with_refresh_token)(
                    token[TokenResponseFields.REFRESH_TOKEN], self.client_id, self.kusto_uri
                )
                if token is not None:
                    return self._get_header_from_dict(token)

        # obtain token from AAD
        if self.authentication_method is AuthenticationMethod.aad_username_password:
            token = await sync_to_async(self.auth_context.acquire_token_with_username_password)(self.kusto_uri, self.username, self.password, self.client_id)
        elif self.authentication_method is AuthenticationMethod.aad_application_key:
            token = await sync_to_async(self.auth_context.acquire_token_with_client_credentials)(self.kusto_uri, self.client_id, self.client_secret)
        elif self.authentication_method is AuthenticationMethod.aad_device_login:
            code = await sync_to_async(self.auth_context.acquire_user_code)(self.kusto_uri, self.client_id)
            print(code[OAuth2DeviceCodeResponseParameters.MESSAGE])
            webbrowser.open(code[OAuth2DeviceCodeResponseParameters.VERIFICATION_URL])
            token = await sync_to_async(self.auth_context.acquire_token_with_device_code)(self.kusto_uri, code, self.client_id)
        elif self.authentication_method is AuthenticationMethod.aad_application_certificate:
            token = await sync_to_async(self.auth_context.acquire_token_with_client_certificate)(
                self.kusto_uri, self.client_id, self.certificate, self.thumbprint
            )
        else:
            raise self._authentication_method_missing_exception()

        return self._get_header_from_dict(token)

    @aio_documented_by(_AadHelperSync.acquire_authorization_header)
    async def acquire_authorization_header(self) -> str:
        try:
            return await self._acquire_authorization_header()
        except (AdalError, KustoClientError) as error:
            raise self._build_kusto_authentication_error(error)

    @aio_documented_by(_AadHelperSync._get_azure_cli_auth_token)
    async def _get_azure_cli_auth_token(self) -> dict:
        try:
            profile = self._load_azure_cli_profile()
            raw_token = await sync_to_async(profile.get_raw_token)()
            token_data = raw_token[0][2]

            return token_data

        except ModuleNotFoundError:
            try:
                token_path = self._get_env_azure_token_path()
                async with AIOFile(token_path) as af:
                    raw_data = await af.read()
                    data = json.loads(raw_data)

                # TODO: not sure I should take the first
                return data[0]
            except Exception:
                pass

    async def get_token_from_msi(self) -> dict:
        try:
            credentials = await sync_to_async(MSIAuthentication)(**self.msi_params)
        except Exception as e:
            raise KustoClientError.msi_token_exception(self.msi_params, e)

        return credentials.token

import uuid
import requests
from enum import Enum, unique

from six import text_type

from .version import VERSION

__version__ = VERSION


class KustoConnectionStringBuilder(object):
    @unique
    class _ValidKeywords(Enum):
        data_source = "Data Source"
        aad_user_id = "AAD User ID"
        password = "Password"
        application_client_id = "Application Client Id"
        application_key = "Application Key"
        authority_id = "Authority Id"

    _Keywords = {
        "Data Source": _ValidKeywords.data_source,
        "Addr": _ValidKeywords.data_source,
        "Address": _ValidKeywords.data_source,
        "Network Address": _ValidKeywords.data_source,
        "Server": _ValidKeywords.data_source,
        "AAD User ID": _ValidKeywords.aad_user_id,
        "Password": _ValidKeywords.password,
        "Pwd": _ValidKeywords.password,
        "Application Client Id": _ValidKeywords.application_client_id,
        "AppClientId": _ValidKeywords.application_client_id,
        "Application Key": _ValidKeywords.application_key,
        "AppKey": _ValidKeywords.application_key,
        "Authority Id": _ValidKeywords.authority_id,
        "AuthorityId": _ValidKeywords.authority_id,
        "Authority": _ValidKeywords.authority_id,
        "TenantId": _ValidKeywords.authority_id,
        "Tenant": _ValidKeywords.authority_id,
        "tid": _ValidKeywords.authority_id,
    }

    def __init__(self, connection_string):
        self._internal_dict = {}
        if connection_string is not None and "=" not in connection_string.partition(";")[0]:
            connection_string = "Data Source=" + connection_string
        for kvp_string in connection_string.split(";"):
            kvp = kvp_string.split("=")
            self[kvp[0]] = kvp[1]

    def __setitem__(self, key, value):
        try:
            keyword = self._Keywords[key]
        except ValueError:
            raise ValueError("%s is not supported as an item in KustoConnectionStringBuilder" % key)

        self._internal_dict[keyword] = value

    @classmethod
    def with_aad_user_password_authentication(cls, connection_string, user_id, password):
        _ensure_value_is_valid(connection_string)
        _ensure_value_is_valid(user_id)
        _ensure_value_is_valid(password)
        kcsb = cls(connection_string)
        kcsb[kcsb._ValidKeywords.aad_user_id] = user_id
        kcsb[kcsb._ValidKeywords.password] = password
        return kcsb

    @classmethod
    def with_aad_application_key_authentication(
        cls, connection_string, application_client_id, application_key
    ):
        _ensure_value_is_valid(connection_string)
        _ensure_value_is_valid(application_client_id)
        _ensure_value_is_valid(application_key)
        kcsb = cls(connection_string)
        kcsb[kcsb._ValidKeywords.application_client_id] = application_client_id
        kcsb[kcsb._ValidKeywords.application_key] = application_key
        return kcsb

    @classmethod
    def with_aad_device_authentication(cls, connection_string):
        _ensure_value_is_valid(connection_string)
        return cls(connection_string)

    @property
    def data_source(self):
        if self._ValidKeywords.data_source in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.data_source]

    @property
    def aad_user_id(self):
        if self._ValidKeywords.aad_user_id in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.aad_user_id]

    @property
    def password(self):
        if self._ValidKeywords.password in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.password]

    @property
    def application_client_id(self):
        if self._ValidKeywords.application_client_id in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.application_client_id]

    @property
    def application_key(self):
        if self._ValidKeywords.application_key in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.application_key]

    @property
    def authority_id(self):
        if self._ValidKeywords.authority_id in self._internal_dict:
            return self._internal_dict[self._ValidKeywords.authority_id]

    @authority_id.setter
    def authority_id(self, value):
        self[self._ValidKeywords.authority_id] = value


def _ensure_value_is_valid(value):
    if not value or not value.strip():
        raise ValueError

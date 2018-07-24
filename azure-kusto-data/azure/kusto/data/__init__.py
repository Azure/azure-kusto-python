from enum import Enum
from six import text_type
from .version import VERSION
__version__ = VERSION

class KustoConnectionStringBuilder(dict):
    class _Keywords(Enum):
        data_source = "Data Source"
        aad_user_id = "AAD User ID"
        password = "Password"
        application_client_id = "Application Client Id"
        application_key = "Application Key"
        authority_id = "Authority Id"

    def __init__(self, connection_string, *args, **kwargs):
        self._constructing = True
        if connection_string is not None and '=' not in connection_string.split(';')[0]:
            connection_string = 'Data Source=' + connection_string
        for kvp_string in connection_string.split(';'):
            kvp = kvp_string.split('=')
            self[kvp[0]] = kvp[1]
        super(KustoConnectionStringBuilder, self).__init__(*args, **kwargs)
        self._is_valid()
        self._constructing = False

    def __setitem__(self, key, value):
        try:
            keyword = KustoConnectionStringBuilder._Keywords(key)
        except ValueError:
            raise ValueError("%s is not supported as an item in KustoConnectionStringBuilder" % key)
        super(KustoConnectionStringBuilder, self).__setitem__(keyword, value)
        if not self._constructing:
            self._is_valid()

    def update(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise TypeError("update expected at most 1 arguments, "
                                "got %d" % len(args))
            other = dict(args[0])
            for key in other:
                self[key] = other[key]
        if kwargs:
            for key in kwargs:
                self[key] = kwargs[key]
        self._is_valid()

    def setdefault(self, key, value=None):
        if key not in self:
            self[key] = value
        self._is_valid()
        return self[key]

    @classmethod
    def with_aad_user_password_authentication(cls, connection_string, user_id, password):
        _ensure_value_is_valid(user_id)
        _ensure_value_is_valid(password)
        return cls(connection_string,\
            {KustoConnectionStringBuilder._Keywords.aad_user_id: user_id,\
            KustoConnectionStringBuilder._Keywords.password: password})

    @classmethod
    def with_aad_application_key_authentication(cls, connection_string, application_client_id, application_key):
        _ensure_value_is_valid(application_client_id)
        _ensure_value_is_valid(application_key)
        return cls(connection_string,\
            {KustoConnectionStringBuilder._Keywords.application_client_id: application_client_id,\
            KustoConnectionStringBuilder._Keywords.application_key: application_key})

    @classmethod
    def with_aad_device_authentication(cls, connection_string):
        return cls(connection_string)
    
    @property
    def data_source(self):
        if KustoConnectionStringBuilder._Keywords.data_source in self:
            return self[KustoConnectionStringBuilder._Keywords.data_source]
        return None

    @property
    def aad_user_id(self):
        if KustoConnectionStringBuilder._Keywords.aad_user_id in self:
            return self[KustoConnectionStringBuilder._Keywords.aad_user_id]
        return None

    @property
    def password(self):
        if KustoConnectionStringBuilder._Keywords.password in self:
            return self[KustoConnectionStringBuilder._Keywords.password]
        return None

    @property
    def application_client_id(self):
        if KustoConnectionStringBuilder._Keywords.application_client_id in self:
            return self[KustoConnectionStringBuilder._Keywords.application_client_id]
        return None

    @property
    def application_key(self):
        if KustoConnectionStringBuilder._Keywords.application_key in self:
            return self[KustoConnectionStringBuilder._Keywords.application_key]
        return None

    @property
    def authority_id(self):
        if KustoConnectionStringBuilder._Keywords.authority_id in self:
            return self[KustoConnectionStringBuilder._Keywords.authority_id]
        return None

    @authority_id.setter
    def authority_id(self, value):
        self[KustoConnectionStringBuilder._Keywords.authority_id] = value

    def _is_valid(self):
        _ensure_value_is_valid(self[KustoConnectionStringBuilder._Keywords.data_source])
        error_message_template = "specified '{0}' authentication method has incorrect properties set"
        if KustoConnectionStringBuilder._Keywords.aad_user_id in self:
            error_message = error_message_template.format(KustoConnectionStringBuilder._Keywords.aad_user_id.value)
            _ensure_value_is_valid(self[KustoConnectionStringBuilder._Keywords.password])
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.application_client_id, error_message)
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.application_key, error_message)
        elif KustoConnectionStringBuilder._Keywords.application_client_id in self:
            error_message = error_message_template.format(KustoConnectionStringBuilder._Keywords.application_client_id.value)
            _ensure_value_is_valid(self[KustoConnectionStringBuilder._Keywords.application_key])
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.aad_user_id, error_message)
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.password, error_message)
        else:
            error_message = error_message_template.format('Device login')
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.application_client_id, error_message)
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.application_key, error_message)
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.aad_user_id, error_message)
            self._ensure_value_does_not_exists(KustoConnectionStringBuilder._Keywords.password, error_message)

    def _ensure_value_does_not_exists(self, value, error_message):
        if value in self:
            raise AssertionError(error_message)

def _ensure_value_is_valid(value):
    if not isinstance(value, text_type):
        raise TypeError
    if not value or not value.strip():
        raise ValueError

class KustoClientFactory(object):
    @staticmethod
    def create_csl_provider(kusto_connection_string_builder):
        from ._kusto_client import _KustoClient
        return _KustoClient(kusto_connection_string_builder)

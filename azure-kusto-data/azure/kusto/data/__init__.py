from enum import Enum
from six import text_type
from .security import AuthenticationMethod
from .version import VERSION
__version__ = VERSION

class KustoConnectionStringBuilder(dict):
    class _Keywords(Enum):
        data_source = "data_source"
        user_id = "user_id"
        password = "password"
        application_client_id = "application_client_id"
        application_key = "application_key"
        authority_id = "authority_id"

    def __init__(self, connection_string, authentication_method, *args, **kwargs):
        self.connection_string = connection_string
        self._authentication_method = authentication_method
        super(KustoConnectionStringBuilder, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        try:
            keyword = KustoConnectionStringBuilder._Keywords(key)
        except ValueError:
            raise ValueError("%s is not supported as an item in KustoConnectionStringBuilder" % key)
        super(KustoConnectionStringBuilder, self).__setitem__(keyword, value)

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

    def setdefault(self, key, value=None):
        if key not in self:
            self[key] = value
        return self[key]

    @classmethod
    def with_aad_user_password_authentication(cls, connection_string, user_id, password):
        _ensure_value_is_valid(user_id)
        _ensure_value_is_valid(password)
        return cls(connection_string, AuthenticationMethod.aad_username_password,\
            {KustoConnectionStringBuilder._Keywords.user_id: user_id,\
            KustoConnectionStringBuilder._Keywords.password: password})

    @classmethod
    def with_aad_application_key_authentication(cls, connection_string, application_client_id, application_key):
        _ensure_value_is_valid(application_client_id)
        _ensure_value_is_valid(application_key)
        return cls(connection_string, AuthenticationMethod.aad_application_key,\
            {KustoConnectionStringBuilder._Keywords.application_client_id: application_client_id,\
            KustoConnectionStringBuilder._Keywords.application_key: application_key})

    @classmethod
    def with_aad_device_authentication(cls, connection_string):
        return cls(connection_string, AuthenticationMethod.aad_device_login)
    
    @property
    def authentication_method(self):
        return self._authentication_method

    @property
    def authority_id(self):
        return self[KustoConnectionStringBuilder._Keywords.authority_id]

    @authority_id.setter
    def authority_id(self, value):
        self[KustoConnectionStringBuilder._Keywords.authority_id] = value

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

import datetime
import re

from math import log2

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def parse_type(string: str) -> type:
    if string == 'str':
        return str
    if string == 'bool':
        return bool
    if string == 'int':
        return int
    if string == 'float':
        return float
    if string == 'list':
        return list
    if string == 'set':
        return set
    if string == 'dict':
        return dict
    if string == 'object':
        return object
    if string == 'tuple':
        return tuple
    if string == 'datetime':
        return datetime.datetime
    if string == 'timedelta':
        return datetime.timedelta


def to_snake_case(string: str) -> str:
    s1 = first_cap_re.sub(r'\1_\2', string)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def to_camel_case(string: str) -> str:
    first, *rest = string.split('_')
    return first + ''.join(word.capitalize() for word in rest)


def get_azure_cli_auth_token(resource):
    from azure.cli.core._profile import Profile
    from azure.cli.core._session import ACCOUNT
    from azure.cli.core._environment import get_config_dir
    from adal import AuthenticationContext
    import os

    azure_folder = get_config_dir()
    ACCOUNT.load(os.path.join(azure_folder, 'azureProfile.json'))
    profile = Profile(storage=ACCOUNT)
    token_data = profile.get_raw_token()[0][2]
    return AuthenticationContext(f"https://login.microsoftonline.com/common").acquire_token_with_refresh_token(token_data['refreshToken'], token_data['_clientId'], f'https://{resource}.kusto.windows.net')['accessToken']


_suffixes = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']


def human_readable(size):
    # determine binary order in steps of size 10
    # (coerce to int, // still returns a float)
    order = int(log2(size) / 10) if size else 0
    # format file size
    # (.4g results in rounded numbers for exact matches and max 3 decimals,
    # should never resort to exponent values)
    return '{:.4g} {}'.format(size / (1 << (order * 10)), _suffixes[order])

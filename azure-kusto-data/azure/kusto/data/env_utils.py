import os


def get_env(*args, optional=False, default=None):
    """Return the first environment variable that is defined."""
    for arg in args:
        if arg in os.environ:
            return os.environ[arg]
    if optional or default:
        return default
    raise ValueError("No environment variables found: {}".format(args))


def set_env(key, value):
    """Set the environment variable."""
    os.environ[key] = value


def get_app_id(optional=False):
    """Return the app id."""
    result = get_env("APP_ID", "AZURE_CLIENT_ID", optional=optional)
    os.environ["AZURE_CLIENT_ID"] = result
    return result


def get_auth_id(optional=False):
    """Return the auth id."""
    result = get_env("AUTH_ID", "APP_AUTH_ID", "AZURE_TENANT_ID", optional=optional)
    os.environ["AZURE_TENANT_ID"] = result
    return result


def get_app_key(optional=False):
    """Return the app key."""
    result = get_env("APP_KEY", "AZURE_CLIENT_SECRET", optional=optional)
    os.environ["AZURE_CLIENT_SECRET"] = result
    return result

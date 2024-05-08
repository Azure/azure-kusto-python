from typing import TypedDict


class ProxyDict(TypedDict, total=False):
    http: str
    https: str

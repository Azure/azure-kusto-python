"""A class to parse uris recieved from the DM."""

import re

_URI_FORMAT = re.compile("https://(\\w+).(queue|blob|table).core.windows.net/([\\w,-]+)\\?(.*)")


class _ConnectionString:
    def __init__(self, storage_account_name, object_type, object_name, sas):
        self.storage_account_name = storage_account_name
        self.object_type = object_type
        self.object_name = object_name
        self.sas = sas

    @classmethod
    def parse(cls, uri):
        """Parses uri into a _ConnectionString object"""
        match = _URI_FORMAT.search(uri)
        return cls(match.group(1), match.group(2), match.group(3), match.group(4))

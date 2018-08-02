"""A class to parse uris recieved from the DM."""

import re


class _ConnectionString:
    def __init__(self, storage_account_name, objectType, objectName, sas):
        self.storage_account_name = storage_account_name
        self.object_type = objectType
        self.object_name = objectName
        self.sas = sas

    @staticmethod
    def parse(uri):
        """ Parses uri into a _ConnectionString object """
        match = re.search(
            "https://(\\w+).(queue|blob|table).core.windows.net/([\\w,-]+)\\?(.*)", uri
        )
        return _ConnectionString(match.group(1), match.group(2), match.group(3), match.group(4))

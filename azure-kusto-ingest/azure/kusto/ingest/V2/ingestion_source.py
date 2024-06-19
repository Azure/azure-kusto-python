import uuid

from azure.kusto.data import DataFormat


class IngestionSource:
    def __init__(self, format: DataFormat):
        self.source_id = uuid.uuid4()
        self.format = format

"""Kusto Ingest Client"""

import base64
import os
import random
import tempfile
import time
import uuid
from typing import Union

from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.storage.common import CloudStorageAccount

from .descriptors import BlobDescriptor, FileDescriptor
from ._ingestion_blob_info import _IngestionBlobInfo
from .ingestion_properties import DataFormat, IngestionProperties
from ._resource_manager import _ResourceManager


class KustoIngestClient:
    """
    Kusto ingest client provides methods to allow ingestion into kusto (ADX).
    To learn more about the different types of ingestions and when to use each, visit:
    https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
    """

    def __init__(self, kcsb: Union[str, KustoConnectionStringBuilder]):
        """Kusto Ingest Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        """
        self._resource_manager = _ResourceManager(KustoClient(kcsb))

    def ingest_from_dataframe(self, df, ingestion_properties: IngestionProperties):
        """
        Enqueue an ingest command from local files.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param pandas.DataFrame df: input dataframe to ingest.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        from pandas import DataFrame

        if not isinstance(df, DataFrame):
            raise ValueError("Expected DataFrame instance, found {}".format(type(df)))

        file_name = "df_{id}_{timestamp}_{pid}.csv.gz".format(id=id(df), timestamp=int(time.time()), pid=os.getpid())
        temp_file_path = os.path.join(tempfile.gettempdir(), file_name)

        df.to_csv(temp_file_path, index=False, encoding="utf-8", header=False, compression="gzip")

        ingestion_properties.format = DataFormat.CSV

        self.ingest_from_file(temp_file_path, ingestion_properties)

        os.unlink(temp_file_path)

    def ingest_from_file(self, file_descriptor: Union[FileDescriptor, str], ingestion_properties: IngestionProperties):
        """
        Enqueue an ingest command from local files.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        containers = self._resource_manager.get_containers()

        if isinstance(file_descriptor, FileDescriptor):
            descriptor = file_descriptor
        else:
            descriptor = FileDescriptor(file_descriptor)

        should_compress = not (
            ingestion_properties.format in [DataFormat.AVRO, DataFormat.ORC, DataFormat.PARQUET]
            or descriptor.path.endswith(".gz")
            or descriptor.path.endswith(".zip")
        )

        with descriptor.open(should_compress) as stream:
            blob_name = "{db}__{table}__{guid}__{file}".format(
                db=ingestion_properties.database, table=ingestion_properties.table, guid=descriptor.source_id or uuid.uuid4(), file=descriptor.stream_name
            )

            container_details = random.choice(containers)
            storage_client = CloudStorageAccount(container_details.storage_account_name, sas_token=container_details.sas)
            blob_service = storage_client.create_block_blob_service()

            blob_service.create_blob_from_stream(container_name=container_details.object_name, blob_name=blob_name, stream=stream)
            url = blob_service.make_blob_url(container_details.object_name, blob_name, sas_token=container_details.sas)

            self.ingest_from_blob(BlobDescriptor(url, descriptor.size, descriptor.source_id), ingestion_properties=ingestion_properties)

    def ingest_from_blob(self, blob_descriptor: BlobDescriptor, ingestion_properties: IngestionProperties):
        """
        Enqueue an ingest command from azure blobs.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        queues = self._resource_manager.get_ingestion_queues()

        queue_details = random.choice(queues)
        storage_client = CloudStorageAccount(queue_details.storage_account_name, sas_token=queue_details.sas)
        queue_service = storage_client.create_queue_service()
        authorization_context = self._resource_manager.get_authorization_context()
        ingestion_blob_info = _IngestionBlobInfo(blob_descriptor, ingestion_properties=ingestion_properties, auth_context=authorization_context)
        ingestion_blob_info_json = ingestion_blob_info.to_json()
        encoded = base64.b64encode(ingestion_blob_info_json.encode("utf-8")).decode("utf-8")
        queue_service.put_message(queue_name=queue_details.object_name, content=encoded)

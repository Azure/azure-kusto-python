"""Kusto Ingest Client"""

import base64
import random
import uuid
import os
import time
import tempfile
import io

from azure.storage.common import CloudStorageAccount

from azure.kusto.data.request import KustoClient
from ._descriptors import BlobDescriptor, FileDescriptor
from ._ingestion_blob_info import _IngestionBlobInfo
from ._resource_manager import _ResourceManager
from .exceptions import MissingMappingReference
from ._ingestion_properties import DataFormat

_1KB = 1024
_1MB = _1KB * _1KB


class KustoIngestClient(object):
    """Kusto ingest client for Python.
    KustoIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.

    Tests are run using pytest.
    """

    def __init__(self, kcsb, use_streaming_ingest=False):
        """Kusto Ingest Client constructor.
        :param KustoConnectionStringBuilder kcsb: The connection string to initialize KustoClient.
        :param boolean use_streaming_ingest: indicates whether to use queued ingest or streaming ingest if possible.
        """
        self._kusto_client = KustoClient(kcsb)
        self._resource_manager = _ResourceManager(self._kusto_client)
        self._use_streaming_ingest = use_streaming_ingest
        self._streaming_ingestion_size_limit = 4 * _1MB

    def ingest_from_dataframe(self, df, ingestion_properties):
        """Ingest from pandas DataFrame.
        :param pandas.DataFrame df: input dataframe to ingest.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        
        """

        from pandas import DataFrame

        if not isinstance(df, DataFrame):
            raise ValueError("Expected DataFrame instance, found {}".format(type(df)))

        file_name = "df_{timestamp}_{pid}.csv.gz".format(timestamp=int(time.time()), pid=os.getpid())
        temp_file_path = os.path.join(tempfile.gettempdir(), file_name)

        df.to_csv(temp_file_path, index=False, encoding="utf-8", header=False, compression="gzip")

        fd = FileDescriptor(temp_file_path)

        ingestion_properties.format = DataFormat.csv
        self.ingest_from_file(fd, ingestion_properties)

        fd.delete_files()
        os.unlink(temp_file_path)

    def ingest_from_file(self, file_descriptor, ingestion_properties):
        """Ingest from local files.
        :param file_descriptor: a FileDescriptor to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if isinstance(file_descriptor, FileDescriptor):
            descriptor = file_descriptor
        else:
            descriptor = FileDescriptor(file_descriptor)

        if self._use_streaming_ingest and descriptor.size < self._streaming_ingestion_size_limit:
            if (
                ingestion_properties.format == DataFormat.json
                or ingestion_properties.format == DataFormat.singlejson
                or ingestion_properties.format == DataFormat.avro
            ) and ingestion_properties.mapping_reference is None:
                raise MissingMappingReference

            self._kusto_client.execute_streaming_ingest(
                ingestion_properties.database,
                ingestion_properties.table,
                descriptor.zipped_stream,
                ingestion_properties.format.name,
                mapping_name=ingestion_properties.mapping_reference,
                content_length=str(descriptor.size),
                content_encoding="gzip",
            )
        else:
            blob_name = "{db}__{table}__{guid}__{file}".format(
                db=ingestion_properties.database,
                table=ingestion_properties.table,
                guid=uuid.uuid4(),
                file=descriptor.stream_name,
            )

            containers = self._resource_manager.get_containers()
            container_details = random.choice(containers)
            storage_client = CloudStorageAccount(
                container_details.storage_account_name, sas_token=container_details.sas
            )
            blob_service = storage_client.create_block_blob_service()

            blob_service.create_blob_from_stream(
                container_name=container_details.object_name, blob_name=blob_name, stream=descriptor.zipped_stream
            )
            url = blob_service.make_blob_url(container_details.object_name, blob_name, sas_token=container_details.sas)

            self.ingest_from_blob(
                BlobDescriptor(url, descriptor.size, descriptor.source_id), ingestion_properties=ingestion_properties
            )

        descriptor.delete_files()

    def ingest_from_blob(self, blob_descriptor, ingestion_properties):
        """Enqueuing an ingest command from azure blobs.
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to
               be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        queues = self._resource_manager.get_ingestion_queues()

        queue_details = random.choice(queues)
        storage_client = CloudStorageAccount(queue_details.storage_account_name, sas_token=queue_details.sas)
        queue_service = storage_client.create_queue_service()
        authorization_context = self._resource_manager.get_authorization_context()
        ingestion_blob_info = _IngestionBlobInfo(
            blob_descriptor, ingestion_properties=ingestion_properties, auth_context=authorization_context
        )
        ingestion_blob_info_json = ingestion_blob_info.to_json()
        encoded = base64.b64encode(ingestion_blob_info_json.encode("utf-8")).decode("utf-8")
        queue_service.put_message(queue_name=queue_details.object_name, content=encoded)

    def ingest_from_stream(self, stream, ingestion_properties):
        """Ingest from in-memroy streams.
        :param BytesIO or StringIO stream: in-memory stream.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """

        if isinstance(stream, io.BytesIO) or isinstance(stream, io.StringIO):
            stream.seek(0, io.SEEK_END)
            stream_size = stream.tell()
            stream.seek(0, io.SEEK_SET)
        else:
            raise ValueError("Expected BytesIO or StringIO instance, found {}".format(type(stream)))

        if self._use_streaming_ingest and stream_size < self._streaming_ingestion_size_limit:
            if (
                ingestion_properties.format == DataFormat.json
                or ingestion_properties.format == DataFormat.singlejson
                or ingestion_properties.format == DataFormat.avro
            ) and ingestion_properties.mapping_reference is None:
                raise MissingMappingReference

            self._kusto_client.execute_streaming_ingest(
                ingestion_properties.database,
                ingestion_properties.table,
                stream,
                ingestion_properties.format.name,
                mapping_name=ingestion_properties.mapping_reference,
                content_length=stream_size,
            )
        else:
            blob_name = "{db}__{table}__{guid}__{file}".format(
                db=ingestion_properties.database, table=ingestion_properties.table, guid=uuid.uuid4(), file="stream"
            )

            containers = self._resource_manager.get_containers()
            container_details = random.choice(containers)
            storage_client = CloudStorageAccount(
                container_details.storage_account_name, sas_token=container_details.sas
            )
            blob_service = storage_client.create_block_blob_service()

            # As of azure.storage.blob bug, create_blob_from_stream method fails when provided with text streams
            if isinstance(stream, io.BytesIO):
                blob_service.create_blob_from_stream(
                    container_name=container_details.object_name, blob_name=blob_name, stream=stream
                )
            else:
                blob_service.create_blob_from_text(
                    container_name=container_details.object_name, blob_name=blob_name, text=stream.getvalue()
                )

            url = blob_service.make_blob_url(container_details.object_name, blob_name, sas_token=container_details.sas)

            self.ingest_from_blob(BlobDescriptor(url, stream_size), ingestion_properties)

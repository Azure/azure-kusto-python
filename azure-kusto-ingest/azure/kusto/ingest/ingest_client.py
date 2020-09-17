# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import random
import tempfile
import time
import uuid
from typing import Union
from urllib.parse import urlparse

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient, TextBase64EncodePolicy

from ._ingestion_blob_info import _IngestionBlobInfo
from ._resource_manager import _ResourceManager
from .descriptors import BlobDescriptor, FileDescriptor
from .exceptions import KustoInvalidEndpointError
from .ingestion_properties import DataFormat, IngestionProperties


class KustoIngestClient:
    """
    Kusto ingest client provides methods to allow queued ingestion into kusto (ADX).
    To learn more about the different types of ingestions and when to use each, visit:
    https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
    """

    _INGEST_PREFIX = "ingest-"
    _EXPECTED_SERVICE_TYPE = "DataManagement"

    def __init__(self, kcsb: Union[str, KustoConnectionStringBuilder]):
        """Kusto Ingest Client constructor.
        :param kcsb: The connection string to initialize KustoClient.
        """
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)
        self._connection_datasource = kcsb.data_source
        self._resource_manager = _ResourceManager(KustoClient(kcsb))
        self._endpoint_service_type = None
        self._suggested_endpoint_uri = None

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
        try:
            containers = self._resource_manager.get_containers()
        except KustoServiceError as ex:
            self._validate_endpoint_service_type()
            raise ex

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

            random_container = random.choice(containers)

            blob_service = BlobServiceClient(random_container.account_uri)
            blob_client = blob_service.get_blob_client(container=random_container.object_name, blob=blob_name)
            blob_client.upload_blob(data=stream)

            self.ingest_from_blob(BlobDescriptor(blob_client.url, descriptor.size, descriptor.source_id), ingestion_properties=ingestion_properties)

    def ingest_from_blob(self, blob_descriptor: BlobDescriptor, ingestion_properties: IngestionProperties):
        """
        Enqueue an ingest command from azure blobs.
        To learn more about ingestion methods go to:
        https://docs.microsoft.com/en-us/azure/data-explorer/ingest-data-overview#ingestion-methods
        :param azure.kusto.ingest.BlobDescriptor blob_descriptor: An object that contains a description of the blob to be ingested.
        :param azure.kusto.ingest.IngestionProperties ingestion_properties: Ingestion properties.
        """
        try:
            queues = self._resource_manager.get_ingestion_queues()
        except KustoServiceError as ex:
            self._validate_endpoint_service_type()
            raise ex

        random_queue = random.choice(queues)
        queue_service = QueueServiceClient(random_queue.account_uri)
        authorization_context = self._resource_manager.get_authorization_context()
        ingestion_blob_info = _IngestionBlobInfo(blob_descriptor, ingestion_properties=ingestion_properties, auth_context=authorization_context)
        ingestion_blob_info_json = ingestion_blob_info.to_json()
        # TODO: perhaps this needs to be more visible
        content = ingestion_blob_info_json
        queue_client = queue_service.get_queue_client(queue=random_queue.object_name, message_encode_policy=TextBase64EncodePolicy())
        queue_client.send_message(content=content)

    def _validate_endpoint_service_type(self):
        if not self._hostname_starts_with_ingest(self._connection_datasource):
            if not self._endpoint_service_type:
                self._endpoint_service_type = self._retrieve_service_type()

            if self._EXPECTED_SERVICE_TYPE != self._endpoint_service_type:
                if not self._suggested_endpoint_uri:
                    self._suggested_endpoint_uri = self._generate_endpoint_suggestion(self._connection_datasource)
                    if not self._suggested_endpoint_uri:
                        raise KustoInvalidEndpointError(self._EXPECTED_SERVICE_TYPE, self._endpoint_service_type)
                raise KustoInvalidEndpointError(self._EXPECTED_SERVICE_TYPE, self._endpoint_service_type, self._suggested_endpoint_uri)

    def _retrieve_service_type(self):
        return self._resource_manager.retrieve_service_type()

    def _generate_endpoint_suggestion(self, datasource):
        """The default is not passing a suggestion to the exception String"""
        endpoint_uri_to_suggest_str = None
        if datasource.strip():
            try:
                endpoint_uri_to_suggest = urlparse(datasource)  # Standardize URL formatting
                endpoint_uri_to_suggest = urlparse(endpoint_uri_to_suggest.scheme + "://" + self._INGEST_PREFIX + endpoint_uri_to_suggest.hostname)
                endpoint_uri_to_suggest_str = endpoint_uri_to_suggest.geturl()
            except Exception:
                # TODO: Add logging infrastructure so we can tell the user as a warning:
                #   "Couldn't generate suggested endpoint due to problem parsing datasource, with exception: {ex}. The correct endpoint is usually the Engine endpoint with '{self._INGEST_PREFIX}' prepended to the hostname."
                pass
        return endpoint_uri_to_suggest_str

    def _hostname_starts_with_ingest(self, datasource):
        datasource_uri = urlparse(datasource)
        hostname = datasource_uri.hostname
        return hostname and hostname.startswith(self._INGEST_PREFIX)

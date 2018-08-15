"""Kusto ingest client for Python."""

import base64
import random
import uuid
from six import text_type

from azure.storage.common import CloudStorageAccount
from azure.cosmosdb.table.tableservice import TableService

from azure.kusto.data.request import KustoClient
from azure.kusto.data.exceptions import KustoClientError
from ._descriptors import BlobDescriptor, FileDescriptor
from ._ingestion_blob_info import _IngestionBlobInfo
from ._resource_manager import _ResourceManager
from ._ingestion_properties import ReportLevel, ReportMethod
from .ingestion_status import IngestionStatus, Status
from .ingestion_result import KustoIngestionResult, TableReportKustoIngestionResult


class KustoIngestClient:
    """
    Kusto ingest client for Python.

    KustoIngestClient works with both 2.x and 3.x flavors of Python.
    All primitive types are supported.
    KustoIngestClient takes care of ADAL authentication, and queueing ingest jobs.
    When using KustoIngestClient, you can choose between three options for authenticating:

    Option 1:
    You'll need to have your own AAD application and know your
    client credentials (client_id and client_secret).
    >>> kusto_cluster = 'https://ingest-help.kusto.windows.net'
    >>> kusto_ingest_client = KustoIngestClient(kusto_cluster,
                                   client_id='your_app_id',
                                   client_secret='your_app_secret')

    Option 2:
    You can use KustoClient's client id (set as a default in the constructor) 
    and authenticate using your username and password.
    >>> kusto_cluster = 'https://ingest-help.kusto.windows.net'
    >>> kusto_ingest_client = KustoIngestClient(kusto_cluster,
                                   username='your_username',
                                   password='your_password')

    Option 3:
    You can use KustoClient's client id (set as a default in the constructor) 
    and authenticate using your username and an AAD pop up.
    >>> kusto_cluster = 'https://ingest-help.kusto.windows.net'
    >>> kusto_ingest_client = KustoIngestClient(kusto_cluster)
    """

    def __init__(
        self,
        kusto_cluster,
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        authority=None,
    ):
        """
        Kusto Client constructor.
        Parameters
        ----------
        kusto_cluster : str
            Kusto cluster endpoint. Example: https://ingest-help.kusto.windows.net
        client_id : str
            The AAD application ID of the application making the request to Kusto
        client_secret : str
            The AAD application key of the application making the request to Kusto.
            if this is given, then username/password should not be.
        username : str
            The username of the user making the request to Kusto.
            if this is given, then password must follow and the client_secret should not be given.
        password : str
            The password matching the username of the user making the request to Kusto
        version : 'v1', optional
            REST API version, defaults to v1.
        authority : 'microsoft.com', optional
            In case your tenant is not microsoft please use this param.
        """
        kusto_client = KustoClient(
            kusto_cluster,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            authority=authority,
        )
        self._resource_manager = _ResourceManager(kusto_client)

    def ingest_from_multiple_files(self, files, delete_sources_on_success, ingestion_properties):
        """
        Enqueuing an ingest command from local files.

        Parameters
        ----------
        files : List of FileDescriptor or file paths.
            The list of files to be ingested.
        delete_sources_on_success : bool.
            After a successful ingest, whether to delete the origin files.
        ingestion_properties : kusto_ingest_client.ingestion_properties.IngestionProperties
            The ingestion properties.
        """
        blobs = list()
        file_descriptors = list()
        for file in files:
            if isinstance(file, FileDescriptor):
                descriptor = file
            else:
                descriptor = FileDescriptor(file, deleteSourcesOnSuccess=delete_sources_on_success)
            file_descriptors.append(descriptor)
            blob_name = (
                ingestion_properties.database
                + "__"
                + ingestion_properties.table
                + "__"
                + text_type(uuid.uuid4())
                + "__"
                + descriptor.stream_name
            )
            containers = self._resource_manager.get_containers()
            container_details = random.choice(containers)
            storage_client = CloudStorageAccount(
                container_details.storage_account_name, sas_token=container_details.sas
            )
            blob_service = storage_client.create_block_blob_service()
            blob_service.create_blob_from_stream(
                container_name=container_details.object_name,
                blob_name=blob_name,
                stream=descriptor.zipped_stream,
            )
            url = blob_service.make_blob_url(
                container_details.object_name, blob_name, sas_token=container_details.sas
            )
            blobs.append(BlobDescriptor(url, descriptor.size))

        ingestion_result = self.ingest_from_multiple_blobs(
            blobs, delete_sources_on_success, ingestion_properties
        )
        for descriptor in file_descriptors:
            descriptor.delete_files(True)

        return ingestion_result

    def ingest_from_multiple_blobs(self, blobs, delete_sources_on_success, ingestion_properties):
        """
        Enqueuing an ingest command from azure blobs.

        Parameters
        ----------
        blobs : List of BlobDescriptor.
            The list of blobs to be ingested.
            Please provide the raw blob size to each of the descriptors.
        delete_sources_on_success : bool.
            After a successful ingest, whether to delete the origin files.
        ingestion_properties : kusto_ingest_client.ingestion_properties.IngestionProperties
            The ingestion properties.
        """
        if not blobs:
            raise KustoClientError("No blobs were found to ingest")

        should_report_to_ingestions_status_table = (
            ingestion_properties.report_level != ReportLevel.DoNotReport
            and ingestion_properties.report_method != ReportMethod.Queue
        )
        if should_report_to_ingestions_status_table:
            ingestions_status_table = random.choice(
                self._resource_manager.get_ingestions_status_tables()
            )
            table_service = TableService(
                account_name=ingestions_status_table.storage_account_name,
                sas_token=ingestions_status_table.sas,
            )

        map_of_source_id_to_ingestion_status = {}
        map_of_source_id_to_ingestion_status_in_table = {}

        for blob in blobs:
            authorization_context = self._resource_manager.get_authorization_context()
            ingestion_blob_info = _IngestionBlobInfo(
                blob, ingestion_properties, delete_sources_on_success, authorization_context
            )
            ingestion_source_id = ingestion_blob_info.properties["Id"]
            ingestion_status = IngestionStatus.create_ingestion_status(
                ingestion_source_id, blob, ingestion_properties
            )

            if should_report_to_ingestions_status_table:
                ingestion_status_in_table = {
                    "TableConnectionString": ingestions_status_table.unparse(),
                    "PartitionKey": ingestion_source_id,
                    "RowKey": ingestion_source_id,
                }
                ingestion_blob_info.properties["IngestionStatusInTable"] = ingestion_status_in_table

                table_service.insert_entity(
                    table_name=ingestions_status_table.object_name, entity=ingestion_status
                )
                map_of_source_id_to_ingestion_status_in_table[
                    ingestion_source_id
                ] = ingestion_status_in_table
            else:
                ingestion_status.Status = Status.Queued.value
                map_of_source_id_to_ingestion_status[ingestion_source_id] = ingestion_status

            queues = self._resource_manager.get_ingestion_queues()
            queue_details = random.choice(queues)
            storage_client = CloudStorageAccount(
                queue_details.storage_account_name, sas_token=queue_details.sas
            )
            queue_service = storage_client.create_queue_service()
            authorization_context = self._resource_manager.get_authorization_context()
            ingestion_blob_info = _IngestionBlobInfo(
                blob, ingestion_properties, delete_sources_on_success, authorization_context
            )
            ingestion_blob_info_json = ingestion_blob_info.to_json()
            encoded = base64.b64encode(ingestion_blob_info_json.encode("utf-8")).decode("utf-8")
            queue_service.put_message(queue_name=queue_details.object_name, content=encoded)

            if should_report_to_ingestions_status_table:
                return TableReportKustoIngestionResult(
                    map_of_source_id_to_ingestion_status_in_table
                )
            else:
                return KustoIngestionResult(map_of_source_id_to_ingestion_status)

from typing import Optional, Dict, Union, List, IO, AnyStr

from azure.storage.blob import BlobServiceClient
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest._resource_manager import _ResourceManager, _ResourceUri
from azure.kusto.ingest.base_ingest_client import BaseIngestClient
from azure.kusto.ingest.descriptors import BlobDescriptor, FileDescriptor, StreamDescriptor
from azure.kusto.data.exceptions import KustoBlobError, KustoClientError
from azure.kusto.ingest.V2.blob_source import BlobSource
from azure.kusto.ingest.V2.local_source import LocalSource


class KustoStorageUploader:
    _MAX_RETRIES = 3

    def __init__(self, kcsb: Union[str, KustoConnectionStringBuilder], auto_correct_endpoint: bool = True):

        super().__init__()
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)

        if auto_correct_endpoint:
            kcsb["Data Source"] = BaseIngestClient.get_ingestion_endpoint(kcsb.data_source)

        self._connection_datasource = kcsb.data_source
        self._resource_manager = _ResourceManager(KustoClient(kcsb))
        self._endpoint_service_type = None
        self._suggested_endpoint_uri = None
        self._proxy_dict: Optional[Dict[str, str]] = None
        self.application_for_tracing = kcsb.client_details.application_for_tracing
        self.client_version_for_tracing = kcsb.client_details.version_for_tracing

    def upload_blob(
            self,
            containers: List[_ResourceUri],
            descriptor: Union[FileDescriptor, "StreamDescriptor"],
            database: str,
            table: str,
            stream: IO[AnyStr],
            proxy_dict: Optional[Dict[str, str]],
            timeout: int,
            max_retries: int,
    ) -> "BlobDescriptor":
        """
        Uploads and transforms FileDescriptor or StreamDescriptor into a BlobDescriptor instance
        :param List[_ResourceUri] containers: blob containers
        :param Union[FileDescriptor, "StreamDescriptor"] descriptor:
        :param string database: database to be ingested to
        :param string table: table to be ingested to
        :param IO[AnyStr] stream: stream to be ingested from
        :param Optional[Dict[str, str]] proxy_dict: proxy urls
        :param int timeout: Azure service call timeout in seconds
        :return new BlobDescriptor instance
        """
        blob_name = "{db}__{table}__{guid}__{file}".format(db=database, table=table, guid=descriptor.source_id, file=descriptor.stream_name)

        retries_left = min(max_retries, len(containers))
        for container in containers:
            succeeded = True
            try:
                blob_service = BlobServiceClient(container.account_uri, proxies=proxy_dict)
                blob_client = blob_service.get_blob_client(container=container.object_name, blob=blob_name)
                blob_client.upload_blob(data=stream, timeout=timeout)
                return BlobDescriptor(blob_client.url, descriptor.size, descriptor.source_id)
            except Exception as e:
                succeeded = False
                retries_left = retries_left - 1
                if retries_left == 0:
                    raise KustoBlobError(e)
            finally:
                self._resource_manager.report_resource_usage_result(container.storage_account_name, succeeded)

    def close(self) -> None:
        self._resource_manager.close()
        BaseIngestClient().close()

    def set_proxy(self, proxy_url: str):
        self._resource_manager.set_proxy(proxy_url)
        self._proxy_dict = {"http": proxy_url, "https": proxy_url}

    def upload_local_source(self, local: LocalSource):
        try:
            with local.Data() as local_stream:
                if local_stream is None or local_stream.length == 0:
                  ??  tracer
                    raise KustoClientError(local.source_id, f"KustoStorageUploader.upload_async: No data in file. Skipping uploading of file: '{local.name}'." )
            ??  blob_name =
            blob_name = "{db}__{table}__{guid}__{file}".format(db=database, table=table, guid=descriptor.source_id, file=descriptor.stream_name)
            blob_uri = upload_blob(?? )
            return BlobSource(blob_uri, local)
        except Exception as ex:
            if isinstance(ex, KustoClientError):
                ex.ingestion_source = local.name
                raise KustoClientError(local.source_id, f"KustoStorageUploader.upload_async: No data in file. Skipping uploading of file: '{local.name}'." )
            else:
                raise KustoClientError(local.source_id, local.name, f"Failed to upload file '{local.name}'.", ex)




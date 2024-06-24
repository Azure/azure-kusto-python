from typing import Optional, Dict, Union, IO, AnyStr

from azure.storage.blob import BlobServiceClient
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest._resource_manager import _ResourceManager
from azure.kusto.ingest.base_ingest_client import BaseIngestClient
from azure.kusto.ingest.descriptors import BlobDescriptor
from azure.kusto.data.exceptions import KustoBlobError, KustoUploadError
from azure.kusto.ingest.V2.blob_source import BlobSource
from azure.kusto.ingest.V2.local_source import LocalSource


class KustoStorageUploader:
    _MAX_RETRIES = 3
    _SERVICE_CLIENT_TIMEOUT_SECONDS = 10 * 60

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
        local_source: LocalSource,
        stream: IO[AnyStr],
    ) -> "BlobDescriptor":
        """
        Uploads and transforms FileDescriptor or StreamDescriptor into a BlobDescriptor instance
        :param LocalSource local_source:
        :param IO[AnyStr] stream: stream to be ingested from
        :return new BlobDescriptor instance
        """

        blob_name = local_source.name + local_source.compression_type
        containers = self._resource_manager.get_containers()
        retries_left = min(self._MAX_RETRIES, len(containers))
        for container in containers:
            succeeded = True
            try:
                blob_service = BlobServiceClient(container.account_uri, proxies=self._proxy_dict)
                blob_client = blob_service.get_blob_client(container=container.object_name, blob=blob_name)
                blob_client.upload_blob(data=stream, timeout=self._SERVICE_CLIENT_TIMEOUT_SECONDS)
                return BlobDescriptor(blob_client.url, local_source.size, local_source.source_id)
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
            with local.data() as local_stream:
                if local_stream is None or local_stream.length == 0:
                    raise KustoUploadError(local.name)

            blob_uri = self.upload_blob(local, local_stream)
            return BlobSource(blob_uri.__str__(), local)
        except Exception as ex:
            if isinstance(ex, KustoUploadError):
                raise KustoUploadError(local.name)
            else:
                raise KustoUploadError(local.name)

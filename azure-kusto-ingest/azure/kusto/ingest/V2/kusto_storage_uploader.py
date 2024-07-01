import uuid
from typing import Optional, Dict, Union, IO, AnyStr

from azure.storage.blob import BlobServiceClient
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.ingest._resource_manager import _ResourceManager
from azure.kusto.ingest.descriptors import BlobDescriptor
from azure.kusto.data.exceptions import KustoBlobError, KustoUploadError
from azure.kusto.ingest.V2.blob_source import BlobSource
from azure.kusto.ingest.V2.local_source import LocalSource


class KustoStorageUploader:
    _MAX_RETRIES = 3
    _SERVICE_CLIENT_TIMEOUT_SECONDS = 10 * 60

    def __init__(self, kcsb: Union[str, KustoConnectionStringBuilder]):
        super().__init__()
        if not isinstance(kcsb, KustoConnectionStringBuilder):
            kcsb = KustoConnectionStringBuilder(kcsb)

        self._connection_datasource = kcsb.data_source
        self._resource_manager = _ResourceManager(KustoClient(kcsb))
        self._endpoint_service_type = None
        self._suggested_endpoint_uri = None
        self._proxy_dict: Optional[Dict[str, str]] = None
        self.application_for_tracing = kcsb.client_details.application_for_tracing
        self.client_version_for_tracing = kcsb.client_details.version_for_tracing

    def upload_blob(
        self,
        blob_name: str,
        stream: IO[AnyStr],
        size: Optional[int] = None,
        source_id: Union[str, uuid] = None,
    ) -> "BlobDescriptor":
        """
        Uploads and transforms FileDescriptor or StreamDescriptor into a BlobDescriptor instance
        :param str blob_name:
        :param IO[AnyStr] stream: stream to be ingested from
        :param Optional[int] size:  estimated size of file if known
        :param Union[str, uuid] source_id: source id
        :return new BlobDescriptor instance
        """

        containers = self._resource_manager.get_containers()
        retries_left = min(self._MAX_RETRIES, len(containers))
        for container in containers:
            succeeded = True
            try:
                blob_service = BlobServiceClient(container.account_uri, proxies=self._proxy_dict)
                blob_client = blob_service.get_blob_client(container=container.object_name, blob=blob_name)
                blob_client.upload_blob(data=stream, timeout=self._SERVICE_CLIENT_TIMEOUT_SECONDS)
                return BlobDescriptor(blob_client.url, size, source_id)

            except Exception as e:
                succeeded = False
                retries_left = retries_left - 1
                if retries_left == 0:
                    raise KustoBlobError(e)
            finally:
                self._resource_manager.report_resource_usage_result(container.storage_account_name, succeeded)

    def close(self) -> None:
        self._resource_manager.close()

    def upload_local_source(self, local_source: LocalSource):
        try:
            local_stream = local_source.data()
            if local_stream is None or len(local_stream) == 0:
                raise KustoUploadError(local_source.name)
            blob_name = local_source.name + "_" + str(local_source.source_id) + "_" + str(local_source.compression_type.name)
            blob_uri = self.upload_blob(blob_name, local_stream, getattr(local_source, "size", None), local_source.source_id).path
            return BlobSource(blob_uri, local_source.format)
        except Exception as ex:
            if isinstance(ex, KustoBlobError):
                raise KustoUploadError(local_source.name)
            else:
                raise ex

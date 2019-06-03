import io
import os
from urllib.parse import urlparse

from azure.storage.blob import BlockBlobService

from kit.dtypes import infer
from kit.models.database import Table


def table_from_blob(blob_uri, name=None, **kwargs) -> Table:
    url = urlparse(blob_uri)
    minimum_rows = 200
    minimum_file_size = 1024 * 1024
    account, service_type, _, _, _ = url.hostname.split('.')
    container, blob_path = url.path.strip('/').split('/', 1)
    blob_path_without_extension, extension = os.path.splitext(blob_path)
    blobname = os.path.basename(blob_path_without_extension)
    blob_service = BlockBlobService(account)

    output_stream = io.BytesIO()
    blob = blob_service.get_blob_to_stream(container, blob_path, output_stream, start_range=0, end_range=minimum_file_size)
    current_file_size = output_stream.tell()
    buffered_stream = io.StringIO()

    output_stream.seek(0)

    partial_data = current_file_size > minimum_file_size
    if partial_data:
        # since we only have partial data, we need to read row by row before trying to parse to csv
        while output_stream.tell() < current_file_size:
            buffered_stream.write(output_stream.readline().decode('utf-8'))
        buffered_stream.seek(0)
    else:
        buffered_stream = output_stream

    columns = infer.columns_from_stream(
        buffered_stream, includes_headers=kwargs.get('includes_headers', False))

    return Table(name or blob_path, columns)

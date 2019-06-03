import io
import json
import os
from urllib.parse import urlparse
from urllib.request import urlopen

from azure.storage.blob import BlockBlobService
from urllib3.util import url


class LocalFile:
    def __init__(self, path: str, eager: bool = True):
        self.path = path
        self.data = None

        if eager:
            self.ensure()
            self.load()

    def ensure(self):
        if not os.path.exists(self.path):
            raise ValueError("{} file does not exist".format(self.__class__.__name__))

    @property
    def basename(self):
        return os.path.basename(self.path)

    def load(self):
        with open(self.path, 'r') as f:
            self.data = json.load(f)

    def save(self):
        with open(self.path, mode='w+') as f:
            json.dump(self.data, f)


class RemoteFile:
    def __init__(self, uri: str):
        self.uri = uri

    def download(self):
        uri_type = self.get_uri_type(self.uri)
        # blob storage
        if uri_type == 'azure_blob':
            self.download_blob(self.uri)
        if uri_type == 'plain':
            self.download_file(self.uri)

    def get_uri_type(self, uri):
        url = urlparse(uri)
        is_http = url.scheme in ['http', 'https']
        if url.scheme == 'abfss' or (is_http and 'storage.blob.core.windows.net' in url.hostname):
            return 'azure_blob'
        if is_http:
            return 'plain'

    def download_file(self, file_uri):
        self.data = json.load(urlopen(file_uri))

    def download_blob(self, blob_uri):
        minimum_rows = 200
        minimum_file_size = 1024 * 1024
        account, service_type, _, _, _ = url.hostname.split('.')
        container, blob_path = url.path.strip('/').split('/', 1)
        blob_path_without_extension, extension = os.path.splitext(blob_path)
        blobname = os.path.basename(blob_path_without_extension)
        blob_service = BlockBlobService(account)

        output_stream = io.BytesIO()
        blob = blob_service.get_blob_to_stream(container, blob_path, output_stream)
        buffered_stream = io.StringIO()

        output_stream.seek(0)

        self.data = json.load(output_stream)

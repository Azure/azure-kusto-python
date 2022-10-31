# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import random
import shutil
import struct
import uuid
from gzip import GzipFile
from io import BytesIO, SEEK_END
from typing import Union, Optional, AnyStr, IO, List, Dict
from zipfile import ZipFile

from azure.storage.blob import BlobServiceClient

from azure.kusto.data.exceptions import KustoBlobError
from azure.kusto.ingest._resource_manager import _ResourceUri

OptionalUUID = Optional[Union[str, uuid.UUID]]


def ensure_uuid(maybe_uuid: OptionalUUID) -> uuid.UUID:
    if not maybe_uuid:
        return uuid.uuid4()

    if type(maybe_uuid) == uuid.UUID:
        return maybe_uuid

    return uuid.UUID(f"{maybe_uuid}", version=4)


class FileDescriptor:
    """FileDescriptor is used to describe a file that will be used as an ingestion source."""

    # Gzip keeps the decompressed stream size as a UINT32 in the last 4 bytes of the stream, however this poses a limit to the expressed size which is 4GB
    # The standard says that when the size is bigger than 4GB, the UINT rolls over.
    # The below constant expresses the maximal size of a compressed stream that will not cause the UINT32 to rollover given a maximal compression ratio of 1:40
    GZIP_MAX_DISK_SIZE_FOR_DETECTION = int(4 * 1024 * 1024 * 1024 / 40)
    DEFAULT_COMPRESSION_RATIO = 11

    def __init__(self, path: str, size: Optional[int] = None, source_id: OptionalUUID = None):
        """
        :param path: file path.
        :type path: str.
        :param size: estimated size of file if known. if None or 0 will try to guess.
        :type size: Optional[int].
        :param source_id: a v4 uuid to serve as the source's id.
        :type source_id: OptionalUUID
        """
        self.path: str = path
        self._size: Optional[int] = size
        self._detect_size_once: bool = not size

        self.source_id: uuid.UUID = ensure_uuid(source_id)
        self.stream_name: str = os.path.basename(self.path)

    @property
    def size(self) -> int:
        if self._detect_size_once:
            self._detect_size()
            self._detect_size_once = False

        return self._size

    @size.setter
    def size(self, size: int):
        if size:
            self._size = size
            self._detect_size_once = False

    def _detect_size(self):
        uncompressed_size = 0
        if self.path.endswith(".gz"):
            # This logic follow after the C# implementation
            # See IngstionHelpers.cs for an explanation as to what stands behind it
            with open(self.path, "rb") as f:
                disk_size = f.seek(-4, SEEK_END)
                uncompressed_size = struct.unpack("I", f.read(4))[0]
                if (disk_size >= uncompressed_size) or (disk_size >= self.GZIP_MAX_DISK_SIZE_FOR_DETECTION):
                    uncompressed_size = disk_size * self.DEFAULT_COMPRESSION_RATIO

        elif self.path.endswith(".zip"):
            with ZipFile(self.path) as zip_archive:
                for f in zip_archive.infolist():
                    uncompressed_size += f.file_size

        else:
            uncompressed_size = os.path.getsize(self.path)

        self._size = uncompressed_size

    @property
    def is_compressed(self) -> bool:
        return self.path.endswith(".gz") or self.path.endswith(".zip")

    def open(self, should_compress: bool) -> BytesIO:
        if should_compress:
            file_stream = self.compress_stream()
        else:
            file_stream = open(self.path, "rb")
        return file_stream

    def compress_stream(self) -> BytesIO:
        self.stream_name += ".gz"
        file_stream = BytesIO()
        with open(self.path, "rb") as f_in, GzipFile(filename="data", fileobj=file_stream, mode="wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        file_stream.seek(0)
        return file_stream


class BlobDescriptor:
    """BlobDescriptor is used to describe a blob that will be used as an ingestion source"""

    def __init__(self, path: str, size: Optional[int] = None, source_id: OptionalUUID = None):
        """
        :param path: blob uri.
        :type path: str.
        :param size: estimated size of file if known.
        :type size: Optional[int].
        :param source_id: a v4 uuid to serve as the sources id.
        :type source_id: OptionalUUID
        """
        self.path: str = path
        self.size: Optional[int] = size
        self.source_id: uuid.UUID = ensure_uuid(source_id)

    @staticmethod
    def upload_from_different_descriptor(
        containers: List[_ResourceUri],
        descriptor: Union[FileDescriptor, "StreamDescriptor"],
        database: str,
        table: str,
        stream: IO[AnyStr],
        proxy_dict: Optional[Dict[str, str]],
        timeout: int,
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
        random_container = random.choice(containers)
        try:
            blob_service = BlobServiceClient(random_container.account_uri, proxies=proxy_dict)
            blob_client = blob_service.get_blob_client(container=random_container.object_name, blob=blob_name)
            blob_client.upload_blob(data=stream, timeout=timeout)
        except Exception as e:
            raise KustoBlobError(e)
        return BlobDescriptor(blob_client.url, descriptor.size, descriptor.source_id)


class StreamDescriptor:
    """StreamDescriptor is used to describe a stream that will be used as ingestion source"""

    # TODO: currently we always assume that streams are gz compressed (will get compressed before sending), should we expand that?
    def __init__(
        self, stream: IO[AnyStr], source_id: OptionalUUID = None, is_compressed: bool = False, stream_name: Optional[str] = None, size: Optional[int] = None
    ):
        """
        :param stream: in-memory stream object.
        :type stream: io.BaseIO
        :param source_id: a v4 uuid to serve as the sources id.
        :type source_id: OptionalUUID
        :param is_compressed: specify if the provided stream is compressed
        :type is_compressed: boolean
        """
        self.stream: IO[AnyStr] = stream
        self.source_id: uuid.UUID = ensure_uuid(source_id)
        self.is_compressed: bool = is_compressed
        self.stream_name: str = stream_name
        if self.stream_name is None:
            self.stream_name = "stream"
            if is_compressed:
                self.stream_name += ".gz"
        self.size: Optional[int] = size

    def compress_stream(self) -> None:
        stream = self.stream
        zipped_stream = BytesIO()
        buffer = stream.read()
        with GzipFile(filename="data", fileobj=zipped_stream, mode="wb") as f_out:
            if isinstance(buffer, str):
                data = bytes(buffer, "utf-8")
                f_out.write(data)
            else:
                f_out.write(buffer)
        zipped_stream.seek(0)
        self.is_compressed = True
        self.stream_name += ".gz"
        self.stream = zipped_stream

    @staticmethod
    def from_file_descriptor(file_descriptor: Union[FileDescriptor, str]) -> "StreamDescriptor":
        """
        Transforms FileDescriptor instance into StreamDescriptor instance. Note that stream is open when instance is returned
        :param Union[FileDescriptor, str] file_descriptor: File Descriptor instance
        :return new StreamDescriptor instance
        """
        if isinstance(file_descriptor, FileDescriptor):
            descriptor = file_descriptor
        else:
            descriptor = FileDescriptor(file_descriptor)
        stream = open(descriptor.path, "rb")
        is_compressed = descriptor.path.endswith(".gz") or descriptor.path.endswith(".zip")
        stream_descriptor = StreamDescriptor(stream, descriptor.source_id, is_compressed, descriptor.stream_name, descriptor.size)
        return stream_descriptor

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import shutil
import struct
import uuid
from gzip import GzipFile
from io import BytesIO, SEEK_END
from typing import AnyStr, Union
from zipfile import ZipFile

from typing.io import IO


def assert_uuid4(maybe_uuid: str, error_message: str):
    # none is valid value for our purposes
    if maybe_uuid is None:
        return

    maybe_uuid = uuid.UUID("{}".format(maybe_uuid)) if maybe_uuid is not None else None
    if maybe_uuid and maybe_uuid.version != 4:
        raise ValueError(error_message)


class FileDescriptor:
    """FileDescriptor is used to describe a file that will be used as an ingestion source."""

    # Gzip keeps the decompressed stream size as a UINT32 in the last 4 bytes of the stream, however this poses a limit to the expressed size which is 4GB
    # The standard says that when the size is bigger then 4GB, the UINT rolls over.
    # The below constant expresses the maximal size of a compressed stream that will not cause the UINT32 to rollover given a maximal compression ratio of 1:40
    GZIP_MAX_DISK_SIZE_FOR_DETECTION = int(4 * 1024 * 1024 * 1024 / 40)
    DEFAULT_COMPRESSION_RATIO = 11

    def __init__(self, path, size=0, source_id=None):
        """
        :param path: file path.
        :type path: str.
        :param size: estimated size of file if known. if None or 0 will try to guess.
        :type size: int.
        :param source_id uuid: a v4 uuid to serve as the sources id.
        :type source_id: str (of a uuid4) or uuid4.
        """
        self.path = path
        self._size = size
        self._detect_size_once = size < 1

        assert_uuid4(source_id, "source_id must be a valid uuid4")
        self.source_id = source_id
        self.stream_name = os.path.basename(self.path)

    @property
    def size(self):
        if self._detect_size_once:
            self._detect_size()
            self._detect_size_once = False

        return self._size

    @size.setter
    def size(self, size):
        if size > 0:
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

    def open(self, should_compress: bool) -> BytesIO:
        if should_compress:
            self.stream_name += ".gz"
            file_stream = BytesIO()
            with open(self.path, "rb") as f_in, GzipFile(filename="data", fileobj=file_stream, mode="wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            file_stream.seek(0)
        else:
            file_stream = open(self.path, "rb")

        return file_stream


class BlobDescriptor:
    """FileDescriptor is used to describe a file that will be used as an ingestion source"""

    def __init__(self, path: str, size: int, source_id: str = None):
        """
        :param path: blob uri.
        :type path: str.
        :param size: estimated size of file if known.
        :type size: int.
        :param source_id uuid: a v4 uuid to serve as the sources id.
        :type source_id: str (of a uuid4) or uuid4.
        """
        self.path = path
        self.size = size
        assert_uuid4(source_id, "source_id must be a valid uuid4")
        self.source_id = source_id


class StreamDescriptor:
    """StreamDescriptor is used to describe a stream that will be used as ingestion source"""

    def __init__(self, stream: Union[IO[AnyStr], BytesIO], source_id: str = None, is_compressed: bool = False):
        """
        :param stream: in-memory stream object.
        :type stream: io.BaseIO
        :param source_id: a v4 uuid to serve as the sources id.
        :type source_id: str (of a uuid4) or uuid4.
        :param is_compressed: specify if the provided stream is compressed
        :type is_compressed: boolean
        """
        self.stream = stream
        assert_uuid4(source_id, "source_id must be a valid uuid4")
        self.source_id = source_id
        self.is_compressed = is_compressed

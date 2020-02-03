"""Descriptors the ingest command should work with."""

import os
import shutil
import uuid
from gzip import GzipFile
from io import BytesIO


def assert_uuid4(maybe_uuid, error_message):
    # none is valid value for our purposes
    if maybe_uuid is None:
        return

    maybe_uuid = uuid.UUID("{}".format(maybe_uuid)) if maybe_uuid is not None else None
    if maybe_uuid and maybe_uuid.version != 4:
        raise ValueError(error_message)


class FileDescriptor:
    """FileDescriptor is used to describe a file that will be used as an ingestion source."""

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
        self.size = size

        assert_uuid4(source_id, "source_id must be a valid uuid4")
        self.source_id = source_id
        self.stream_name = os.path.basename(self.path)

        if self.path.endswith(".gz") or self.path.endswith(".zip"):
            # TODO: this can be improved by reading last 4 bytes
            self.size = int(os.path.getsize(self.path)) * 11
        elif not self.size or self.size <= 0:
            self.size = int(os.path.getsize(self.path))

    def open(self, should_compress):
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

    def __init__(self, path, size, source_id=None):
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

    def __init__(self, stream, source_id=None, is_compressed=False):
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

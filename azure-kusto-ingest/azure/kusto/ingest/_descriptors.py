"""Descriptors the ingest command should work with."""

import os
from io import BytesIO, SEEK_SET, SEEK_END
import shutil
from gzip import GzipFile
import tempfile
import uuid


def assert_uuid4(maybe_uuid, error_message):
    # none is valid value for our purposes
    if maybe_uuid is None:
        return

    maybe_uuid = uuid.UUID("{}".format(maybe_uuid)) if maybe_uuid is not None else None
    if maybe_uuid and maybe_uuid.version != 4:
        raise ValueError(error_message)


class FileDescriptor(object):
    """FileDescriptor is used to describe a file that will be used as an ingestion source."""

    # TODO: this should be changed. holding zipped data in memory isn't efficient
    # also, init should be a lean method, not potentially reading and writing files
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
            self.zipped_stream = open(self.path, "rb")
            if not self.size or self.size <= 0:
                # TODO: this can be improved by reading last 4 bytes
                self.size = int(os.path.getsize(self.path)) * 5
        else:
            self.size = int(os.path.getsize(self.path))
            self.stream_name += ".gz"
            self.zipped_stream = BytesIO()
            with open(self.path, "rb") as f_in, GzipFile(
                filename="data", fileobj=self.zipped_stream, mode="wb"
            ) as f_out:
                shutil.copyfileobj(f_in, f_out)
            self.zipped_stream.seek(0)

    def delete_files(self):
        """Deletes the gz file if the original file was not zipped.
        In case of success deletes the original file as well."""
        if self.zipped_stream is not None:
            self.zipped_stream.close()


class BlobDescriptor(object):
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


class StreamDescriptor(object):
    """StreamDescriptor is used to describe a stream that will be used as ingestion source"""

    def __init__(self, stream, size=None, source_id=None, is_zipped_stream=False):
        """
        :param stream: in-memory stream object.
        :type stream: io.BaseIO
        :param size: size of the provided stream.
        :type: size: int
        :param source_id: a v4 uuid to serve as the sources id.
        :type source_id: str (of a uuid4) or uuid4.
        """
        self.stream = stream
        if size is None:
            stream.seek(0, SEEK_END)
            size = stream.tell()
            stream.seek(0, SEEK_SET)

        self.size = size
        assert_uuid4(source_id, "source_id must be a valid uuid4")
        self.source_id = source_id
        self.is_zipped_stream = is_zipped_stream

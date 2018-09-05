"""Descriptors the ingest command should work with."""

import os
from io import BytesIO
import shutil
from gzip import GzipFile
import tempfile


class FileDescriptor(object):
    """A file to ingest."""

    # TODO: this should be changed. holding zipped data in memory isn't efficient
    # also, init should be a lean method, not potentially reading and writing files
    def __init__(self, path, size=0):
        self.path = path
        self.size = size
        self.stream_name = os.path.basename(self.path)
        if self.path.endswith(".gz") or self.path.endswith(".zip"):
            self.zipped_stream = open(self.path, "rb")
            if self.size <= 0:
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
    """A blob to ingest."""

    def __init__(self, path, size):
        self.path = path
        self.size = size

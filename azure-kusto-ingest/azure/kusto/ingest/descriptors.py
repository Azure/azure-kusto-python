"""Descriptors the ingest command should work with."""

import os
from io import BytesIO
import shutil
from gzip import GzipFile
import tempfile


class FileDescriptor(object):
    """A file to ingest."""

    def __init__(self, path, size=0, deleteSourcesOnSuccess=False):
        self.path = path
        self.size = size
        self.delete_sources_on_success = deleteSourcesOnSuccess
        self.stream_name = os.path.basename(self.path)
        if self.path.endswith(".gz") or self.path.endswith(".zip"):
            # review: what is the class providing that requires keeping this
            # file open? And could that need be equally served by a context
            # manager to help guarantee you close the file promptly and
            # definitively?
            self.zipped_stream = open(self.path, 'rb')
            if self.size <= 0:
                self.size = int(os.path.getsize(self.path)) * 5
        else:
            self.size = int(os.path.getsize(self.path))
            self.stream_name += '.gz'
            self.zipped_stream = BytesIO()
            with open(self.path, "rb") as f_in, GzipFile(filename='data', fileobj=self.zipped_stream, mode="wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            self.zipped_stream.seek(0)

    # review: if you made this close() then you could use
    # https://docs.python.org/3/library/contextlib.html#contextlib.closing with
    # this class.
    def delete_files(self, success):
        """
        Deletes the gz file if the original file was not zipped.
        In case of success deletes the original file as well.
        """
        # review: your __init__() always sets self.zipped_strem to not be None,
        # so this won't be an issue.
        if self.zipped_stream is not None:
            self.zipped_stream.close()
        if success and self.delete_sources_on_success:
            os.remove(self.path)


class BlobDescriptor(object):
    """A class that defines a blob to ingest."""

    def __init__(self, path, size):
        self.path = path
        self.size = size

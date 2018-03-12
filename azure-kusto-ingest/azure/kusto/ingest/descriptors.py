""" A file contains all descriptors ingest command should work with.
"""

import os
import shutil
import gzip
import tempfile

class FileDescriptor(object):
    """ A class that defines a file to ingest. """
    def __init__(self, path, size=0, deleteSourcesOnSuccess=False):
        self.path = path
        self.size = size
        self.delete_sources_on_success = deleteSourcesOnSuccess
        self.zipped_temp_file = None
        if self.path.endswith(".gz") or self.path.endswith(".zip"):
            if self.size <= 0:
                self.size = int(os.path.getsize(self.path)) * 5
        else:
            file_path, extension = os.path.splitext(self.path)
            suffix = extension + ".gz"
            prefix = os.path.basename(file_path) + "__"
            self.zipped_temp_file = tempfile.NamedTemporaryFile("wb", suffix=suffix, prefix=prefix)
            self._zip_file()

    def _zip_file(self):
        with open(self.path, 'rb') as f_in, gzip.open(self.zipped_temp_file.file.name, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        self.size = int(os.path.getsize(f_in.name))

    def zipped_file(self):
        """ Gets the path to the zipped file to upload to a blob. """
        if self.zipped_temp_file is not None:
            return self.zipped_temp_file.file.name
        return self.path

    def delete_files(self, success):
        """
        Deletes the gz file if the original file was not zipped.
        In case of success deletes the original file as well.
        """
        if self.zipped_temp_file is not None:
            self.zipped_temp_file.close()
        if success and self.delete_sources_on_success:
            os.remove(self.path)

class BlobDescriptor(object):
    """ A class that defines a blob to ingest. """
    def __init__(self, path, size):
        self.path = path
        self.size = size

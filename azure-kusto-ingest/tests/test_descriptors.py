import sys
from os import path
import unittest
from azure.kusto.ingest import FileDescriptor


class DescriptorsTest(unittest.TestCase):
    """Test class for FileDescriptor and BlobDescriptor."""

    uncompressed_size = 1569
    mock_size = 11

    def test_unzipped_file_with_size(self):
        """Tests FileDescriptor with size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, self.mock_size)
        with descriptor.open(True) as stream:
            assert descriptor.size == self.mock_size
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_unzipped_file_without_size(self):
        """Tests FileDescriptor without size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 0)
        with descriptor.open(True) as stream:
            assert descriptor.size == self.uncompressed_size
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_zipped_file_with_size(self):
        """Tests FileDescriptor with size and zipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, self.mock_size)
        with descriptor.open(False) as stream:
            assert descriptor.size == self.mock_size
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_gzip_file_without_size(self):
        """Tests FileDescriptor without size and zipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 0)
        with descriptor.open(False) as stream:
            assert descriptor.size == self.uncompressed_size
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_zip_file_without_size(self):
        """Tests FileDescriptor without size and zipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.zip")
        descriptor = FileDescriptor(filePath, 0)
        with descriptor.open(False) as stream:
            # the zip archive contains 2 copies of the source file
            assert descriptor.size == self.uncompressed_size * 2
            assert descriptor.stream_name.endswith(".csv.zip")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_unzipped_file_dont_compress(self):
        """Tests FileDescriptor with size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, self.mock_size)
        with descriptor.open(False) as stream:
            assert descriptor.size == self.mock_size
            assert descriptor.stream_name.endswith(".csv")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

import sys
from os import path
import unittest
from azure.kusto.ingest import FileDescriptor


class DescriptorsTest(unittest.TestCase):
    """Test class for FileDescriptor and BlobDescriptor."""

    def test_unzipped_file_with_size(self):
        """Tests FileDescriptor with size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 10)
        with descriptor.open(True) as stream:
            assert descriptor.size == 10
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
            assert descriptor.size > 10
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_zipped_file_with_size(self):
        """Tests FileDescriptor with size and zipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 10)
        with descriptor.open(False) as stream:
            assert descriptor.size == 10
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_zipped_file_without_size(self):
        """Tests FileDescriptor without size and zipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 0)
        with descriptor.open(False) as stream:
            assert descriptor.size == 5071
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

    def test_unzipped_file_dont_compress(self):
        """Tests FileDescriptor with size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 10)
        with descriptor.open(False) as stream:
            assert descriptor.size == 10
            assert descriptor.stream_name.endswith(".csv")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed == True

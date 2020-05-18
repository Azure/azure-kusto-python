# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import sys
from os import path
import unittest
from azure.kusto.ingest import FileDescriptor


class DescriptorsTest(unittest.TestCase):
    """Test class for FileDescriptor and BlobDescriptor."""

    # this is the size with LF line endings
    uncompressed_size = 1569
    # this is the size with CRLF line endings
    uncompressed_size_2 = 1578
    mock_size = 10

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

        assert stream.closed is True

    def test_unzipped_file_without_size(self):
        """Tests FileDescriptor without size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 0)
        with descriptor.open(True) as stream:

            # TODO: since we don't know if the file is opened on CRLF system or an LF system, allow both sizes
            #   a more robust approach would be to open the file and check
            assert descriptor.size in (self.uncompressed_size, self.uncompressed_size_2)
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed is True

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

        assert stream.closed is True

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

        assert stream.closed is True

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

        assert stream.closed is True

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

        assert stream.closed is True

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import sys
import uuid
from io import BytesIO

import pytest

from azure.kusto.ingest import FileDescriptor, BlobDescriptor, StreamDescriptor
from .common import get_input_folder_path


class TestDescriptors:
    """Test class for FileDescriptor and BlobDescriptor."""

    # this is the size with LF line endings
    uncompressed_size = 1569
    # this is the size with CRLF line endings
    uncompressed_size_2 = 1578
    mock_size = 10

    INVALID_UUID = "12345"
    TEST_UUID_STR = "5bcc12b7-e35c-4c76-a40a-2d89e6c2c7dd"
    TEST_UUID = uuid.UUID("5bcc12b7-e35c-4c76-a40a-2d89e6c2c7dd", version=4)

    def test_unzipped_file_with_size(self):
        """Tests FileDescriptor with size and unzipped file."""
        file_path = str(get_input_folder_path("dataset.csv"))
        descriptor = FileDescriptor(file_path, self.mock_size)
        with descriptor.open(True) as stream:
            assert descriptor.size == self.mock_size
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed is True

    def test_unzipped_file_without_size(self):
        """Tests FileDescriptor without size and unzipped file."""
        file_path = str(get_input_folder_path("dataset.csv"))
        descriptor = FileDescriptor(file_path, 0)
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
        file_path = str(get_input_folder_path("dataset.csv.gz"))
        descriptor = FileDescriptor(file_path, self.mock_size)
        with descriptor.open(False) as stream:
            assert descriptor.size == self.mock_size
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed is True

    def test_gzip_file_without_size(self):
        """Tests FileDescriptor without size and zipped file."""
        file_path = str(get_input_folder_path("dataset.csv.gz"))
        descriptor = FileDescriptor(file_path, 0)
        with descriptor.open(False) as stream:
            assert descriptor.size == self.uncompressed_size
            assert descriptor.stream_name.endswith(".csv.gz")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed is True

    def test_zip_file_without_size(self):
        """Tests FileDescriptor without size and zipped file."""
        file_path = str(get_input_folder_path("dataset.csv.zip"))
        descriptor = FileDescriptor(file_path, 0)
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
        file_path = str(get_input_folder_path("dataset.csv"))
        descriptor = FileDescriptor(file_path, self.mock_size)
        with descriptor.open(False) as stream:
            assert descriptor.size == self.mock_size
            assert descriptor.stream_name.endswith(".csv")
            if sys.version_info[0] >= 3:
                assert stream.readable()
            assert stream.tell() == 0

        assert stream.closed is True

    def test_uuid_stream_descriptor(self):
        dummy_stream = BytesIO(b"dummy")

        descriptor = StreamDescriptor(dummy_stream)
        assert descriptor.source_id
        assert descriptor.source_id != TestDescriptors.TEST_UUID
        assert uuid.UUID(str(descriptor.source_id), version=4)

        descriptor = StreamDescriptor(dummy_stream, source_id=TestDescriptors.TEST_UUID_STR)
        assert descriptor.source_id == TestDescriptors.TEST_UUID

        descriptor = StreamDescriptor(dummy_stream, source_id=TestDescriptors.TEST_UUID)
        assert descriptor.source_id == TestDescriptors.TEST_UUID

        with pytest.raises(ValueError):
            StreamDescriptor(dummy_stream, source_id=TestDescriptors.INVALID_UUID)

    def test_uuid_file_descriptor(self):
        dummy_file = "dummy"

        descriptor = FileDescriptor(dummy_file)
        assert descriptor.source_id
        assert descriptor.source_id != TestDescriptors.TEST_UUID
        assert uuid.UUID(str(descriptor.source_id), version=4)

        descriptor = FileDescriptor(dummy_file, source_id=TestDescriptors.TEST_UUID_STR)
        assert descriptor.source_id == TestDescriptors.TEST_UUID

        descriptor = FileDescriptor(dummy_file, source_id=TestDescriptors.TEST_UUID)
        assert descriptor.source_id == TestDescriptors.TEST_UUID

        with pytest.raises(ValueError):
            FileDescriptor(dummy_file, source_id=TestDescriptors.INVALID_UUID)

    def test_uuid_blob_descriptor(self):
        dummy_file = "dummy"

        descriptor = BlobDescriptor(dummy_file)
        assert descriptor.source_id
        assert descriptor.source_id != TestDescriptors.TEST_UUID
        assert uuid.UUID(str(descriptor.source_id), version=4)

        descriptor = BlobDescriptor(dummy_file, source_id=TestDescriptors.TEST_UUID_STR)
        assert descriptor.source_id == TestDescriptors.TEST_UUID

        descriptor = BlobDescriptor(dummy_file, source_id=TestDescriptors.TEST_UUID)
        assert descriptor.source_id == TestDescriptors.TEST_UUID

        with pytest.raises(ValueError):
            BlobDescriptor(dummy_file, source_id=TestDescriptors.INVALID_UUID)

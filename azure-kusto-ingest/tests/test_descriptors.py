"""Test class for FileDescriptor and BlobDescriptor."""

import sys
from os import path
import unittest
from azure.kusto.ingest.descriptors import FileDescriptor


class DescriptorsTest(unittest.TestCase):
    """Test class for FileDescriptor and BlobDescriptor."""

    @classmethod
    def setup_class(cls):
        cls.csv_file_path = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        cls.zipped_file_path = path.join(
            path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz"
        )

    def test_unzipped_file_with_size(self):
        """ Tests FileDescriptor with size and unzipped file """
        descriptor = FileDescriptor(self.csv_file_path, 10)
        self.assertGreater(descriptor.size, 10)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        if sys.version_info[0] >= 3:
            self.assertTrue(descriptor.zipped_stream.readable())
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertFalse(descriptor.zipped_stream.closed)
        descriptor.delete_files(True)
        self.assertTrue(descriptor.zipped_stream.closed)

    def test_unzipped_file_without_size(self):
        """Tests FileDescriptor without size and unzipped file."""
        descriptor = FileDescriptor(self.csv_file_path, 0)
        self.assertGreater(descriptor.size, 0)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        if sys.version_info[0] >= 3:
            self.assertTrue(descriptor.zipped_stream.readable())
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertFalse(descriptor.zipped_stream.closed)
        descriptor.delete_files(True)
        self.assertTrue(descriptor.zipped_stream.closed)

    def test_zipped_file_with_size(self):
        """Tests FileDescriptor with size and zipped file."""
        descriptor = FileDescriptor(self.zipped_file_path, 10)
        self.assertEqual(descriptor.size, 10)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        if sys.version_info[0] >= 3:
            self.assertTrue(descriptor.zipped_stream.readable())
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertFalse(descriptor.zipped_stream.closed)
        descriptor.delete_files(True)
        self.assertTrue(descriptor.zipped_stream.closed)

    def test_zipped_file_without_size(self):
        """Tests FileDescriptor without size and zipped file."""
        descriptor = FileDescriptor(self.zipped_file_path, 0)
        self.assertEqual(descriptor.size, 2305)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        if sys.version_info[0] >= 3:
            self.assertTrue(descriptor.zipped_stream.readable())
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertFalse(descriptor.zipped_stream.closed)
        descriptor.delete_files(True)
        self.assertTrue(descriptor.zipped_stream.closed)

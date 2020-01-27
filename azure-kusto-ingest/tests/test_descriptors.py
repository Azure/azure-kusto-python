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
            self.assertEquals(descriptor.size, 10)
            self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
            if sys.version_info[0] >= 3:
                self.assertTrue(stream.readable(), True)
            self.assertEqual(stream.tell(), 0)

        self.assertEqual(stream.closed, True)

    def test_unzipped_file_without_size(self):
        """Tests FileDescriptor without size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 0)
        with descriptor.open(True) as stream:
            self.assertGreater(descriptor.size, 0)
            self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
            if sys.version_info[0] >= 3:
                self.assertTrue(stream.readable(), True)
            self.assertEqual(stream.tell(), 0)

        self.assertEqual(stream.closed, True)

    def test_zipped_file_with_size(self):
        """Tests FileDescriptor with size and zipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 10)
        with descriptor.open(False) as stream:
            self.assertGreater(descriptor.size, 10)
            self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
            if sys.version_info[0] >= 3:
                self.assertTrue(stream.readable(), True)
            self.assertEqual(stream.tell(), 0)

        self.assertEqual(stream.closed, True)

    def test_zipped_file_without_size(self):
        """Tests FileDescriptor without size and zipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 0)
        with descriptor.open(False) as stream:
            self.assertEqual(descriptor.size, 5071)
            self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
            if sys.version_info[0] >= 3:
                self.assertTrue(stream.readable(), True)
            self.assertEqual(stream.tell(), 0)

        self.assertEqual(stream.closed, True)

    def test_unzipped_file_dont_compress(self):
        """Tests FileDescriptor with size and unzipped file."""
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 10)
        with descriptor.open(False) as stream:
            self.assertEqual(descriptor.size, 10)
            self.assertTrue(descriptor.stream_name.endswith(".csv"))
            if sys.version_info[0] >= 3:
                self.assertTrue(stream.readable(), True)
            self.assertEqual(stream.tell(), 0)

        self.assertEqual(stream.closed, True)

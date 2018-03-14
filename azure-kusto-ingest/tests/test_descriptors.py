""" Test class for FileDescriptor and BlobDescriptor
"""

from os import path
import unittest
from azure.kusto.ingest import FileDescriptor

class DescriptorsTest(unittest.TestCase):
    """ Test class for FileDescriptor and BlobDescriptor
    """
    def test_unzipped_file_with_size(self):
        """ Tests FileDescriptor with size and unzipped file """
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 10)
        self.assertGreater(descriptor.size, 10)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        self.assertTrue(descriptor.zipped_stream.readable(), True)
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertEqual(descriptor.zipped_stream.closed, False)
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_stream.closed, True)

    def test_unzipped_file_without_size(self):
        """ Tests FileDescriptor without size and unzipped file """
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 0)
        self.assertGreater(descriptor.size, 0)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        self.assertTrue(descriptor.zipped_stream.readable(), True)
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertEqual(descriptor.zipped_stream.closed, False)
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_stream.closed, True)

    def test_zipped_file_with_size(self):
        """ Tests FileDescriptor with size and zipped file """
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 10)
        self.assertEqual(descriptor.size, 10)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        self.assertTrue(descriptor.zipped_stream.readable(), True)
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertEqual(descriptor.zipped_stream.closed, False)
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_stream.closed, True)

    def test_zipped_file_without_size(self):
        """ Tests FileDescriptor without size and zipped file """
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 0)
        self.assertEqual(descriptor.size, 2305)
        self.assertTrue(descriptor.stream_name.endswith(".csv.gz"))
        self.assertTrue(descriptor.zipped_stream.readable(), True)
        self.assertEquals(descriptor.zipped_stream.tell(), 0)
        self.assertEqual(descriptor.zipped_stream.closed, False)
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_stream.closed, True)

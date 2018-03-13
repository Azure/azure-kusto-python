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
        self.assertEqual(descriptor.size, 1578)
        self.assertTrue(descriptor.zipped_file().endswith(".csv.gz"))
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_temp_file.closed, True)

    def test_unzipped_file_without_size(self):
        """ Tests FileDescriptor without size and unzipped file """
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv")
        descriptor = FileDescriptor(filePath, 0)
        self.assertEqual(descriptor.size, 1578)
        self.assertTrue(descriptor.zipped_file().endswith(".csv.gz"))
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_temp_file.closed, True)

    def test_zipped_file_with_size(self):
        """ Tests FileDescriptor with size and zipped file """
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 10)
        self.assertEqual(descriptor.size, 10)
        self.assertEqual(descriptor.zipped_file(), filePath)
        descriptor.delete_files(True)


    def test_zipped_file_without_size(self):
        """ Tests FileDescriptor without size and zipped file """
        filePath = path.join(path.dirname(path.abspath(__file__)), "input", "dataset.csv.gz")
        descriptor = FileDescriptor(filePath, 0)
        self.assertEqual(descriptor.size, 2305)
        self.assertEqual(descriptor.zipped_file(), filePath)
        descriptor.delete_files(True)

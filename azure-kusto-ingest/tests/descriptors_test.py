""" Test class for FileDescriptor and BlobDescriptor
"""

import os
import unittest
from kusto_ingest_client.descriptors import FileDescriptor

class DescriptorsTest(unittest.TestCase):
    """ Test class for FileDescriptor and BlobDescriptor
    """
    def test_unzipped_file_with_size(self):
        """ Tests FileDescriptor with size and unzipped file """
        path = os.path.join(os.getcwd(), "test", "Input", "dataset.csv")
        descriptor = FileDescriptor(path, 10)
        self.assertEqual(descriptor.size, 1569)
        self.assertTrue(descriptor.zipped_file().endswith(".csv.gz"))
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_temp_file.closed, True)

    def test_unzipped_file_without_size(self):
        """ Tests FileDescriptor without size and unzipped file """
        path = os.path.join(os.getcwd(), "test", "Input", "dataset.csv")
        descriptor = FileDescriptor(path, 0)
        self.assertEqual(descriptor.size, 1569)
        self.assertTrue(descriptor.zipped_file().endswith(".csv.gz"))
        descriptor.delete_files(True)
        self.assertEqual(descriptor.zipped_temp_file.closed, True)

    def test_zipped_file_with_size(self):
        """ Tests FileDescriptor with size and zipped file """
        path = os.path.join(os.getcwd(), "test", "Input", "dataset.csv.gz")
        descriptor = FileDescriptor(path, 10)
        self.assertEqual(descriptor.size, 10)
        self.assertEqual(descriptor.zipped_file(), path)
        descriptor.delete_files(True)


    def test_zipped_file_without_size(self):
        """ Tests FileDescriptor without size and zipped file """
        path = os.path.join(os.getcwd(), "test", "Input", "dataset.csv.gz")
        descriptor = FileDescriptor(path, 0)
        self.assertEqual(descriptor.size, 2305)
        self.assertEqual(descriptor.zipped_file(), path)
        descriptor.delete_files(True)

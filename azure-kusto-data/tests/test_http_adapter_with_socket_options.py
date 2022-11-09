# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import pickle
import unittest
import socket

from azure.kusto.data.client import HTTPAdapterWithSocketOptions

class HTTPAdapterWithSocketOptionsTests(unittest.TestCase):
    def test_pickle(self):
        adapter = HTTPAdapterWithSocketOptions(socket_options=[(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)])
        pickled = pickle.dumps(adapter)
        pickle.loads(pickled)

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import pickle
import socket

from azure.kusto.data.client import HTTPAdapterWithSocketOptions


def test_pickle():
    socket_options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
    original = HTTPAdapterWithSocketOptions(socket_options=socket_options)
    unpickled = pickle.loads(pickle.dumps(original))
    assert unpickled.socket_options == socket_options


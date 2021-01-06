# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import time
import unittest
from uuid import uuid4

import mock
from azure.kusto.ingest import QueuedIngestClient
from azure.kusto.ingest._resource_manager import _ResourceUri
from azure.kusto.ingest.status import KustoIngestStatusQueues, SuccessMessage, FailureMessage
from azure.storage.queue import QueueMessage, QueueClient

ENDPOINT_SUFFIX = "sp=rl&st=2020-05-20T13:38:37Z&se=2020-05-21T13:38:37Z&sv=2019-10-10&sr=c&sig=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
OBJECT_TYPE = "core.windows.net"


def mock_message(success):
    m = QueueMessage()
    m.id = uuid4()
    m.insertion_time = time.time()
    m.expiration_time = None
    m.dequeue_count = None

    if success:
        content = {
            "OperationId": str(m.id),
            "Database": "db1",
            "Table": "table1",
            "IngestionSourceId": str(m.id),
            "IngestionSourcePath": "blob/path",
            "RootActivityId": "1",
            "SucceededOn": time.time(),
        }
    else:
        content = {
            "OperationId": str(m.id),
            "Database": "db1",
            "Table": "table1",
            "IngestionSourceId": str(m.id),
            "IngestionSourcePath": "blob/path",
            "RootActivityId": "1",
            "FailedOn": time.time(),
            "Details": "",
            "ErrorCode": "1",
            "FailureStatus": "",
            "OriginatesFromUpdatePolicy": "",
            "ShouldRetry": False,
        }

    m.content = json.dumps(content)
    m.pop_receipt = None
    m.time_next_visible = None

    return m


def fake_peek_factory(f):
    def fake_peek(self, max_messages):
        return f(self.queue_name, max_messages)

    return fake_peek


def fake_receive_factory(f):
    def fake_receive(self, messages_per_page, *args, **kwargs):
        return f(self.queue_name, messages_per_page)

    return fake_receive


def fake_delete_factory(f):
    def fake_delete(self, n):
        return f(self.queue_name, n)

    return fake_delete


class StatusQTests(unittest.TestCase):
    def test_init(self):
        client = QueuedIngestClient("some-cluster")
        qs = KustoIngestStatusQueues(client)

        assert qs.success.message_cls == SuccessMessage
        assert qs.failure.message_cls == FailureMessage

    def test_isempty(self):
        client = QueuedIngestClient("some-cluster")

        fake_peek = fake_peek_factory(
            lambda queue_name, num_messages=1: [mock_message(success=True) for _ in range(0, num_messages)] if "qs" in queue_name else []
        )
        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues") as mocked_get_success_qs, mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs, mock.patch.object(QueueClient, "peek_messages", autospec=True, side_effect=fake_peek) as q_mock:
            fake_failed_queue = _ResourceUri(
                "mocked_storage_account1",
                OBJECT_TYPE,
                "queue",
                "mocked_qf_name",
                ENDPOINT_SUFFIX,
            )
            fake_success_queue = _ResourceUri(
                "mocked_storage_account2",
                OBJECT_TYPE,
                "queue",
                "mocked_qs_name",
                ENDPOINT_SUFFIX,
            )

            mocked_get_success_qs.return_value = [fake_success_queue]
            mocked_get_failed_qs.return_value = [fake_failed_queue]

            qs = KustoIngestStatusQueues(client)

            assert qs.success.is_empty() is False
            assert qs.failure.is_empty() is True

            assert q_mock.call_count == 2
            assert q_mock.call_args_list[0][1]["max_messages"] == 2
            assert q_mock.call_args_list[1][1]["max_messages"] == 2

    def test_peek(self):
        client = QueuedIngestClient("some-cluster")

        fake_peek = fake_peek_factory(
            lambda queue_name, num_messages=1: [
                mock_message(success=True) if "qs" in queue_name else mock_message(success=False) for _ in range(0, num_messages)
            ]
        )

        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues") as mocked_get_success_qs, mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs, mock.patch.object(QueueClient, "peek_messages", autospec=True, side_effect=fake_peek) as q_mock:

            fake_failed_queue1 = _ResourceUri(
                "mocked_storage_account_f1",
                OBJECT_TYPE,
                "queue",
                "mocked_qf_1_name",
                ENDPOINT_SUFFIX,
            )
            fake_failed_queue2 = _ResourceUri(
                "mocked_storage_account_f2",
                OBJECT_TYPE,
                "queue",
                "mocked_qf_2_name",
                ENDPOINT_SUFFIX,
            )
            fake_success_queue = _ResourceUri(
                "mocked_storage_account2",
                OBJECT_TYPE,
                "queue",
                "mocked_qs_name",
                ENDPOINT_SUFFIX,
            )

            mocked_get_success_qs.return_value = [fake_success_queue]
            mocked_get_failed_qs.return_value = [fake_failed_queue1, fake_failed_queue2]

            qs = KustoIngestStatusQueues(client)

            peek_success_actual = qs.success.peek()
            peek_failure_actual = qs.failure.peek(6)

            assert len(peek_success_actual) == 1

            for m in peek_failure_actual:
                assert isinstance(m, FailureMessage) is True

            for m in peek_success_actual:
                assert isinstance(m, SuccessMessage) is True

            assert len(peek_failure_actual) == 6

            actual = {}

            assert len(QueueClient.peek_messages.call_args_list) == 3

            for call_args in q_mock.call_args_list:
                actual[call_args[0][0].queue_name] = actual.get(call_args[0][0].queue_name, 0) + call_args[1]["max_messages"]

            assert actual[fake_failed_queue2.object_name] == 4
            assert actual[fake_failed_queue1.object_name] == 4
            assert actual[fake_success_queue.object_name] == 2

    def test_pop(self):
        client = QueuedIngestClient("some-cluster")

        fake_receive = fake_receive_factory(
            lambda queue_name, num_messages=1: [
                mock_message(success=True) if "qs" in queue_name else mock_message(success=False) for _ in range(0, num_messages)
            ]
        )

        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues") as mocked_get_success_qs, mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs, mock.patch.object(
            QueueClient,
            "receive_messages",
            autospec=True,
            side_effect=fake_receive,
        ) as q_receive_mock, mock.patch.object(
            QueueClient, "delete_message", return_value=None
        ) as q_del_mock:

            fake_failed_queue1 = _ResourceUri(
                "mocked_storage_account_f1",
                OBJECT_TYPE,
                "queue",
                "mocked_qf_1_name",
                ENDPOINT_SUFFIX,
            )
            fake_failed_queue2 = _ResourceUri(
                "mocked_storage_account_f2",
                OBJECT_TYPE,
                "queue",
                "mocked_qf_2_name",
                ENDPOINT_SUFFIX,
            )
            fake_success_queue = _ResourceUri(
                "mocked_storage_account2",
                OBJECT_TYPE,
                "queue",
                "mocked_qs_name",
                ENDPOINT_SUFFIX,
            )

            mocked_get_success_qs.return_value = [fake_success_queue]
            mocked_get_failed_qs.return_value = [fake_failed_queue1, fake_failed_queue2]

            qs = KustoIngestStatusQueues(client)

            get_success_actual = qs.success.pop()
            get_failure_actual = qs.failure.pop(6)

            assert len(get_success_actual) == 1
            assert len(get_failure_actual) == 6

            for m in get_failure_actual:
                assert isinstance(m, FailureMessage)

            for m in get_success_actual:
                assert isinstance(m, SuccessMessage)

            assert q_receive_mock.call_count == 3
            assert q_del_mock.call_count == len(get_success_actual) + len(get_failure_actual)

            assert q_receive_mock.call_args_list[0][1]["messages_per_page"] == 2

            actual = {
                q_receive_mock.call_args_list[1][0][0].queue_name: q_receive_mock.call_args_list[1][1]["messages_per_page"],
                q_receive_mock.call_args_list[2][0][0].queue_name: q_receive_mock.call_args_list[2][1]["messages_per_page"],
            }

            assert actual[fake_failed_queue2.object_name] == 4
            assert actual[fake_failed_queue1.object_name] == 4

    def test_pop_unbalanced_queues(self):
        client = QueuedIngestClient("some-cluster")

        fake_receive = fake_receive_factory(
            lambda queue_name, messages_per_page=1: [mock_message(success=False) for _ in range(0, messages_per_page)] if "1" in queue_name else []
        )
        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues"), mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs, mock.patch.object(
            QueueClient,
            "receive_messages",
            autospec=True,
            side_effect=fake_receive,
        ) as q_receive_mock, mock.patch.object(
            QueueClient, "delete_message", return_value=None
        ):

            fake_failed_queue1 = _ResourceUri(
                "mocked_storage_account_f1",
                OBJECT_TYPE,
                "queue",
                "mocked_qf_1_name",
                ENDPOINT_SUFFIX,
            )
            fake_failed_queue2 = _ResourceUri(
                "mocked_storage_account_f2",
                OBJECT_TYPE,
                "queue",
                "mocked_qf_2_name",
                ENDPOINT_SUFFIX,
            )

            mocked_get_failed_qs.return_value = [fake_failed_queue1, fake_failed_queue2]

            qs = KustoIngestStatusQueues(client)

            get_failure_actual = qs.failure.pop(6)

            assert len(get_failure_actual) == 6

            for m in get_failure_actual:
                assert isinstance(m, FailureMessage)

            assert q_receive_mock.call_count == 3

            actual = {}

            for call_args in q_receive_mock.call_args_list:
                actual[call_args[0][0].queue_name] = actual.get(call_args[0][0].queue_name, 0) + call_args[1]["messages_per_page"]

            assert actual[fake_failed_queue2.object_name] + actual[fake_failed_queue1.object_name] == (4 + 4 + 6)

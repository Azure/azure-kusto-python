from azure.kusto.ingest import KustoIngestClient
from azure.kusto.ingest._resource_manager import _ResourceUri
from azure.kusto.ingest.status import KustoIngestStatusQueues, SuccessMessage, FailureMessage
from azure.storage.queue.models import QueueMessage
from uuid import uuid4
import time
import json
import base64
import unittest
import mock


class StatusQTests(unittest.TestCase):
    def test_init(self):
        client = KustoIngestClient("some-cluster")
        qs = KustoIngestStatusQueues(client)

        assert qs.success.message_cls == SuccessMessage
        assert qs.failure.message_cls == FailureMessage

    @mock.patch("azure.storage.queue.QueueService.peek_messages")
    def test_isempty(self, mocked_q_peek_messages):
        client = KustoIngestClient("some-cluster")
        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues") as mocked_get_success_qs, mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs:

            fake_failed_queue = _ResourceUri("mocked_storage_account1", "queue", "mocked_qf_name", "mocked_sas")
            fake_success_queue = _ResourceUri("mocked_storage_account2", "queue", "mocked_qs_name", "mocked_sas")

            mocked_get_success_qs.return_value = [fake_success_queue]
            mocked_get_failed_qs.return_value = [fake_failed_queue]

            mocked_q_peek_messages.side_effect = (
                lambda queue_name, num_messages=1: [] if queue_name == fake_failed_queue.object_name else [QueueMessage() for _ in range(0, num_messages)]
            )

            qs = KustoIngestStatusQueues(client)

            assert qs.success.is_empty() == False
            assert qs.failure.is_empty() == True

            assert mocked_q_peek_messages.call_count == 2
            assert mocked_q_peek_messages.call_args_list[0][0][0] == fake_success_queue.object_name
            assert mocked_q_peek_messages.call_args_list[0][1]["num_messages"] == 2

            assert mocked_q_peek_messages.call_args_list[1][0][0] == fake_failed_queue.object_name
            assert mocked_q_peek_messages.call_args_list[1][1]["num_messages"] == 2

    @mock.patch("azure.storage.queue.QueueService.peek_messages")
    def test_peek(self, mocked_q_peek_messages):
        client = KustoIngestClient("some-cluster")
        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues") as mocked_get_success_qs, mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs:

            fake_failed_queue1 = _ResourceUri("mocked_storage_account_f1", "queue", "mocked_qf_1_name", "mocked_sas")
            fake_failed_queue2 = _ResourceUri("mocked_storage_account_f2", "queue", "mocked_qf_2_name", "mocked_sas")
            fake_success_queue = _ResourceUri("mocked_storage_account2", "queue", "mocked_qs_name", "mocked_sas")

            mocked_get_success_qs.return_value = [fake_success_queue]
            mocked_get_failed_qs.return_value = [fake_failed_queue1, fake_failed_queue2]

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

                m.content = str(base64.b64encode(json.dumps(content).encode("utf-8")).decode("utf-8"))
                m.pop_receipt = None
                m.time_next_visible = None

                return m

            mocked_q_peek_messages.side_effect = lambda queue_name, num_messages=1: [
                mock_message(success=True) if queue_name in [fake_success_queue.object_name] else mock_message(success=False) for i in range(0, num_messages)
            ]

            qs = KustoIngestStatusQueues(client)

            peek_success_actual = qs.success.peek()
            peek_failure_actual = qs.failure.peek(6)

            assert len(peek_success_actual) == 1

            for m in peek_failure_actual:
                assert isinstance(m, FailureMessage) == True

            for m in peek_success_actual:
                assert isinstance(m, SuccessMessage) == True

            assert len(peek_failure_actual) == 6

            actual = {}

            assert len(mocked_q_peek_messages.call_args_list) == 3

            for call_args in mocked_q_peek_messages.call_args_list:
                actual[call_args[0][0]] = actual.get(call_args[0][0], 0) + call_args[1]["num_messages"]

            assert actual[fake_failed_queue2.object_name] == 4
            assert actual[fake_failed_queue1.object_name] == 4
            assert actual[fake_success_queue.object_name] == 2

    @mock.patch("azure.storage.queue.QueueService.get_messages")
    @mock.patch("azure.storage.queue.QueueService.delete_message")
    def test_pop(self, mocked_q_del_messages, mocked_q_get_messages):
        client = KustoIngestClient("some-cluster")
        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues") as mocked_get_success_qs, mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs:

            fake_failed_queue1 = _ResourceUri("mocked_storage_account_f1", "queue", "mocked_qf_1_name", "mocked_sas")
            fake_failed_queue2 = _ResourceUri("mocked_storage_account_f2", "queue", "mocked_qf_2_name", "mocked_sas")
            fake_success_queue = _ResourceUri("mocked_storage_account2", "queue", "mocked_qs_name", "mocked_sas")

            mocked_get_success_qs.return_value = [fake_success_queue]
            mocked_get_failed_qs.return_value = [fake_failed_queue1, fake_failed_queue2]

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

                m.content = str(base64.b64encode(json.dumps(content).encode("utf-8")).decode("utf-8"))
                m.pop_receipt = None
                m.time_next_visible = None

                return m

            mocked_q_get_messages.side_effect = lambda queue_name, num_messages=1: [
                mock_message(success=True) if queue_name in [fake_success_queue.object_name] else mock_message(success=False) for i in range(0, num_messages)
            ]

            mocked_q_del_messages.return_value = None

            qs = KustoIngestStatusQueues(client)

            get_success_actual = qs.success.pop()
            get_failure_actual = qs.failure.pop(6)

            assert len(get_success_actual) == 1
            assert len(get_failure_actual) == 6

            for m in get_failure_actual:
                assert isinstance(m, FailureMessage) == True

            for m in get_success_actual:
                assert isinstance(m, SuccessMessage) == True

            assert mocked_q_get_messages.call_count == 3
            assert mocked_q_del_messages.call_count == len(get_success_actual) + len(get_failure_actual)

            assert mocked_q_get_messages.call_args_list[0][0][0] == fake_success_queue.object_name
            assert mocked_q_get_messages.call_args_list[0][1]["num_messages"] == 2

            actual = {
                mocked_q_get_messages.call_args_list[1][0][0]: mocked_q_get_messages.call_args_list[1][1]["num_messages"],
                mocked_q_get_messages.call_args_list[2][0][0]: mocked_q_get_messages.call_args_list[2][1]["num_messages"],
            }

            assert actual[fake_failed_queue2.object_name] == 4
            assert actual[fake_failed_queue1.object_name] == 4

    @mock.patch("azure.storage.queue.QueueService.get_messages")
    @mock.patch("azure.storage.queue.QueueService.delete_message")
    def test_pop_unbalanced_queues(self, mocked_q_del_messages, mocked_q_get_messages):
        client = KustoIngestClient("some-cluster")
        with mock.patch.object(client._resource_manager, "get_successful_ingestions_queues") as mocked_get_success_qs, mock.patch.object(
            client._resource_manager, "get_failed_ingestions_queues"
        ) as mocked_get_failed_qs:

            fake_failed_queue1 = _ResourceUri("mocked_storage_account_f1", "queue", "mocked_qf_1_name", "mocked_sas")
            fake_failed_queue2 = _ResourceUri("mocked_storage_account_f2", "queue", "mocked_qf_2_name", "mocked_sas")

            mocked_get_failed_qs.return_value = [fake_failed_queue1, fake_failed_queue2]

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

                m.content = str(base64.b64encode(json.dumps(content).encode("utf-8")).decode("utf-8"))
                m.pop_receipt = None
                m.time_next_visible = None

                return m

            mocked_q_get_messages.side_effect = (
                lambda queue_name, num_messages=1: [mock_message(success=True) for i in range(0, num_messages)]
                if queue_name == fake_failed_queue1.object_name
                else []
            )

            mocked_q_del_messages.return_value = None

            qs = KustoIngestStatusQueues(client)

            get_failure_actual = qs.failure.pop(6)

            assert len(get_failure_actual) == 6

            for m in get_failure_actual:
                assert isinstance(m, FailureMessage) == True

            assert mocked_q_get_messages.call_count == 3

            actual = {}

            for call_args in mocked_q_get_messages.call_args_list:
                actual[call_args[0][0]] = actual.get(call_args[0][0], 0) + call_args[1]["num_messages"]

            assert actual[fake_failed_queue2.object_name] + actual[fake_failed_queue1.object_name] == (4 + 4 + 6)

"""Status Queue logic to wrap over Azure Storage Queues"""
import base64
import random
from azure.storage.common import CloudStorageAccount


class QueueDetails(object):
    def __init__(self, name, service):
        self.name = name
        self.service = service

    def __str__(self):
        return "QueueDetails({0.name})".format(self)


class StatusQueue(object):
    """StatusQueue is a class to simplify access to Kusto status queues (backed by azure storage queues).
    """

    def __init__(self, get_queues_func, message_cls):
        self.get_queues_func = get_queues_func
        self.message_cls = message_cls

    def _get_q_services(self):
        return [
            QueueDetails(
                name=queue_details.object_name,
                service=CloudStorageAccount(
                    account_name=queue_details.storage_account_name, sas_token=queue_details.sas
                ).create_queue_service(),
            )
            for queue_details in self.get_queues_func()
        ]

    def is_empty(self):
        """Checks if Status queue has any messages        
        """
        return not self.peek(1, raw=True)

    def _decode_content(self, content):
        return base64.b64decode(content).decode("utf-8")

    def _deserialize_message(self, m):
        """Deserialize a message and return at as `message_cls`
        :param m: original message m.
        """
        return self.message_cls(self._decode_content(m.content))

    # TODO: current implementation takes a union top n /  len(queues), which is not ideal,
    # because the user is not supposed to know that there can be multiple underlying queues
    def peek(self, n=1, raw=False):
        """Peek status queue
        :param int n: number of messages to return as part of peek.
        :param bool raw: should message content be returned as is (no parsing).        
        """

        def _peek_specific_q(_q, _n):
            has_messages = False
            for m in _q.service.peek_messages(_q.name, num_messages=_n):
                if m is not None:
                    has_messages = True
                    result.append(m if raw else self._deserialize_message(m))

                    # short circut to prevent unneeded work
                    if len(result) == n:
                        return True
            return has_messages

        q_services = self._get_q_services()
        random.shuffle(q_services)

        per_q = int(n / len(q_services)) + 1

        result = []

        non_empty_qs = []

        for q in q_services:
            if _peek_specific_q(q, per_q):
                non_empty_qs.append(q)

            if len(result) == n:
                return result

        # in-case queues aren't balanced, and we didn't get enough messages, iterate again and this time get all that we can
        for q in non_empty_qs:
            _peek_specific_q(q, n)
            if len(result) == n:
                return result

        # because we ask for n / len(qs) + 1, we might get more message then requests
        return result

    # TODO: current implementation takes a union top n /  len(queues), which is not ideal,
    # because the user is not supposed to know that there can be multiple underlying queues
    def pop(self, n=1, raw=False, delete=True):
        """Pop status queue
        :param int n: number of messages to return as part of peek.
        :param bool raw: should message content be returned as is (no parsing).
        :param bool delete: should message be deleted after pop. default is True as this is expected of a q.
        """

        def _pop_specific_q(_q, _n):
            has_messages = False
            for m in _q.service.get_messages(_q.name, num_messages=_n):
                if m is not None:
                    has_messages = True
                    result.append(m if raw else self._deserialize_message(m))
                    if delete:
                        _q.service.delete_message(_q.name, m.id, m.pop_receipt)

                    # short circut to prevent unneeded work
                    if len(result) == n:
                        return True
            return has_messages

        q_services = self._get_q_services()
        random.shuffle(q_services)

        per_q = int(n / len(q_services)) + 1

        result = []

        non_empty_qs = []

        for q in q_services:
            if _pop_specific_q(q, per_q):
                non_empty_qs.append(q)

            if len(result) == n:
                return result

        # in-case queues aren't balanced, and we didn't get enough messages, iterate again and this time get all that we can
        for q in non_empty_qs:
            _pop_specific_q(q, n)
            if len(result) == n:
                return result

        # because we ask for n / len(qs) + 1, we might get more message then requests
        return result

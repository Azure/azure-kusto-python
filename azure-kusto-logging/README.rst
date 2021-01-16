Microsoft Azure Kusto Logging for Python
========================================

Overview
--------

Azure kusto logging is a handler which bufferize traces then write them in Kusto. 

Logging is done through the standard python `logging module <https://docs.python.org/3/library/logging.html>`_. with the exception of formatters, which makes little sense in a context where the traces will be queried on the attribute values.

Initialization is done the usual way, with a connection string KustoConnectionStringBuilder

.. code-block:: python

    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    cluster = "<insert here your cluster name>"
    client_id = "<insert here your AAD application id>"
    client_secret = "<insert here your AAD application key>"
    authority_id = "<insert here your AAD tenant id>"

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)
    client = KustoClient(kcsb)



Patterns
--------


The handler can be used by itself, it behaves the same as a `memory handler <https://docs.python.org/3/library/logging.handlers.html?highlight=memoryhandler#logging.handlers.MemoryHandler>`_.

The useStreaming flag is set if the Kusto streaming interface is used (the kcsb should be consistent)
capacity is the max number of records kept in memory 
flushLevel is the minimal level where the buffer will be flushed forcibly, even if not full.
retries is an array containing the waiting times between retries (default: 5s then 30s then 60s)

although the handler works with a logging.DEBUG level, it's not advisable to do so: Some traces are skipped to avoid reentrency issues, and some debug traces from the underlying libraries can appear.

.. code-block:: python

    kh = KustoHandler(kcsb=kcsb, database=test_db, table=test_table, useStreaming=True, capacity=50000, flushLevel=logging.CRITICAL)
    kh.setLevel(logging.INFO)
    logging.getLogger().addHandler(kh)
    logging.getLogger().setLevel(logging.INFO)


The most appropriate use is to leverage the queue handler / queue listener. As the Kusto ingestion mechanism in not thread-safe, the queue listener will ensure that only a single thread will be used.

queues will also ensure that the performance of the caller is not blocked waiting when the buffer is written over the network.


.. code-block:: python

    kh = KustoHandler(kcsb=kcsb, database=test_db, table=test_table, useStreaming=True, capacity=50000, flushLevel=logging.CRITICAL)
    kh.setLevel(logging.INFO)

    q = Queue()
    qh = QueueHandler(q)

    ql = QueueListener(q, kh)
    ql.start()

    logger = logging.getLogger()
    logger.addHandler(qh)
    logger.setLevel(logging.INFO)



* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Kusto query sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.

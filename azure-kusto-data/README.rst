Microsoft Azure Kusto Library for Python
========================================

Overview
--------

.. code-block:: python

    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    cluster = "<insert here your cluster name>"
    client_id = "<insert here your AAD application id>"
    client_secret = "<insert here your AAD application key>"
    authority_id = "<insert here your AAD tenant id>"

    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)
    # It is a good practice to re-use the KustoClient instance, as it maintains a pool of connections to the Kusto service.
    # This sample shows how to create a client and close it in the same scope, for demonstration purposes.
    with KustoClient(kcsb) as client:
        db = "Samples"
        query = "StormEvents | take 10"

        response = client.execute(db, query)
        for row in response.primary_results[0]:
            print(row[0], " ", row["EventType"])



*Kusto Python Client* Library provides the capability to query Kusto clusters using Python.
It is Python 3.x compatible and supports
all data types through familiar Python DB API interface.

It's possible to use the library, for instance, from `Jupyter Notebooks
<http://jupyter.org/>`_.
which are attached to Spark clusters,
including, but not exclusively, `Azure Databricks
<https://azure.microsoft.com/en-us/services/databricks/>`_. instances.

Async Client
~~~~~~~~~~~~
Kusto now provides an asynchronous client for queries.

To use the client, first install the package with the "aio" extra:

.. code:: bash

    pip install azure-kusto-data[aio]

The async client uses exact same interface as the regular client, except
that it lives in the ``azure.kusto.data.aio`` namespace, and it returns
``Futures`` you will need to ``await`` its

.. code:: python

    from azure.kusto.data import KustoConnectionStringBuilder
    from azure.kusto.data.aio import KustoClient

    cluster = "<insert here your cluster name>"
    client_id = "<insert here your AAD application id>"
    client_secret = "<insert here your AAD application key>"
    authority_id = "<insert here your AAD tenant id>"


    async def sample():
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, authority_id)
        async with KustoClient(kcsb) as client:
            db = "Samples"
            query = "StormEvents | take 10"

            response = await client.execute(db, query)
            for row in response.primary_results[0]:
                print(row[0], " ", row["EventType"])

Links
~~~~~

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Kusto query sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-data/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.

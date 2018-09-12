Microsoft Azure Kusto Ingest Library for Python
===============================================

.. code-block:: python

    from azure.kusto.data.request import KustoConnectionStringBuilder
    from azure.kusto.ingest import KustoIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat

    ingestion_props = IngestionProperties(database="{database_name}", table="{table_name}", dataFormat=DataFormat.csv)
    client = KustoIngestClient("https://ingest-{cluster_name}.kusto.windows.net")

    file_descriptor = FileDescriptor("{filename}.csv", 3333)  # 3333 is the raw size of the data in bytes.
    client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)
    client.ingest_from_file("{filename}.csv", ingestion_properties=ingestion_props)

    blob_descriptor = BlobDescriptor("https://{path_to_blob}.csv.gz?sas", 10)  # 10 is the raw size of the data in bytes.
    client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)
    

Overview
--------

*Kusto Python Ingest Client* Library provides the capability to ingest data into Kusto clusters using Python.
It is Python 2.x/3.x compatible and supports data types through familiar Python DB API interface.

It's possible to use the library, for instance, from `Jupyter Notebooks <http://jupyter.org/>`_ which are attached to Spark clusters,
including, but not exclusively, `Azure Databricks <https://azure.microsoft.com/en-us/services/databricks>`_ instances.

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Data ingest sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.

.. image:: https://travis-ci.org/Azure/azure-kusto-python.svg
    :target: https://travis-ci.org/Azure/azure-kusto-python

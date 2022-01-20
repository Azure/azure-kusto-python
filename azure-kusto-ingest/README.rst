Microsoft Azure Kusto Ingest Library for Python
===============================================

.. code-block:: python

    from azure.kusto.data import KustoConnectionStringBuilder, DataFormat
    from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor

    ingestion_props = IngestionProperties(database="{database_name}", table="{table_name}", data_format=DataFormat.CSV)
    client = QueuedIngestClient(KustoConnectionStringBuilder.with_interactive_login("https://ingest-{cluster_name}.kusto.windows.net"))

    file_descriptor = FileDescriptor("{filename}.csv", 15360)  # in this example, the raw (uncompressed) size of the data is 15KB (15360 bytes)
    client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)
    client.ingest_from_file("{filename}.csv", ingestion_properties=ingestion_props)

    blob_descriptor = BlobDescriptor("https://{path_to_blob}.csv.gz?sas", 51200)  # in this example, the raw (uncompressed) size of the data is 50KB (52100 bytes)
    client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)
    

Overview
--------

*Kusto Python Ingest Client* Library provides the capability to ingest data into Kusto clusters using Python.
It is Python 3.x compatible and supports data types through familiar Python DB API interface.

It's possible to use the library, for instance, from `Jupyter Notebooks <http://jupyter.org/>`_ which are attached to Spark clusters,
including, but not exclusively, `Azure Databricks <https://azure.microsoft.com/en-us/services/databricks>`_ instances.

* `How to install the package <https://github.com/Azure/azure-kusto-python#install>`_.

* `Data ingest sample <https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py>`_.

* `GitHub Repository <https://github.com/Azure/azure-kusto-python/tree/master/azure-kusto-data>`_.

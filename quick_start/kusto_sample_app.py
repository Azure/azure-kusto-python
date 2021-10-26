# Todo - README:
#  1) Run: pip install azure-kusto-data azure-kusto-ingest
#  2) Fill in or edit the sections commented as 'To Do'
#  3) Run the script
#  4) Read additional comments for tips and reference material


import json
import os
import time
import typing

from azure.kusto.data.exceptions import KustoClientError, KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

from azure.kusto.ingest import (
    QueuedIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    StreamDescriptor,
    DataFormat,
    ReportLevel,
    IngestionMappingType,
    KustoStreamingIngestClient,
)

# Todo - Config:
#  If the sample was download from OneClick it, kusto_sample_config.json should be pre-populated with your cluster's details:
#  If the sample was taken from GitHub, open kusto_sample_config.json and edit the cluster name and database to point to your cluster.
configFileName = "kusto_sample_config.json"

# Todo - Config (Optional): Change the authentication method from Device Code (User Prompt) to one of the other options
#  Some of the auth modes require additional environment variables to be set in order to work (check the use below)
#  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
authenticationMode = "UserPrompt"  # choose between: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)
waitForUser = True


def main():
    print("Kusto sample app is starting...")
    config = read_config(configFileName)

    kusto_uri = config["KustoUri"]
    ingest_uri = config["IngestUri"]
    database_name = config["DatabaseName"]
    table_name = config["TableName"]
    use_existing_table = str(config["UseExistingTable"]).lower()

    if authenticationMode == "UserPrompt":
        print("")
        print("You will be prompted for credentials during this script.")
        print("Please, return to the console after authenticating.")
        wait_for_user()

    # Tip: Avoid creating a new Kusto Client for each use. Instead, create the clients once and use them as long as possible.
    kusto_connection_string = create_connection_string(kusto_uri, authenticationMode)
    ingest_connection_string = create_connection_string(ingest_uri, authenticationMode)
    kusto_client = KustoClient(kusto_connection_string)
    ingest_client = QueuedIngestClient(ingest_connection_string)

    if use_existing_table == "false":
        table_schema = config["TableSchema"]
        print("")
        print(f"Creating table '{database_name}.{table_name}' if it does not exist:")
        # Tip: This is commonly a one-time configuration to make
        # Learn More: For additional information on how to create tables see: https://docs.microsoft.com/azure/data-explorer/one-click-table
        command = f".create table {table_name} {table_schema}"
        if not run_control_command(kusto_client, database_name, command):
            print("Failed to create or validate table exists.")
            exit(-1)

        wait_for_user()

    print("")
    print(f"Altering the batching policy for '{table_name}'")
    # Tip 1: This is an optional step. Most users should be fine with the defaults.
    # Tip 2: This is a one-time configuration to make, no actual need to repeat it.
    batching_policy = '{ "MaximumBatchingTimeSpan": "00:00:10", "MaximumNumberOfItems": 500, "MaximumRawDataSizeMB": 1024 }'
    command = f".alter table {table_name} policy ingestionbatching @'{batching_policy}'"
    if not run_control_command(kusto_client, database_name, command):
        print("Failed to alter the ingestion policy!")
        print("This could be the result of insufficient permissions.")
        print("The sample will still run, though ingestion will be delayed for 5 minutes")

    # Learn More:
    #  Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
    #   1) more then a 1000 files were queued for ingestion for the same table by the same user
    #   2) more then 1GB of data was queued for ingestion for the same table by the same user
    #   3) More then 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
    #  In order to speed up this sample app, we attempt to modify the default ingestion policy to ingest data after 10 seconds have passed
    #  For additional information on customizing the ingestion batching policy see:
    #   https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy
    #  You may also skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient
    wait_for_user()

    # Learn More: For additional information on Kusto Query Language see: https://docs.microsoft.com/azure/data-explorer/write-queries
    print("")
    print(f"Initial row count for '{database_name}.{table_name}' is:")
    run_query(kusto_client, database_name, f"{table_name} | summarize count()")
    wait_for_user()

    files = config["Data"]
    for file in files:
        source_type = str(file["SourceType"]).lower()
        uri = file["DataSourceUri"]
        data_format = str_to_data_format(str(file["DataFormat"]))
        use_existing_mapping = str(file["UseExistingMapping"]).lower()
        mapping_name = file["MappingName"]
        mapping_value = file["MappingValue"]

        # Learn More: For additional information on how to ingest data to Kusto in Python see:
        #  https://docs.microsoft.com/azure/data-explorer/python-ingest-data
        if data_format in {DataFormat.JSON, DataFormat.MULTIJSON, DataFormat.SINGLEJSON}:
            if use_existing_mapping == "false":
                print("")
                print(f"Attempting to create a json mapping reference named '{mapping_name}'")
                # Tip: This is commonly a one-time configuration to make
                mapping_command = f".create-or-alter table {table_name} ingestion json mapping '{mapping_name}' '{mapping_value}'"
                mapping_exists = run_control_command(kusto_client, database_name, mapping_command)
                if not mapping_exists:
                    print(f"failed to create a json  mapping reference named {mapping_name}")
                    print(f"skipping json ingestion")
                    continue

                # learn More: For more information about providing inline mappings or mapping references see:
                #  https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings

            print("")
            print(f"Attempting to ingest '{uri}' from {source_type}")
            # Tip: When ingesting json files, if a each row is represented by a single line json, use MULTIJSON format even if the file only includes one line.
            # When the json contains whitespace formatting, use SINGLEJSON. In this case only one data row json object per file is allowed.
            data_format = DataFormat.MULTIJSON if data_format == data_format.JSON else data_format
            if source_type == "file":
                ingest_data_from_file(ingest_client, database_name, table_name, uri, data_format, mapping_name)
            else:  # assume source is a blob
                ingest_data_from_blob(ingest_client, database_name, table_name, uri, data_format, mapping_name)

            wait_for_user()

        else:  # file is not in any json format
            print("")
            print(f"Attempting to ingest '{uri}' from {source_type}")
            if source_type == "file":
                ingest_data_from_file(ingest_client, database_name, table_name, uri, data_format)
            else:  # assume source is a blob
                ingest_data_from_blob(ingest_client, database_name, table_name, uri, data_format)

            wait_for_user()

    print("")
    print("Sleeping for a few seconds to make sure queued ingestion has completed")
    print("Mind, this may take longer depending on the file size and ingestion policy")
    for x in range(20, 0, -1):
        print(f"{x} ", end="\r")
        time.sleep(1)

    print("")
    print(f"Post ingestion row count for '{database_name}.{table_name}' is:")
    run_query(kusto_client, database_name, f"{table_name} | summarize count()")

    print("")
    print(f"Post ingestion sample data query:")
    run_query(kusto_client, database_name, f"{table_name} | take 2")


def run_control_command(client: KustoClient, db: str, command: str) -> bool:
    try:
        res = client.execute_mgmt(db, command)
        print("")
        print("Response:")
        for row in res.primary_results[0]:
            print(row.to_list())

        return True

    except KustoClientError as ex:
        print(f"Client error while trying to execute command '{command}' on database '{db}'")
        print(ex)

    except KustoServiceError as ex:
        print(f"Server error while trying to execute command '{command}' on database '{db}'")
        print(ex)

    except Exception as ex:
        print(f"Unknown error while trying to execute command '{command}' on database '{db}'")
        print(ex)

    return False


def run_query(client: KustoClient, db: str, query: str):
    try:
        res = client.execute_query(db, query)
        for row in res.primary_results[0]:
            print(row.to_list())

        return True

    except KustoClientError as ex:
        print(f"Client error while trying to execute query '{query}' on database '{db}'")
        print(ex)

    except KustoServiceError as ex:
        print(f"Server error while trying to execute query '{query}' on database '{db}'")
        print(ex)

    except Exception as ex:
        print(f"Unknown error while trying to execute query '{query}' on database '{db}'")
        print(ex)

    return False


def ingest_data_from_file(client: QueuedIngestClient, db: str, table: str, file_path: str, file_format: DataFormat, mapping_ref: str = None):
    ingestion_props = IngestionProperties(
        database=f"{db}",
        table=f"{table}",
        ingestion_mapping_reference=mapping_ref,
        # Learn More: For additional information about supported data formats, see
        #  https://docs.microsoft.com/azure/data-explorer/ingestion-supported-formats
        data_format=file_format,
        # Todo - Config: Setting the ingestion batching policy takes up to 5 minutes to have an effect.
        #  For the sake of the sample we set Flush-Immediately, but in practice it should not be commonly used.
        #  Comment the below line after running the sample for the first few times!
        flush_immediately=True,
    )

    # Tip: For optimal ingestion batching it's best to specify the uncompressed data size in the file descriptor
    file_descriptor = FileDescriptor(file_path)
    client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)

    # Tip: Kusto can also ingest data from open streams and pandas dataframes.
    #  See the python SDK azure.kusto.ingest samples for additional references.


def ingest_data_from_blob(client: QueuedIngestClient, db: str, table: str, blob_path: str, file_format: DataFormat, mapping_ref: str = None):
    ingestion_props = IngestionProperties(
        database=f"{db}",
        table=f"{table}",
        ingestion_mapping_reference=mapping_ref,
        # Learn More: For additional information about supported data formats, see
        #  https://docs.microsoft.com/azure/data-explorer/ingestion-supported-formats
        data_format=file_format,
        # Todo - Config: Setting the ingestion batching policy takes up to 5 minutes to have an effect.
        #  For the sake of the sample we set Flush-Immediately, but in practice it should not be commonly used.
        #  Comment the below line after running the sample for the first few times!
        flush_immediately=True,
    )

    # Tip: For optimal ingestion batching it's best to specify the uncompressed data size in the file descriptor
    blob_descriptor = BlobDescriptor(blob_path)
    client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_props)

    # Tip: Kusto can also ingest data from open streams and pandas dataframes.
    #  See the python SDK azure.kusto.ingest samples for additional references.


def create_connection_string(cluster: str, auth_mode: str) -> KustoConnectionStringBuilder:
    # Learn More: For additional information on how to authorize users and apps on Kusto Database see:
    #  https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
    if auth_mode == "UserPrompt":
        return create_interactive_auth_connection_string(cluster)

    elif auth_mode == "ManagedIdentity":
        return create_managed_identity_connection_string(cluster)

    elif auth_mode == "AppKey":
        return create_app_key_connection_string(cluster)

    elif auth_mode == "AppCertificate":
        return create_app_cert_connection_string(cluster)

    else:
        die(f"Unexpected Auth mode: '{auth_mode}'")


def create_interactive_auth_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # prompt user for credentials with device code auth
    return KustoConnectionStringBuilder.with_interactive_login(cluster)


def create_managed_identity_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # Connect using the system or user provided managed identity (azure service only)
    # Todo - Config (Optional): Managed identity client id if you are using a User Assigned Managed Id
    client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")

    if client_id is None:
        return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster)
    else:
        return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster, client_id=client_id)


def create_app_key_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # Todo - Config (Optional): App Id & tenant, and App Key to authenticate with
    app_id = os.environ.get("APP_ID")
    app_tenant = os.environ.get("APP_TENANT")
    app_key = os.environ.get("APP_KEY")

    # Learn More: For information on how to procure an AAD Application in Azure see:
    #  https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
    return KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, app_id, app_key, app_tenant)


def create_app_cert_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # Todo - Config (Optional): App Id & tenant, and certificate to authenticate with
    app_id = os.environ.get("APP_ID")
    app_tenant = os.environ.get("APP_TENANT")
    pem_file_path = os.environ.get("PEM_FILE_PATH")
    thumbprint = os.environ.get("CERT_THUMBPRINT")
    public_cert_path = os.environ.get("PUBLIC_CERT_FILE_PATH")  # Only used on Subject Name and Issuer Auth
    public_certificate = None
    pem_certificate = None

    # Learn More: For information on how to procure an AAD Application in Azure see:
    #  https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
    try:
        with open(pem_file_path, "r") as pem_file:
            pem_certificate = pem_file.read()
    except Exception as ex:
        die(f"Failed to load PEM file from {pem_file_path}", ex)

    if public_cert_path is None:
        try:
            with open(public_cert_path, "r") as cert_file:
                public_certificate = cert_file.read()
        except Exception as ex:
            die(f"Failed to load public certificate file from {public_cert_path}", ex)

        return KustoConnectionStringBuilder.with_aad_application_certificate_sni_authentication(
            cluster, app_id, pem_certificate, public_certificate, thumbprint, app_tenant
        )
    else:
        return KustoConnectionStringBuilder.with_aad_application_certificate_authentication(cluster, app_id, pem_certificate, thumbprint, app_tenant)


def read_config(config_file: str) -> dict:
    try:
        with open(config_file, "r") as config_file:
            config = json.load(config_file)
            return config
    except Exception as ex:
        die(f"Failed to load config file from {config_file}", ex)


def die(error: str, ex: Exception = None):
    print("")
    print("Script failed:")
    print(error)
    if ex is not None:
        print(ex)

    exit(-1)


def wait_for_user():
    if waitForUser:
        input("Press Enter to continue...")


def str_to_data_format(format_str: str) -> DataFormat:
    format_str = format_str.lower()

    if format_str == "csv":
        return DataFormat.CSV
    elif format_str == "tsv":
        return DataFormat.TSV
    elif format_str == "scsv":
        return DataFormat.SCSV
    elif format_str == "sohsv":
        return DataFormat.SOHSV
    elif format_str == "psv":
        return DataFormat.PSV
    elif format_str == "txt":
        return DataFormat.TXT
    elif format_str == "json":
        return DataFormat.JSON
    elif format_str == "singlejson":
        return DataFormat.SINGLEJSON
    elif format_str == "avro":
        return DataFormat.AVRO
    elif format_str == "parquet":
        return DataFormat.PARQUET
    elif format_str == "multijson":
        return DataFormat.MULTIJSON
    elif format_str == "orc":
        return DataFormat.ORC
    elif format_str == "tsve":
        return DataFormat.TSVE
    elif format_str == "raw":
        return DataFormat.RAW
    elif format_str == "w3clogfile":
        return DataFormat.W3CLOGFILE
    else:
        die(fr"Unexpected data format {format_str}")


if __name__ == "__main__":
    main()

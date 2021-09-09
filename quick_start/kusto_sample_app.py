# Todo - Start Here:
#  1) Run setup.bat to install necessary dependencies.
#  2) Follow the To-Do comments for instructions, tips and reference material
#  3) run the script

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

# Todo - Config (Auto-Filled properties):
kustoUri = "https://yogiladadx.westeurope.dev.kusto.windows.net"
ingestUri = "https://ingest-yogiladadx.westeurope.dev.kusto.windows.net"
databaseName = "e2e"
tableName = "SampleTable"
tableSchema = "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, " \
              "xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, " \
              "xdynamicWithNulls:dynamic)"
tableMappingRef = "SampleTable_schema"
# Todo - Learn More: For additional information about supported data formats, see
#  https://docs.microsoft.com/en-us/azure/data-explorer/ingestion-supported-formats
fileFormat = DataFormat.CSV

# Todo - Config (Optional): Change the authentication method from Device Code (User Prompt) to one of the other options
#  Some of the auth modes require additional environment variables to be set in order to work (check the use below)
#  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
authenticationMode = "deviceCode"  # choose between: (deviceCode|managedIdentity|AppKey|AppCertificate)


def main():
    print("kusto sample app is starting")

    # Todo - Learn More: For additional information on how to authorize users and apps on Kusto Database see:
    #  https://docs.microsoft.com/en-us/azure/data-explorer/manage-database-permissions
    if authenticationMode == "deviceCode":
        print("During the run you will be prompted by the browser to enter a device code.")
        print("Return to the console of the sample app and copy the code to provide from it.")
        input("Press enter to continue...")

    # Todo - Tip: Avoid creating a new Kusto Client for each use.
    #  Create the clients once and use them as long as possible.
    kusto_connection_string = create_connection_string(kustoUri, authenticationMode)
    ingest_connection_string = create_connection_string(ingestUri, authenticationMode)
    kusto_client = KustoClient(kusto_connection_string)
    ingest_client = QueuedIngestClient(ingest_connection_string)

    # Todo - Learn More: For additional information on how to create tables see: https://docs.microsoft.com/en-us/azure/data-explorer/one-click-table
    print("")
    print(f"Creating table '{databaseName}.{tableName}' if needed:")
    # Todo (Yochai) what's the table creation command
    command = f".create table {tableName} {tableSchema}"
    if not run_control_command(kusto_client, databaseName, command):
        print("Failed to create or validate table exists.")
        exit(-1)

    # Todo - Learn More: For additional information on Kusto Query Language see: https://docs.microsoft.com/en-us/azure/data-explorer/write-queries
    print("")
    print(f"Initial row count for '{databaseName}.{tableName}' is:")
    run_query(kusto_client, databaseName, f"{tableName} | summarize count()")

    # Todo - Learn More:
    #  Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
    #   1) more then a 1000 files were queued for ingestion for the same table by the same user
    #   2) more then 1GB of data was queued for ingestion for the same table by the same user
    #   3) More then 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
    #  In order to speed up this sample app, we attempt to modify the default ingestion policy to ingest data after 10 seconds have passed
    #  For additional information on customizing the ingestion batching policy see:
    #   https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/batchingpolicy
    #  You may also skip the batching for some files using the FlushImmediatly property, though this option should be used with care as it is inefficient
    print("")
    print(f"Altering the batching policy for '{tableName}'")
    batching_policy = '{ "MaximumBatchingTimeSpan": "00:00:10", "MaximumNumberOfItems": 500, "MaximumRawDataSizeMB": 1024 }'
    command = f".alter table {tableName} policy ingestionbatching @'{batching_policy}'"
    if not run_control_command(kusto_client, databaseName, command):
        print("Failed to alter the ingestion policy!")
        print("This could be the result of insufficient permissions.")
        print("The sample will still run, though ingestion will be delayed for 5 minutes")

    # Todo - Learn More: For additional information on how to ingest data to Kusto in Python see:
    #  https://docs.microsoft.com/en-us/azure/data-explorer/python-ingest-data
    print("")
    ingest_file = input("Please enter a directory of files to ingest from:")
    print(f"Attempting to ingest '{ingest_file}'")
    ingest_data_from_file(ingest_client, databaseName, tableName, ingest_file)

    print("")
    print("Sleeping for a few seconds to make sure queued ingestion has completed")
    print("Mind, this may take longer dependeing on the file size and ingestion policy")
    time.sleep(20)

    print("")
    print(f"Post ingestion row count for '{databaseName}.{tableName}' is:")
    run_query(kusto_client, databaseName, f"{tableName} | summarize count()")


def run_control_command(client: KustoClient, db: str, command: str) -> bool:
    try:
        res = client.execute_mgmt(db, command)
        for row in res.primary_results[0]:
            print(row.to_list())

        return True

    except KustoClientError as ex:
        die(f"Client error while trying to execute command '{command}' on database '{db}'", ex)

    except KustoServiceError as ex:
        die(f"Server error while trying to execute command '{command}' on database '{db}'", ex)

    except Exception as ex:
        die(f"Unknown error while trying to execute command '{command}' on database '{db}'", ex)

    return False


def run_query(client: KustoClient, db: str, query: str):
    try:
        res = client.execute_query(db, query)
        for row in res.primary_results[0]:
            print(row.to_list())

        return True

    except KustoClientError as ex:
        die(f"Client error while trying to execute query '{query}' on database '{db}'", ex)

    except KustoServiceError as ex:
        die(f"Server error while trying to execute query '{query}' on database '{db}'", ex)

    except Exception as ex:
        die(f"Unknown error while trying to execute query '{query}' on database '{db}'", ex)

    return False


def ingest_data_from_file(client: QueuedIngestClient, db: str, table: str, file_path: str):
    ingestion_props = IngestionProperties(
        database=f"{databaseName}",
        table=f"{tableName}",
        data_format=fileFormat,

        # Todo - Configure: Setting the ingestion batching policy takes up to 5 minutes to have an effect.
        #  For the sake of the sample we set Flush-Immediately, but in practice it should not be commonly used.
        #  Comment the below line after running the sample for the first few times!
        flush_immediately=True,

        # Todo - Tip: in case a mapping is required provide either a mapping or a mapping reference for a pre-configured mapping
        # ingestion_mapping_type=IngestionMappingType.JSON
        # ingestion_mapping=
        # ingestion_mapping_reference="{json_mapping_that_already_exists_on_table}"
    )

    # Todo - Tip: for optimal ingestion batching it's best to specify the uncompressed data size in the file descriptor
    file_descriptor = FileDescriptor(f"{file_path}")
    client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)

    # Todo - Tip: Kusto can also ingest data from blobs, open streams and pandas dataframes.
    #  See the python SDK azure.kusto.ingest samples for additional references.


def create_connection_string(cluster: str, auth_mode: str) -> KustoConnectionStringBuilder:
    if auth_mode == "deviceCode":
        return create_device_code_connection_string(cluster)

    elif auth_mode == "managedIdentity":
        return create_managed_identity_connection_string(cluster)

    elif auth_mode == "AppKey":
        return create_app_key_connection_string(cluster)

    elif auth_mode == "AppCertificate":
        return create_app_cert_connection_string(cluster)

    else:
        die(f"Unexpected Auth mode: '{auth_mode}'")


def create_device_code_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # prompt user for credentials with device code auth
    return KustoConnectionStringBuilder.with_aad_device_authentication(cluster)


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
    # Todo Learn More: For information on how to procure an AAD Application in Azure see:
    #  https://docs.microsoft.com/en-us/azure/data-explorer/provision-azure-ad-app
    app_id = os.environ.get("APP_ID")
    app_tenant = os.environ.get("APP_TENANT")
    app_key = os.environ.get("APP_KEY")

    return KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, app_id, app_key, app_tenant)


def create_app_cert_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # Todo - Config (Optional): App Id & tenant, and certificate to authenticate with
    # Todo Learn More: For information on how to procure an AAD Application in Azure see:
    #  https://docs.microsoft.com/en-us/azure/data-explorer/provision-azure-ad-app
    app_id = os.environ.get("APP_ID")
    app_tenant = os.environ.get("APP_TENANT")
    pem_file_path = os.environ.get("PEM_FILE_PATH")
    thumbprint = os.environ.get("CERT_THUMBPRINT")
    public_cert_path = os.environ.get("PUBLIC_CERT_FILE_PATH")  # Only used on Subject Name and Issuer Auth
    public_certificate = None
    pem_certificate = None

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

        return KustoConnectionStringBuilder.with_aad_application_certificate_authentication(cluster, app_id, pem_certificate, thumbprint, app_tenant)
    else:
        return KustoConnectionStringBuilder.with_aad_application_certificate_sni_authentication(
            cluster, app_id, pem_certificate, public_certificate, thumbprint, app_tenant
        )


def die(error: str, ex: Exception = None):
    print(error)
    if ex is not None:
        print(ex)

    exit(-1)


main()

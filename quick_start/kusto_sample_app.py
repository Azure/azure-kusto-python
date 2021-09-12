# Todo - Start Here:
#  1) Run: pip install azure-kusto-data azure-kusto-ingest
#  2) Fill in or edit the sections commented as 'To Do - Config'
#  3) Run the script
#  4) Follow additional To-Do comments for tips and reference material


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
#  If the sample was download from OneClick it should be pre-populated with your cluster's details:
#  If the sample was taken from GitHub, edit the cluster name and database to point to your cluster.
kustoUri = "https://sdkse2etest.eastus.kusto.windows.net"
ingestUri = "https://ingest-sdkse2etest.eastus.kusto.windows.net"
databaseName = "e2e"
tableName = "SampleTable"
tableSchema = (
    "(rownumber:int, rowguid:string, xdouble:real, xfloat:real, xbool:bool, xint16:int, xint32:int, xint64:long, xuint8:long, xuint16:long, "
    "xuint32:long, xuint64:long, xdate:datetime, xsmalltext:string, xtext:string, xnumberAsText:string, xtime:timespan, xtextWithNulls:string, "
    "xdynamicWithNulls:dynamic)"
)
jsonMappingRef = "SampleTableMapping"
jsonMapping = (
    '[{"Properties":{"Path":"$.rownumber"},"column":"rownumber","datatype":"int"},'
    '{"Properties":{"Path":"$.rowguid"},"column":"rowguid","datatype":"string"},'
    '{"Properties":{"Path":"$.xdouble"},"column":"xdouble","datatype":"real"},'
    '{"Properties":{"Path":"$.xfloat"},"column":"xfloat","datatype":"real"},'
    '{"Properties":{"Path":"$.xbool"},"column":"xbool","datatype":"bool"},'
    '{"Properties":{"Path":"$.xint16"},"column":"xint16","datatype":"int"},'
    '{"Properties":{"Path":"$.xint32"},"column":"xint32","datatype":"int"},'
    '{"Properties":{"Path":"$.xint64"},"column":"xint64","datatype":"long"},'
    '{"Properties":{"Path":"$.xuint8"},"column":"xuint8","datatype":"long"},'
    '{"Properties":{"Path":"$.xuint16"},"column":"xuint16","datatype":"long"},'
    '{"Properties":{"Path":"$.xuint32"},"column":"xuint32","datatype":"long"},'
    '{"Properties":{"Path":"$.xuint64"},"column":"xuint64","datatype":"long"},'
    '{"Properties":{"Path":"$.xdate"},"column":"xdate","datatype":"datetime"},'
    '{"Properties":{"Path":"$.xsmalltext"},"column":"xsmalltext","datatype":"string"},'
    '{"Properties":{"Path":"$.xtext"},"column":"xtext","datatype":"string"},'
    '{"Properties":{"Path":"$.rowguid"},"column":"xnumberAsText","datatype":"string"},'
    '{"Properties":{"Path":"$.xtime"},"column":"xtime","datatype":"timespan"},'
    '{"Properties":{"Path":"$.xtextWithNulls"},"column":"xtextWithNulls","datatype":"string"},'
    '{"Properties":{"Path":"$.xdynamicWithNulls"},"column":"xdynamicWithNulls","datatype":"dynamic"}]'
)
csvSample = "dataset.csv"
jsonSample = "dataset.json"

# Todo - Config (Optional): Change the authentication method from Device Code (User Prompt) to one of the other options
#  Some of the auth modes require additional environment variables to be set in order to work (check the use below)
#  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
authenticationMode = "userPrompt"  # choose between: (userPrompt|managedIdentity|AppKey|AppCertificate)
waitForUser = True


def main():
    print("Kusto sample app is starting...")
    if authenticationMode == "userPrompt":
        print("")
        print("You will be prompted for credentials during this script.")
        print("Please, return to the console after authenticating.")
        wait_for_user()

    # Todo - Tip: Avoid creating a new Kusto Client for each use.
    #  Create the clients once and use them as long as possible.
    kusto_connection_string = create_connection_string(kustoUri, authenticationMode)
    ingest_connection_string = create_connection_string(ingestUri, authenticationMode)
    kusto_client = KustoClient(kusto_connection_string)
    ingest_client = QueuedIngestClient(ingest_connection_string)

    print("")
    print(f"Creating table '{databaseName}.{tableName}' if it does not exist:")
    # Todo - Tip: this is commonly a one-time command
    # Todo - Learn More: For additional information on how to create tables see: https://docs.microsoft.com/en-us/azure/data-explorer/one-click-table
    command = f".create table {tableName} {tableSchema}"
    if not run_control_command(kusto_client, databaseName, command):
        print("Failed to create or validate table exists.")
        exit(-1)

    wait_for_user()

    print("")
    print(f"Altering the batching policy for '{tableName}'")
    # Todo - Tip: this is commonly a one-time command
    batching_policy = '{ "MaximumBatchingTimeSpan": "00:00:10", "MaximumNumberOfItems": 500, "MaximumRawDataSizeMB": 1024 }'
    command = f".alter table {tableName} policy ingestionbatching @'{batching_policy}'"
    if not run_control_command(kusto_client, databaseName, command):
        print("Failed to alter the ingestion policy!")
        print("This could be the result of insufficient permissions.")
        print("The sample will still run, though ingestion will be delayed for 5 minutes")

    # Todo - Learn More:
    #  Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
    #   1) more then a 1000 files were queued for ingestion for the same table by the same user
    #   2) more then 1GB of data was queued for ingestion for the same table by the same user
    #   3) More then 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
    #  In order to speed up this sample app, we attempt to modify the default ingestion policy to ingest data after 10 seconds have passed
    #  For additional information on customizing the ingestion batching policy see:
    #   https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/batchingpolicy
    #  You may also skip the batching for some files using the FlushImmediatly property, though this option should be used with care as it is inefficient
    wait_for_user()

    # Todo - Learn More: For additional information on Kusto Query Language see: https://docs.microsoft.com/en-us/azure/data-explorer/write-queries
    print("")
    print(f"Initial row count for '{databaseName}.{tableName}' is:")
    run_query(kusto_client, databaseName, f"{tableName} | summarize count()")
    wait_for_user()

    # Todo - Learn More: For additional information on how to ingest data to Kusto in Python see:
    #  https://docs.microsoft.com/en-us/azure/data-explorer/python-ingest-data
    if csvSample is not None:
        print("")
        print(f"Attempting to ingest '{csvSample}'")
        ingest_data_from_file(ingest_client, databaseName, tableName, csvSample, DataFormat.CSV)
        wait_for_user()

    if jsonSample is not None:
        print("")
        print(f"Attempting to create a json mapping reference named '{jsonMappingRef}'")
        # Todo - Tip: this is commonly a one-time command
        mapping_command = f".create-or-alter table {tableName} ingestion json mapping '{jsonMappingRef}' '{jsonMapping}'"
        mapping_exists = run_control_command(kusto_client, databaseName, mapping_command)
        if not mapping_exists:
            print(f"failed to create a json  mapping reference named {jsonMappingRef}")
            print(f"skipping json ingestion")

        # Todo - learn more:  for more information about providing inline mappings or mapping references see:
        #  https://docs.microsoft.com/en-us/azure/data-explorer/kusto/management/mappings

        wait_for_user()

        if mapping_exists:
            print("")
            print(f"Attempting to ingest '{jsonSample}'")
            ingest_data_from_file(ingest_client, databaseName, tableName, jsonSample, DataFormat.MULTIJSON, jsonMappingRef)
            wait_for_user()

    print("")
    print("Sleeping for a few seconds to make sure queued ingestion has completed")
    print("Mind, this may take longer depending on the file size and ingestion policy")
    for x in range(20, 0, -1):
        print(f"{x} ", end="\r")
        time.sleep(1)

    print("")
    print(f"Post ingestion row count for '{databaseName}.{tableName}' is:")
    run_query(kusto_client, databaseName, f"{tableName} | summarize count()")

    print("")
    print(f"Post ingestion sample data query:")
    run_query(kusto_client, databaseName, f"{tableName} | take 2")


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


def ingest_data_from_file(client: QueuedIngestClient, db: str, table: str, file_path: str, file_format: str, mapping_ref: str = None):
    ingestion_props = IngestionProperties(
        database=f"{db}",
        table=f"{table}",
        # Todo - Learn More: For additional information about supported data formats, see
        #  https://docs.microsoft.com/en-us/azure/data-explorer/ingestion-supported-formats
        data_format=file_format,
        # Todo - Config: Setting the ingestion batching policy takes up to 5 minutes to have an effect.
        #  For the sake of the sample we set Flush-Immediately, but in practice it should not be commonly used.
        #  Comment the below line after running the sample for the first few times!
        flush_immediately=True,
        ingestion_mapping_reference=mapping_ref,
    )

    # Todo - Tip: for optimal ingestion batching it's best to specify the uncompressed data size in the file descriptor
    file_descriptor = FileDescriptor(f"{file_path}")
    client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_props)

    # Todo - Tip: Kusto can also ingest data from blobs, open streams and pandas dataframes.
    #  See the python SDK azure.kusto.ingest samples for additional references.


def create_connection_string(cluster: str, auth_mode: str) -> KustoConnectionStringBuilder:
    # Todo - Learn More: For additional information on how to authorize users and apps on Kusto Database see:
    #  https://docs.microsoft.com/en-us/azure/data-explorer/manage-database-permissions
    if auth_mode == "userPrompt":
        return create_interactive_auth_connection_string(cluster)

    elif auth_mode == "managedIdentity":
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
    print("")
    print("Script failed:")
    print(error)
    if ex is not None:
        print(ex)

    exit(-1)


def wait_for_user():
    if waitForUser:
        input("Press Enter to continue...")


if __name__ == "__main__":
    main()

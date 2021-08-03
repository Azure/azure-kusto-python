# ==============
# !!! README !!!
# ==============
#
# Run setup.bat to install necessary dependencies.
# Follow the '# Config (Mandatory|Optional):' comments and fill in any necessary mandatory and optional arguments depending on your scenario
# run the script

import io
import os
import typing

from datetime import timedelta

from azure.kusto.data.exceptions import KustoServiceError
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

# Config (Mandatory):
kustoUri = "https://sdkse2etest.eastus.kusto.windows.net"
ingestUri = "https://ingest-sdkse2etest.eastus.kusto.windows.net"
databaseName = "e2e"
tableName = "quick_start_table"
tableSchema = ""
ingestionDirectory = ""
fileFormat = ""

# Config (Optional): Change the authentication method
# Note some of the auth modes require additional environment variables to be set in order to work (check the use below)
authenticationMode = "deviceCode"  # choose between: (deviceCode|managedIdentity|AppKey|AppCertificate)


def main():
    print("kusto sample app is starting")

    kusto_connection_string = create_connection_string(kustoUri, authenticationMode)
    ingest_connection_string = create_connection_string(ingestUri, authenticationMode)
    kusto_client = KustoClient(kusto_connection_string)
    ingest_client = QueuedIngestClient(ingest_connection_string)

    print(f"Creating table '{databaseName}.{tableName}' if needed:")
    command = ""
    run_control_command(kusto_client, databaseName, tableName, command)
    print("")

    print(f"Initial row count for '{databaseName}.{tableName}' is:")
    run_query(kusto_client, databaseName, f"{tableName} | summarize count()")
    print("")

    print(f"Ingesting all files from '{ingestionDirectory}'")
    ingest_data_from_folder(ingest_client, databaseName, tableName, ingestionDirectory)
    print("")

    print(f"Post ingestion row count for '{databaseName}.{tableName}' is:")
    run_query(kusto_client, databaseName, f"{tableName} | summarize count()")


def create_connection_string(cluster:str, auth_mode:str) -> KustoConnectionStringBuilder:
    kcsb = KustoConnectionStringBuilder(cluster)

    if auth_mode == "deviceCode":
        # prompt user for credentials with device code auth
        kcsb.with_aad_device_authentication(cluster)

    elif auth_mode == "managedIdentity":
        # Connect using the system or user provided managed identity (azure service only)
        # Config (Optional): Managed identity client id if you are using a User Assigned Managed Id
        client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")

        if client_id is None:
            kcsb.with_aad_managed_service_identity_authentication(cluster)
        else:
            kcsb.with_aad_managed_service_identity_authentication(cluster, client_id=client_id)

    elif auth_mode == "AppKey":
        # Config (Optional): App Id & tenant, and App Key to authenticate with
        app_id = os.environ.get("APP_ID")
        app_tenant = os.environ.get("APP_TENANT")
        app_key = os.environ.get("APP_KEY")

        kcsb.with_aad_application_key_authentication(cluster, app_id, app_key, app_tenant)

    elif auth_mode == "AppCertificate":
        # Config (Optional): App Id & tenant, and certificate to authenticate with
        raise Exception("AppCertificate auth mode is not implemented")

    else:
        raise Exception(f"Unexpected Auth mode: '{auth_mode}'")

    return kcsb


def run_control_command(client:KustoClient, db:str, table:str, command:str):
    print("not run")
    pass


def run_query(client:KustoClient, db:str, query:str):
    print("0")
    pass


def ingest_data_from_folder(client:QueuedIngestClient, db:str, table:str, folder_path:str):
    print("nothing to ingest")
    pass


main()

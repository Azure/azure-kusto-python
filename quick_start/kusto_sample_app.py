# ==============
# !!! README !!!
# ==============

# 1) Run setup.bat to install necessary dependencies.
# 2) Follow the To-Do comments and fill in any necessary mandatory and optional arguments depending on your scenario
# 3) run the script

# Todo cleanup the import section when done
import io
import os
import typing

from datetime import timedelta

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

# Todo Config (AutoFill):
kustoUri = "https://sdkse2etest.eastus.kusto.windows.net"
ingestUri = "https://ingest-sdkse2etest.eastus.kusto.windows.net"
databaseName = "e2e"
tableName = "aa"
tableSchema = ""
fileFormat = "csv"  # choose between: (csv|json)

# Todo Config (Mandatory):
ingestionDirectory = None

# Todo Config (Optional): Change the authentication method
# Note!
# Some of the auth modes require additional environment variables to be set in order to work (check the use below)
authenticationMode = "deviceCode"  # choose between: (deviceCode|managedIdentity|AppKey|AppCertificate)


def main():
    print("kusto sample app is starting")

    if authenticationMode == "deviceCode":
        print("During the run you will be prompted by the browser to enter a device code.")
        print("Return to the console of the sample app and copy the code to provide from it.")
        input("Press enter to continue...")

    kusto_connection_string = create_connection_string(kustoUri, authenticationMode)
    ingest_connection_string = create_connection_string(ingestUri, authenticationMode)
    kusto_client = KustoClient(kusto_connection_string)
    ingest_client = QueuedIngestClient(ingest_connection_string)

    print(f"Creating table '{databaseName}.{tableName}' if needed:")
    # Todo what's the table creation command
    command = ".show version"
    if not run_control_command(kusto_client, databaseName, command):
        print("Failed to create or validate table exists.")
        exit(-1)

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
    if auth_mode == "deviceCode":
        # prompt user for credentials with device code auth
        return KustoConnectionStringBuilder.with_aad_device_authentication(cluster)

    elif auth_mode == "managedIdentity":
        # Connect using the system or user provided managed identity (azure service only)
        # Todo Config (Optional): Managed identity client id if you are using a User Assigned Managed Id
        client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")

        if client_id is None:
            return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster)
        else:
            return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster, client_id=client_id)

    elif auth_mode == "AppKey":
        # Todo Config (Optional): App Id & tenant, and App Key to authenticate with
        app_id = os.environ.get("APP_ID")
        app_tenant = os.environ.get("APP_TENANT")
        app_key = os.environ.get("APP_KEY")

        return KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, app_id, app_key, app_tenant)

    elif auth_mode == "AppCertificate":
        # Todo Config (Optional): App Id & tenant, and certificate to authenticate with
        # Todo implement cert based auth
        raise Exception("AppCertificate auth mode is not implemented")

    else:
        raise Exception(f"Unexpected Auth mode: '{auth_mode}'")


def run_control_command(client:KustoClient, db:str, command:str) -> bool:
    try:
        res = client.execute_mgmt(db, command)
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


def run_query(client:KustoClient, db:str, query:str):
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


def ingest_data_from_folder(client:QueuedIngestClient, db:str, table:str, folder_path:str):
    print("nothing to ingest")
    # todo iterate over all files of the folder and queue them for ingestion
    # todo demonstrate use of Flush immediately option
    # todo demonstrate ingestion of json and csv files
    pass


main()

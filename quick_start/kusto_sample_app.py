# ==============
# !!! README !!!
# ==============

# Todo:
# 1) Run setup.bat to install necessary dependencies.
# 2) Follow the To-Do comments and fill in any necessary mandatory and optional arguments depending on your scenario
# 3) run the script

# Todo (Yochai) cleanup the import section when done
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

    # Todo - for information on how to authorize users and apps on Kusto Database see:
    #  https://docs.microsoft.com/en-us/azure/data-explorer/manage-database-permissions
    if authenticationMode == "deviceCode":
        print("During the run you will be prompted by the browser to enter a device code.")
        print("Return to the console of the sample app and copy the code to provide from it.")
        input("Press enter to continue...")

    kusto_connection_string = create_connection_string(kustoUri, authenticationMode)
    ingest_connection_string = create_connection_string(ingestUri, authenticationMode)
    kusto_client = KustoClient(kusto_connection_string)
    ingest_client = QueuedIngestClient(ingest_connection_string)

    # Todo - for more information on how to create tables see: https://docs.microsoft.com/en-us/azure/data-explorer/one-click-table
    print("")
    print(f"Creating table '{databaseName}.{tableName}' if needed:")
    # Todo (Yochai) what's the table creation command
    command = ".show version"
    if not run_control_command(kusto_client, databaseName, command):
        print("Failed to create or validate table exists.")
        exit(-1)

    # Todo - for more information on how to query Kusto see: https://docs.microsoft.com/en-us/azure/data-explorer/write-queries
    print("")
    print(f"Initial row count for '{databaseName}.{tableName}' is:")
    run_query(kusto_client, databaseName, f"{tableName} | summarize count()")

    # Todo - for more information on how to ingest data to Kusto in Python see: https://docs.microsoft.com/en-us/azure/data-explorer/python-ingest-data
    print("")
    print(f"Ingesting all files from '{ingestionDirectory}'")
    ingest_data_from_folder(ingest_client, databaseName, tableName, ingestionDirectory)

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


def ingest_data_from_folder(client: QueuedIngestClient, db: str, table: str, folder_path: str):
    print("nothing to ingest")
    # todo (yochai) iterate over all files of the folder and queue them for ingestion
    # todo (yochai) demonstrate use of Flush immediately option
    # todo (yochai) demonstrate ingestion of json and csv files
    pass


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
    # Todo Config (Optional): Managed identity client id if you are using a User Assigned Managed Id
    client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")

    if client_id is None:
        return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster)
    else:
        return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster, client_id=client_id)


def create_app_key_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # Todo - for information on how to procure an AAD Application in Azure see: https://docs.microsoft.com/en-us/azure/data-explorer/provision-azure-ad-app
    # Todo Config (Optional): App Id & tenant, and App Key to authenticate with
    app_id = os.environ.get("APP_ID")
    app_tenant = os.environ.get("APP_TENANT")
    app_key = os.environ.get("APP_KEY")

    return KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, app_id, app_key, app_tenant)


def create_app_cert_connection_string(cluster: str) -> KustoConnectionStringBuilder:
    # Todo - for information on how to procure an AAD Application in Azure see: https://docs.microsoft.com/en-us/azure/data-explorer/provision-azure-ad-app
    # Todo Config (Optional): App Id & tenant, and certificate to authenticate with
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

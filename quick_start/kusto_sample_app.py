import json
import os
import time
import uuid
from distutils.util import strtobool
from typing import ClassVar

from azure.kusto.data.exceptions import KustoClientError, KustoServiceError
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder, ClientRequestProperties

from azure.kusto.ingest import (
    QueuedIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    DataFormat,
)

from ingest import IngestionMappingType


class KustoSampleApp:
    # TODO (config - optional): Change the authentication method from "User Prompt" to any of the other options
    #  Some of the auth modes require additional environment variables to be set in order to work (see usage in generate_connection_string below)
    #  Managed Identity Authentication only works when running as an Azure service (webapp, function, etc.)
    AUTHENTICATION_MODE = "UserPrompt"  # Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)

    # TODO (config - optional): Toggle to False to execute this script "unattended"
    WAIT_FOR_USER = True

    # TODO (config):
    #  If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details
    #  If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately
    CONFIG_FILE_NAME = "kusto_sample_config.json"

    BATCHING_POLICY = "{ \"MaximumBatchingTimeSpan\": \"00:00:10\", \"MaximumNumberOfItems\": 500, \"MaximumRawDataSizeMB\": 1024 }"
    WAIT_FOR_INGEST_SECONDS = 20

    _step = 1
    use_existing_table: ClassVar[bool]
    database_name: ClassVar[str]
    table_name: ClassVar[str]
    table_schema: ClassVar[str]
    kusto_url: ClassVar[str]
    ingest_url: ClassVar[str]
    data_to_ingest: ClassVar[list]
    should_alter_table: ClassVar[bool]
    should_query_data: ClassVar[bool]
    should_ingest_data: ClassVar[bool]

    @classmethod
    def start(cls) -> None:
        cls.load_configs(cls.CONFIG_FILE_NAME)

        if cls.AUTHENTICATION_MODE == "UserPrompt":
            cls.wait_for_user_to_proceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.")

        kusto_connection_string = cls.generate_connection_string(cls.kusto_url, cls.AUTHENTICATION_MODE)
        ingest_connection_string = cls.generate_connection_string(cls.ingest_url, cls.AUTHENTICATION_MODE)
        # Tip: Avoid creating a new Kusto/ingest client for each use. Instead, create the clients once and reuse them.
        kusto_client = KustoClient(kusto_connection_string)
        ingest_client = QueuedIngestClient(ingest_connection_string)

        if cls.use_existing_table:
            if cls.should_alter_table:
                # Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
                # Learn More: For more information about altering table schemas, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                cls.alter_merge_existing_table_to_provided_schema(kusto_client, cls.database_name, cls.table_name, cls.table_schema)

            if cls.should_query_data:
                # Learn More: For more information about Kusto Query Language (KQL), see: https://docs.microsoft.com/azure/data-explorer/write-queries
                cls.query_existing_number_of_rows(kusto_client, cls.database_name, cls.table_name)

        else:
            # Tip: This is generally a one-time configuration
            # Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
            cls.create_new_table(kusto_client, cls.database_name, cls.table_name, cls.table_schema)

        if cls.should_ingest_data:
            for file in cls.data_to_ingest:
                data_format = DataFormat[str(file["format"]).upper()]
                mapping_name = file["mappingName"]

                # Tip: This is generally a one-time configuration.
                # Learn More: For more information about providing inline mappings and mapping references, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
                if not cls.create_ingestion_mappings(strtobool(file["useExistingMapping"].lower()), kusto_client, cls.database_name, cls.table_name, mapping_name, file["mappingValue"], data_format):
                    continue

                # Learn More: For more information about ingesting data to Kusto in Python, see: https://docs.microsoft.com/azure/data-explorer/python-ingest-data
                cls.ingest(file, data_format, ingest_client, cls.database_name, cls.table_name, mapping_name)

            cls.wait_for_ingestion_to_complete()

        if cls.should_query_data:
            cls.execute_validation_queries(kusto_client, cls.database_name, cls.table_name, cls.should_ingest_data)

    @classmethod
    def wait_for_user_to_proceed(cls, upcoming_operation: str) -> None:
        print()
        print(f"Step {cls._step}: {upcoming_operation}")
        cls._step = cls._step + 1
        if cls.WAIT_FOR_USER:
            input("Press ENTER to proceed with this operation...")

    @classmethod
    def die(cls, error: str, ex: Exception = None) -> None:
        print(f"Script failed with error: {error}")
        if ex is not None:
            print("Exception:")
            print(ex)

        exit(-1)

    @classmethod
    def load_configs(cls, config_file_name: str) -> None:
        try:
            with open(config_file_name, "r") as config_file:
                config = json.load(config_file)
        except Exception as ex:
            cls.die(f"Couldn't read load config file from file '{config_file_name}'", ex)

        cls.use_existing_table = strtobool(config["useExistingTable"].lower())
        cls.database_name = config["databaseName"]
        cls.table_name = config["tableName"]
        cls.table_schema = config["tableSchema"]
        cls.kusto_url = config["kustoUri"]
        cls.ingest_url = config["ingestUri"]
        cls.data_to_ingest = config["data"]
        cls.should_alter_table = strtobool(config["alterTable"].lower())
        cls.should_query_data = strtobool(config["queryData"].lower())
        cls.should_ingest_data = strtobool(config["ingestData"].lower())
        if cls.database_name is None or cls.table_name is None or cls.table_schema is None or cls.kusto_url is None or cls.ingest_url is None or cls.data_to_ingest is None:
            cls.die(f"File '{config_file_name}' is missing required fields")

    @classmethod
    def generate_connection_string(cls, cluster_url: str, authentication_mode: str) -> KustoConnectionStringBuilder:
        # Learn More: For additional information on how to authorize users and apps in Kusto, see: https://docs.microsoft.com/azure/data-explorer/manage-database-permissions
        if authentication_mode == "UserPrompt":
            # Prompt user for credentials
            return KustoConnectionStringBuilder.with_interactive_login(cluster_url)
        elif authentication_mode == "ManagedIdentity":
            return cls.create_managed_identity_connection_string(cluster_url)
        elif authentication_mode == "AppKey":
            # Learn More: For information about how to procure an AAD Application, see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
            # TODO (config - optional): App ID & tenant, and App Key to authenticate with
            return KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster_url,
                                                                                        os.environ.get("APP_ID"),
                                                                                        os.environ.get("APP_KEY"),
                                                                                        os.environ.get("APP_TENANT"))
        elif authentication_mode == "AppCertificate":
            return cls.create_application_certificate_connection_string(cluster_url)
        else:
            cls.die(f"Authentication mode '{authentication_mode}' is not supported")

    @classmethod
    def create_managed_identity_connection_string(cls, cluster_url: str) -> KustoConnectionStringBuilder:
        # Connect using the system- or user-assigned managed identity (Azure service only)
        # TODO (config - optional): Managed identity client ID if you are using a user-assigned managed identity
        client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
        if client_id is None:
            return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_url)
        else:
            return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_url,
                                                                                                 client_id=client_id)

    @classmethod
    def create_application_certificate_connection_string(cls, cluster_url: str) -> KustoConnectionStringBuilder:
        # TODO (config - optional): App ID & tenant, path to public certificate and path to private certificate pem file to authenticate with
        app_id = os.environ.get("APP_ID")
        app_tenant = os.environ.get("APP_TENANT")
        private_key_pem_file_path = os.environ.get("PRIVATE_KEY_PEM_FILE_PATH")
        cert_thumbprint = os.environ.get("CERT_THUMBPRINT")
        public_cert_file_path = os.environ.get("PUBLIC_CERT_FILE_PATH")  # Only used for "Subject Name and Issuer" auth
        public_certificate = None
        pem_certificate = None

        try:
            with open(private_key_pem_file_path, "r") as pem_file:
                pem_certificate = pem_file.read()
        except Exception as ex:
            cls.die(f"Failed to load PEM file from {private_key_pem_file_path}", ex)

        if public_cert_file_path:
            try:
                with open(public_cert_file_path, "r") as cert_file:
                    public_certificate = cert_file.read()
            except Exception as ex:
                cls.die(f"Failed to load public certificate file from {public_cert_file_path}", ex)

            return KustoConnectionStringBuilder.with_aad_application_certificate_sni_authentication(
                cluster_url, app_id, pem_certificate, public_certificate, cert_thumbprint, app_tenant
            )
        else:
            return KustoConnectionStringBuilder.with_aad_application_certificate_authentication(cluster_url, app_id,
                                                                                                pem_certificate,
                                                                                                cert_thumbprint, app_tenant)

    @classmethod
    def create_new_table(cls, kusto_client: KustoClient, database_name: str, table_name: str, table_schema: str) -> None:
        cls.wait_for_user_to_proceed(f"Create table '{database_name}.{table_name}'")
        command = f".create table {table_name} {table_schema}"
        if not cls.execute_control_command(kusto_client, database_name, command):
            cls.die(f"Failed to create table or validate it exists using command '{command}'")

        # Learn More:
        #  Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
        #   1) More than 1,000 files were queued for ingestion for the same table by the same user
        #   2) More than 1GB of data was queued for ingestion for the same table by the same user
        #   3) More than 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
        #  For more information about customizing the ingestion batching policy, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy

        # Disabled to prevent an existing batching policy from being unintentionally changed
        if False:
            cls.alter_batching_policy(kusto_client, database_name, table_name)

    @classmethod
    def alter_merge_existing_table_to_provided_schema(cls, kusto_client: KustoClient, database_name: str, table_name: str, table_schema: str) -> None:
        cls.wait_for_user_to_proceed(f"Alter-merge existing table '{database_name}.{table_name}' to align with the provided schema")
        command = f".alter-merge table {table_name} {table_schema}"
        if not cls.execute_control_command(kusto_client, database_name, command):
            cls.die(f"Failed to alter table using command '{command}'")

    @classmethod
    def execute_control_command(cls, client: KustoClient, database_name: str, control_command: str) -> bool:
        try:
            client_request_properties = cls.create_client_request_properties("Python_SampleApp_ControlCommand")
            result = client.execute_mgmt(database_name, control_command, client_request_properties)
            # Tip: Actual implementations wouldn't generally print the response from a control command. We print here to demonstrate what the response looks like.
            print(f"Response from executed control command '{control_command}':")
            for row in result.primary_results[0]:
                print(row.to_list())

            return True
        except KustoClientError as ex:
            print(f"Client error while trying to execute control command '{control_command}' on database '{database_name}'")
            print(ex)
        except KustoServiceError as ex:
            print(f"Server error while trying to execute control command '{control_command}' on database '{database_name}'")
            print(ex)
        except Exception as ex:
            print(f"Unknown error while trying to execute control command '{control_command}' on database '{database_name}'")
            print(ex)

        return False

    @classmethod
    def execute_query(cls, kusto_client: KustoClient, database_name: str, query: str):
        try:
            client_request_properties = cls.create_client_request_properties("Python_SampleApp_Query")
            result = kusto_client.execute_query(database_name, query, client_request_properties)
            print(f"Response from executed query '{query}':")
            for row in result.primary_results[0]:
                print(row.to_list())

            return True
        except KustoClientError as ex:
            print(f"Client error while trying to execute query '{query}' on database '{database_name}'")
            print(ex)
        except KustoServiceError as ex:
            print(f"Server error while trying to execute query '{query}' on database '{database_name}'")
            print(ex)
        except Exception as ex:
            print(f"Unknown error while trying to execute query '{query}' on database '{database_name}'")
            print(ex)

        return False

    @classmethod
    def create_client_request_properties(cls, scope: str, timeout: str = None) -> ClientRequestProperties:
        client_request_properties = ClientRequestProperties()
        client_request_properties.client_request_id = f"{scope};{str(uuid.uuid4())}"
        client_request_properties.application = "kusto_sample_app.py"

        # Tip: Though uncommon, you can alter the request default command timeout using the below command, e.g. to set the timeout to 10 minutes, use "10m"
        if timeout is not None:
            client_request_properties.set_option(ClientRequestProperties.request_timeout_option_name, timeout)

        return client_request_properties

    @classmethod
    def query_existing_number_of_rows(cls, kusto_client: KustoClient, database_name: str, table_name: str) -> None:
        cls.wait_for_user_to_proceed(f"Get existing row count in '{database_name}.{table_name}'")
        cls.execute_query(kusto_client, database_name, f"{table_name} | count")

    @classmethod
    def alter_batching_policy(cls, kusto_client: KustoClient, database_name: str, table_name: str) -> None:
        # Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and
        #  in this sample app, we opt to modify the default ingestion policy to ingest data after at most 10 seconds.
        # Tip 2: This is generally a one-time configuration.
        # Tip 3: You can also skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient.
        cls.wait_for_user_to_proceed(f"Alter the batching policy for table '{database_name}.{table_name}'")
        command = f".alter table {table_name} policy ingestionbatching @'{cls.BATCHING_POLICY}'"
        if not cls.execute_control_command(kusto_client, database_name, command):
            print("Failed to alter the ingestion policy, which could be the result of insufficient permissions. The sample will still run, though ingestion will be delayed for up to 5 minutes.")

    @classmethod
    def create_ingestion_mappings(cls, use_existing_mapping:bool, kusto_client: KustoClient, database_name: str, table_name: str, mapping_name: str, mapping_value: str, data_format: DataFormat) -> bool:
        if not use_existing_mapping:
            if cls.data_format_to_is_mapping_required(data_format) and not mapping_value:
                print(f"The data format '{data_format.name}' requires a mapping, but configuration indicates to not use an existing mapping and no mapping was provided. Skipping this ingestion.")
                return False

            if mapping_value:
                ingestion_mapping_kind = cls.data_format_to_ingestion_mapping_type(data_format).value.lower()
                cls.wait_for_user_to_proceed(f"Create a '{ingestion_mapping_kind}' mapping reference named '{mapping_name}'")

                if not mapping_name:
                    mapping_name = "DefaultQuickstartMapping" + UUID.randomUUID().toString().substring(0, 5)
                mapping_command = f".create-or-alter table {table_name} ingestion {ingestion_mapping_kind} mapping '{mapping_name}' '{mapping_value}'"
                if not cls.execute_control_command(kusto_client, database_name, mapping_command):
                    print(f"Failed to create a '{ingestion_mapping_kind}' mapping reference named '{mapping_name}'. Skipping this ingestion.")
                    return False
        elif cls.data_format_to_is_mapping_required(data_format) and not mapping_name:
            print(f"The data format '{data_format.name}' requires a mapping and the configuration indicates an existing mapping should be used, but none was provided. Skipping this ingestion.")
            return False

        return True

    @classmethod
    def ingest(cls, file: dict, data_format: DataFormat, ingest_client: QueuedIngestClient, database_name: str, table_name: str, mapping_name: str) -> None:
        source_type = str(file["sourceType"]).lower()
        uri = file["dataSourceUri"]
        cls.wait_for_user_to_proceed(f"Ingest '{uri}' from '{source_type}'")
        # Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
        # If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
        data_format = DataFormat.MULTIJSON if data_format == data_format.JSON else data_format

        # Tip: Kusto's Python SDK can ingest data from files, blobs, open streams and pandas dataframes.
        #  See the SDK's samples and the E2E tests in azure.kusto.ingest for additional references.
        if source_type == "localfilesource":
            cls.ingest_from_file(ingest_client, database_name, table_name, uri, data_format, mapping_name)
        elif source_type == "blobsource":
            cls.ingest_from_blob(ingest_client, database_name, table_name, uri, data_format, mapping_name)
        else:
            print(f"Unknown source '{source_type}' for file '{uri}'")

    @classmethod
    def ingest_from_file(cls, ingest_client: QueuedIngestClient, database_name: str, table_name: str, file_path: str,
                         data_format: DataFormat, mapping_name: str = None):
        ingestion_properties = cls.create_ingestion_properties(database_name, table_name, data_format, mapping_name)

        # Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor instead of the default below of 0.
        #  Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
        # Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere
        file_descriptor = FileDescriptor(file_path, size=0, source_id=uuid.uuid4())
        ingest_client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_properties)

    @classmethod
    def ingest_from_blob(cls, client: QueuedIngestClient, database_name: str, table_name: str, blob_url: str,
                         data_format: DataFormat, mapping_name: str = None):
        ingestion_properties = cls.create_ingestion_properties(database_name, table_name, data_format, mapping_name)

        # Tip 1: For optimal ingestion batching and performance, specify the uncompressed data size in the file descriptor instead of the default below of 0.
        #  Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
        # Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere
        blob_descriptor = BlobDescriptor(blob_url, size=0, source_id=str(uuid.uuid4()))
        client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_properties)

    @classmethod
    def create_ingestion_properties(cls, database_name: str, table_name: str, data_format: DataFormat, mapping_name: str) -> IngestionProperties:
        return IngestionProperties(
            database=f"{database_name}",
            table=f"{table_name}",
            ingestion_mapping_reference=mapping_name,
            # Learn More: For more information about supported data formats, see: https://docs.microsoft.com/azure/data-explorer/ingestion-supported-formats
            data_format=data_format,
            # TODO (config - optional): Setting the ingestion batching policy takes up to 5 minutes to take effect.
            #  We therefore set Flush-Immediately for the sake of the sample, but it generally shouldn't be used in practice.
            #  Comment out the line below after running the sample the first few times.
            flush_immediately=True,
        )

    @classmethod
    def wait_for_ingestion_to_complete(cls) -> None:
        print(f"Sleeping {cls.WAIT_FOR_INGEST_SECONDS} seconds for queued ingestion to complete. Note: This may take longer depending on the file size and ingestion batching policy.")
        for x in range(cls.WAIT_FOR_INGEST_SECONDS, 0, -1):
            print(f"{x} ", end="\r")
            time.sleep(1)
        print()
        print()

    @classmethod
    def execute_validation_queries(cls, kusto_client: KustoClient, database_name: str, table_name: str, should_ingest_data: bool) -> None:
        optional_post_ingestion_message = "post-ingestion " if should_ingest_data else ""
        print(f"Step {cls._step}: Get {optional_post_ingestion_message}row count for '{database_name}.{table_name}':")
        cls._step = cls._step + 1
        cls.execute_query(kusto_client, database_name, f"{table_name} | count")

        print("")
        print(f"Step {cls._step}: Get sample (2 records) of {optional_post_ingestion_message}data:")
        cls._step = cls._step + 1
        cls.execute_query(kusto_client, database_name, f"{table_name} | take 2")

    # This method is temporary until the next major version bump, when the DataFormat enum will be enriched with this as a property
    @classmethod
    def data_format_to_ingestion_mapping_type(cls, data_format: DataFormat) -> IngestionMappingType:
        if data_format == DataFormat.CSV:
            return IngestionMappingType.CSV
        elif data_format == DataFormat.TSV:
            return IngestionMappingType.CSV
        elif data_format == DataFormat.SCSV:
            return IngestionMappingType.CSV
        elif data_format == DataFormat.SOHSV:
            return IngestionMappingType.CSV
        elif data_format == DataFormat.PSV:
            return IngestionMappingType.CSV
        elif data_format == DataFormat.TXT:
            return IngestionMappingType.UNKNOWN
        elif data_format == DataFormat.JSON:
            return IngestionMappingType.JSON
        elif data_format == DataFormat.SINGLEJSON:
            return IngestionMappingType.JSON
        elif data_format == DataFormat.AVRO:
            return IngestionMappingType.AVRO
        elif data_format == DataFormat.PARQUET:
            return IngestionMappingType.PARQUET
        elif data_format == DataFormat.MULTIJSON:
            return IngestionMappingType.JSON
        elif data_format == DataFormat.ORC:
            return IngestionMappingType.ORC
        elif data_format == DataFormat.TSVE:
            return IngestionMappingType.CSV
        elif data_format == DataFormat.RAW:
            return IngestionMappingType.UNKNOWN
        elif data_format == DataFormat.W3CLOGFILE:
            return IngestionMappingType.W3CLOGFILE
        else:
            raise ValueError(fr"Unexpected data format {self}")

    # This method is temporary until the next major version bump, when the DataFormat enum will be enriched with this as a property
    @classmethod
    def data_format_to_is_mapping_required(cls, data_format: DataFormat) -> bool:
        if data_format == DataFormat.CSV:
            return False
        elif data_format == DataFormat.TSV:
            return False
        elif data_format == DataFormat.SCSV:
            return False
        elif data_format == DataFormat.SOHSV:
            return False
        elif data_format == DataFormat.PSV:
            return False
        elif data_format == DataFormat.TXT:
            return False
        elif data_format == DataFormat.JSON:
            return True
        elif data_format == DataFormat.SINGLEJSON:
            return True
        elif data_format == DataFormat.AVRO:
            return True
        elif data_format == DataFormat.PARQUET:
            return False
        elif data_format == DataFormat.MULTIJSON:
            return True
        elif data_format == DataFormat.ORC:
            return False
        elif data_format == DataFormat.TSVE:
            return False
        elif data_format == DataFormat.RAW:
            return False
        elif data_format == DataFormat.W3CLOGFILE:
            return False
        else:
            raise ValueError(fr"Unexpected data format {self}")


def main():
    print("Kusto sample app is starting...")
    KustoSampleApp.start()
    print("Kusto sample app done")


if __name__ == "__main__":
    main()

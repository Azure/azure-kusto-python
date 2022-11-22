import dataclasses
import enum
import json
import uuid
from dataclasses import dataclass
from typing import List

import inflection as inflection

from azure.kusto.data import DataFormat, KustoClient
from azure.kusto.ingest import QueuedIngestClient
from utils import AuthenticationModeOptions, Utils


class SourceType(enum.Enum):
    """
    SourceType - represents the type of files used for ingestion
    """

    local_file_source = "localFileSource"
    blob_source = "blobSource"
    nosource = "nosource"


@dataclass
class ConfigData:
    """
    ConfigData object - represents a file from which to ingest
    """

    source_type: SourceType
    data_source_uri: str
    data_format: DataFormat
    use_existing_mapping: bool
    mapping_name: str
    mapping_value: str

    @staticmethod
    def from_json(json_dict: dict) -> "ConfigData":
        json_dict["sourceType"] = SourceType[inflection.underscore(json_dict.pop("sourceType"))]
        json_dict["dataFormat"] = DataFormat[json_dict.pop("format").upper()]
        return data_class_from_json(json_dict, ConfigData)


@dataclass
class ConfigJson:
    """
    ConfigJson object - represents a cluster and DataBase connection configuration file.
    """

    use_existing_table: bool
    database_name: str
    table_name: str
    table_schema: str
    kusto_uri: str
    ingest_uri: str
    data_to_ingest: List[ConfigData]
    alter_table: bool
    query_data: bool
    ingest_data: bool
    authentication_mode: AuthenticationModeOptions
    wait_for_user: bool
    ignore_first_record: bool
    wait_for_ingest_seconds: bool
    batching_policy: str

    @staticmethod
    def from_json(json_dict: dict) -> "ConfigJson":
        json_dict["dataToIngest"] = [ConfigData.from_json(j) for j in json_dict.pop("data")]
        return data_class_from_json(json_dict, ConfigJson)


def remove_extra_keys(json_dict: dict, data_type: type) -> dict:
    assert dataclasses.is_dataclass(data_type)
    field_names = [field.name for field in dataclasses.fields(data_type)]
    return {key: value for key, value in json_dict.items() if key in field_names}


def keys_to_snake_case(json_dict: dict) -> dict:
    return {inflection.underscore(key): val for (key, val) in json_dict.items()}


def data_class_from_json(json_dict: dict, data_type: type) -> dataclasses.dataclass:
    assert dataclasses.is_dataclass(data_type)
    all_keys = keys_to_snake_case(json_dict)
    config_json_keys = remove_extra_keys(all_keys, data_type)
    return data_type(**config_json_keys)


class KustoSampleApp:
    # TODO (config):
    #  If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details
    #  If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately
    CONFIG_FILE_NAME = "kusto_sample_config.json"

    __step = 1
    config = None

    @classmethod
    def load_configs(cls, config_file_name: str) -> None:
        """
        Loads JSON configuration file, and sets the metadata in place.
        :param config_file_name: Configuration file path.
        """
        try:
            with open(config_file_name, "r") as config_file:
                json_dict = json.load(config_file)
                cls.config = ConfigJson.from_json(json_dict)

        except Exception as ex:
            Utils.error_handler(f"Couldn't read load config file from file '{config_file_name}'", ex)

    @classmethod
    def pre_ingestion_querying(cls, config: ConfigJson, kusto_client: KustoClient) -> None:
        """
        First phase, pre ingestion - will reach the provided DB with several control commands and a query based on the configuration File.
        :param config: ConfigJson object containing the SampleApp configuration
        :param kusto_client: Client to run commands
        """
        if config.use_existing_table:
            if config.alter_table:
                # Tip: Usually table was originally created with a schema appropriate for the data being ingested, so this wouldn't be needed.
                # Learn More: For more information about altering table schemas,
                #  see: https://docs.microsoft.com/azure/data-explorer/kusto/management/alter-table-command
                cls.wait_for_user_to_proceed(f"Alter-merge existing table '{config.database_name}.{config.table_name}' to align with the provided schema")
                cls.alter_merge_existing_table_to_provided_schema(kusto_client, config.database_name, config.table_name, config.table_schema)
            if config.query_data:
                # Learn More: For more information about Kusto Query Language (KQL), see: https://docs.microsoft.com/azure/data-explorer/write-queries
                cls.wait_for_user_to_proceed(f"Get existing row count in '{config.database_name}.{config.table_name}'")
                cls.query_existing_number_of_rows(kusto_client, config.database_name, config.table_name)
        else:
            # Tip: This is generally a one-time configuration
            # Learn More: For more information about creating tables, see: https://docs.microsoft.com/azure/data-explorer/one-click-table
            cls.wait_for_user_to_proceed(f"Create table '{config.database_name}.{config.table_name}'")
            cls.create_new_table(kusto_client, config.database_name, config.table_name, config.table_schema)

        # Learn More:
        # Kusto batches data for ingestion efficiency. The default batching policy ingests data when one of the following conditions are met:
        #  1) More than 1,000 files were queued for ingestion for the same table by the same user
        #  2) More than 1GB of data was queued for ingestion for the same table by the same user
        #  3) More than 5 minutes have passed since the first file was queued for ingestion for the same table by the same user
        # For more information about customizing  ingestion batching policy, see: https://docs.microsoft.com/azure/data-explorer/kusto/management/batchingpolicy

        # TODO: Change if needed. Disabled to prevent an existing batching policy from being unintentionally changed
        if False and config.batching_policy:
            cls.wait_for_user_to_proceed(f"Alter the batching policy for table '{config.database_name}.{config.table_name}'")
            cls.alter_batching_policy(kusto_client, config.database_name, config.table_name, config.batching_policy)

    @classmethod
    def alter_merge_existing_table_to_provided_schema(cls, kusto_client: KustoClient, database_name: str, table_name: str, table_schema: str) -> None:
        """
        Alter-merges the given existing table to provided schema.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param table_schema: Table Schema
        """
        command = f".alter-merge table {table_name} {table_schema}"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def query_existing_number_of_rows(cls, kusto_client: KustoClient, database_name: str, table_name: str) -> None:
        """
        Queries the data on the existing number of rows.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        """
        command = f"{table_name} | count"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def query_first_two_rows(cls, kusto_client: KustoClient, database_name: str, table_name: str) -> None:
        """
        Queries the first two rows of the table.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        """
        command = f"{table_name} | take 2"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def create_new_table(cls, kusto_client: KustoClient, database_name: str, table_name: str, table_schema: str) -> None:
        """
        Creates a new table.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param table_schema: Table Schema
        """
        command = f".create table {table_name} {table_schema}"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def alter_batching_policy(cls, kusto_client: KustoClient, database_name: str, table_name: str, batching_policy: str) -> None:
        """
        Alters the batching policy based on BatchingPolicy in configuration.
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param batching_policy: Ingestion batching policy
        """
        # Tip 1: Though most users should be fine with the defaults, to speed up ingestion, such as during development and in this sample app,
        # we opt to modify the default ingestion policy to ingest data after at most 10 seconds.
        # Tip 2: This is generally a one-time configuration.
        # Tip 3: You can skip the batching for some files using the Flush-Immediately property, though this option should be used with care as it is inefficient
        command = f".alter table {table_name} policy ingestionbatching @'{batching_policy}'"
        Utils.Queries.execute_command(kusto_client, database_name, command)

    @classmethod
    def ingestion(cls, config: ConfigJson, kusto_client: KustoClient, ingest_client: QueuedIngestClient) -> None:
        """
        Second phase - The ingestion process.
        :param config: ConfigJson object
        :param kusto_client: Client to run commands
        :param ingest_client: Client to ingest data
        """

        for data_file in config.data_to_ingest:
            # Tip: This is generally a one-time configuration.
            # Learn More: For more information about providing inline mappings and mapping references,
            # see: https://docs.microsoft.com/azure/data-explorer/kusto/management/mappings
            cls.create_ingestion_mappings(
                data_file.use_existing_mapping,
                kusto_client,
                config.database_name,
                config.table_name,
                data_file.mapping_name,
                data_file.mapping_value,
                data_file.data_format,
            )

            # Learn More: For more information about ingesting data to Kusto in Python, see: https://docs.microsoft.com/azure/data-explorer/python-ingest-data
            cls.ingest_data(
                data_file, data_file.data_format, ingest_client, config.database_name, config.table_name, data_file.mapping_name, config.ignore_first_record
            )

        Utils.Ingestion.wait_for_ingestion_to_complete(config.wait_for_ingest_seconds)

    @classmethod
    def create_ingestion_mappings(
        cls,
        use_existing_mapping: bool,
        kusto_client: KustoClient,
        database_name: str,
        table_name: str,
        mapping_name: str,
        mapping_value: str,
        data_format: DataFormat,
    ) -> None:
        """
        Creates Ingestion Mappings (if required) based on given values.
        :param use_existing_mapping: Flag noting if we should the existing mapping or create a new one
        :param kusto_client: Client to run commands
        :param database_name: DB name
        :param table_name: Table name
        :param mapping_name: Desired mapping name
        :param mapping_value: Values of the new mappings to create
        :param data_format: Given data format
        """
        if use_existing_mapping or not mapping_value:
            return

        ingestion_mapping_kind = data_format.ingestion_mapping_kind.value.lower()
        cls.wait_for_user_to_proceed(f"Create a '{ingestion_mapping_kind}' mapping reference named '{mapping_name}'")

        mapping_name = mapping_name if mapping_name else "DefaultQuickstartMapping" + str(uuid.UUID())[:5]
        mapping_command = f".create-or-alter table {table_name} ingestion {ingestion_mapping_kind} mapping '{mapping_name}' '{mapping_value}'"
        Utils.Queries.execute_command(kusto_client, database_name, mapping_command)

    @classmethod
    def ingest_data(
        cls,
        data_file: ConfigData,
        data_format: DataFormat,
        ingest_client: QueuedIngestClient,
        database_name: str,
        table_name: str,
        mapping_name: str,
        ignore_first_record: bool,
    ) -> None:
        """
        Ingest data from given source.
        :param data_file: Given data source
        :param data_format: Given data format
        :param ingest_client: Client to ingest data
        :param database_name: DB name
        :param table_name: Table name
        :param mapping_name: Desired mapping name
        :param ignore_first_record: Flag noting whether to ignore the first record in the table
        """
        source_type = data_file.source_type
        source_uri = data_file.data_source_uri
        cls.wait_for_user_to_proceed(f"Ingest '{source_uri}' from '{source_type.name}'")

        # Tip: When ingesting json files, if each line represents a single-line json, use MULTIJSON format even if the file only contains one line.
        # If the json contains whitespace formatting, use SINGLEJSON. In this case, only one data row json object is allowed per file.
        data_format = DataFormat.MULTIJSON if data_format == data_format.JSON else data_format

        # Tip: Kusto's Python SDK can ingest data from files, blobs, open streams and pandas dataframes.
        # See the SDK's samples and the E2E tests in azure.kusto.ingest for additional references.
        # Note: No need to add "nosource" option as in that case the "ingestData" flag will be set to false, and it will be imppossible to reach this code segemnt anyway.
        if source_type == SourceType.local_file_source:
            Utils.Ingestion.ingest_from_file(ingest_client, database_name, table_name, source_uri, data_format, ignore_first_record, mapping_name)
        elif source_type == SourceType.blob_source:
            Utils.Ingestion.ingest_from_blob(ingest_client, database_name, table_name, source_uri, data_format, ignore_first_record, mapping_name)
        else:
            Utils.error_handler(f"Unknown source '{source_type}' for file '{source_uri}'")

    @classmethod
    def post_ingestion_querying(cls, kusto_client: KustoClient, database_name: str, table_name: str, config_ingest_data: bool) -> None:
        """
        Third and final phase - simple queries to validate the script ran successfully.
        :param kusto_client: Client to run queries
        :param database_name: DB Name
        :param table_name: Table Name
        :param config_ingest_data: Flag noting whether any data was ingested by the script
        """
        optional_post_ingestion_message = "post-ingestion " if config_ingest_data else ""

        # Note: We poll here the ingestion's target table because monitoring successful ingestions is expensive and not recommended.
        #   Instead, the recommended ingestion monitoring approach is to monitor failures.
        # Learn more: https://docs.microsoft.com/azure/data-explorer/kusto/api/netfx/kusto-ingest-client-status#tracking-ingestion-status-kustoqueuedingestclient
        #   and https://docs.microsoft.com/azure/data-explorer/using-diagnostic-logs
        cls.wait_for_user_to_proceed(f"Get {optional_post_ingestion_message}row count for '{database_name}.{table_name}':")
        cls.query_existing_number_of_rows(kusto_client, database_name, table_name)

        cls.wait_for_user_to_proceed(f"Get {optional_post_ingestion_message}row count for '{database_name}.{table_name}':")
        cls.query_first_two_rows(kusto_client, database_name, table_name)

    @classmethod
    def wait_for_user_to_proceed(cls, prompt_msg: str) -> None:
        """
        Handles UX on prompts and flow of program
        :param prompt_msg: Prompt to display to user
        """
        print()
        print(f"Step {cls.__step}: {prompt_msg}")
        cls.__step = cls.__step + 1
        if cls.config.wait_for_user:
            input("Press ENTER to proceed with this operation...")


def main():
    print("Kusto sample app is starting...")

    app = KustoSampleApp()
    app.load_configs(app.CONFIG_FILE_NAME)

    if app.config.authentication_mode == "UserPrompt":
        app.wait_for_user_to_proceed("You will be prompted *twice* for credentials during this script. Please return to the console after authenticating.")

    kusto_connection_string = Utils.Authentication.generate_connection_string(app.config.kusto_uri, app.config.authentication_mode)
    ingest_connection_string = Utils.Authentication.generate_connection_string(app.config.ingest_uri, app.config.authentication_mode)

    # Tip: Avoid creating a new Kusto/ingest client for each use.Instead, create the clients once and reuse them.
    if not kusto_connection_string or not ingest_connection_string:
        Utils.error_handler("Connection String error. Please validate your configuration file.")
    else:
        # Make sure to use context manager for the Kusto client, as it will close the underlying resources properly.
        # Alternatively, you can use the client as a regular Python object and call close() on it.
        with KustoClient(kusto_connection_string) as kusto_client:
            with QueuedIngestClient(ingest_connection_string) as ingest_client:

                app.pre_ingestion_querying(app.config, kusto_client)

                if app.config.ingest_data:
                    app.ingestion(app.config, kusto_client, ingest_client)

                if app.config.query_data:
                    app.post_ingestion_querying(kusto_client, app.config.database_name, app.config.table_name, app.config.ingest_data)

    print("\nKusto sample app done")


if __name__ == "__main__":
    main()

import enum
import json
from typing import ClassVar

from azure.kusto.ingest import QueuedIngestClient

from quick_start.utils import Utils, AuthenticationModeOptions

from azure.kusto.data import DataFormat, KustoClient


class SourceType(enum.Enum):
    """
    SourceType - represents the type of files used for ingestion
    """
    localfilesource = "localFileSource",
    blobsource = "blobSource",


class ConfigData:
    """
    ConfigData object - represents a file from which to ingest
    """
    source_type: ClassVar[SourceType]
    data_source_uri: ClassVar[str]
    data_format: ClassVar[DataFormat]
    use_existing_mapping: ClassVar[bool]
    mapping_name: ClassVar[str]
    mapping_value: ClassVar[str]


class ConfigJson:
    """
    ConfigJson object - represents a cluster and DataBase connection configuration file.
    """
    use_existing_table: ClassVar[bool]
    database_name: ClassVar[str]
    table_name: ClassVar[str]
    table_schema: ClassVar[str]
    kusto_uri: ClassVar[str]
    ingest_uri: ClassVar[str]
    data_to_ingest: ClassVar[list[ConfigData]]
    alter_table: ClassVar[bool]
    query_data: ClassVar[bool]
    ingest_data: ClassVar[bool]
    authentication_mode: ClassVar[AuthenticationModeOptions]
    wait_for_user: ClassVar[bool]
    wait_for_ingest_seconds: ClassVar[bool]
    batching_policy: ClassVar[str]


class KustoSampleApp:
    # TODO (config):
    #  If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details
    #  If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately
    CONFIG_FILE_NAME = "kusto_sample_config.json"

    __step = 1
    config = ConfigJson()

    @classmethod
    def load_configs(cls, config_file_name: str) -> None:
        """
        Loads JSON configuration file, and sets the metadata in place.
        :param config_file_name: Configuration file path.
        """
        try:
            with open(config_file_name, "r") as config_file:
                json_dict = json.load(config_file)

        except Exception as ex:
            Utils.error_handler(f"Couldn't read load config file from file '{config_file_name}'", ex)

        cls.config.use_existing_table = json_dict["useExistingTable"]
        cls.config.database_name = json_dict["databaseName"]
        cls.config.table_name = json_dict["tableName"]
        cls.config.table_schema = json_dict["tableSchema"]
        cls.config.kusto_uri = json_dict["kustoUri"]
        cls.config.ingest_uri = json_dict["ingestUri"]
        cls.config.data_to_ingest = json_dict["data"]
        cls.config.alter_table = json_dict["alterTable"]
        cls.config.query_data = json_dict["queryData"]
        cls.config.ingest_data = json_dict["ingestData"]
        cls.config.authentication_mode = json_dict["AuthenticationMode"]
        cls.config.wait_for_user = json_dict["WaitForUser"]
        cls.config.wait_for_ingest_seconds = json_dict["WaitForIngestSeconds"]
        cls.config.batching_policy = json_dict["BatchingPolicy"]

        if (
                cls.config.database_name is None
                or cls.config.table_name is None
                or cls.config.table_schema is None
                or cls.config.kusto_uri is None
                or cls.config.ingest_uri is None
                or cls.config.data_to_ingest is None
                or cls.config.authentication_mode is None
                or cls.config.wait_for_user is None
                or cls.config.wait_for_ingest_seconds is None
        ):
            Utils.error_handler(f"File '{config_file_name}' is missing required fields")

    @classmethod
    def pre_ingestion_querying(cls, config: ConfigJson, kusto_client: KustoClient):
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
        kusto_client = KustoClient(kusto_connection_string)
        ingest_client = QueuedIngestClient(ingest_connection_string)

        app.pre_ingestion_querying(app.config, kusto_client)

        if app.config.ingestData:
            await app.ingestionAsync(app.config, kusto_client, ingest_client)

        if app.config.queryData:
            await app.postIngestionQueryingAsync(kusto_client, app.config.databaseName, app.config.tableName, app.config.ingestData)

    print("\nKusto sample app done")


if __name__ == "__main__":
    main()

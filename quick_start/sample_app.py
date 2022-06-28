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
    wait_for_user: ClassVar[bool]
    config: ClassVar[ConfigJson]

    @classmethod
    def load_configs(cls, config_file_name: str) -> None:
        """
        Loads JSON configuration file, and sets the metadata in place.
        :param config_file_name: Configuration file path.
        """
        try:
            with open(config_file_name, "r") as config_file:
                cls.config = json.load(config_file)
        except Exception as ex:
            Utils.error_handler(f"Couldn't read load config file from file '{config_file_name}'", ex)

    @classmethod
    def wait_for_user_to_proceed(cls, prompt_msg: str) -> None:
        """
        Handles UX on prompts and flow of program
        :param prompt_msg: Prompt to display to user
        """
        print()
        print(f"Step {cls.__step}: {prompt_msg}")
        cls._step = cls.__step + 1
        if cls.wait_for_user:
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

        app.preIngestionQueryingAsync(app.config, kusto_client)

        if app.config.ingestData:
            await app.ingestionAsync(app.config, kusto_client, ingest_client)

        if app.config.queryData:
            await app.postIngestionQueryingAsync(kusto_client, app.config.databaseName, app.config.tableName, app.config.ingestData)

    print("Kusto sample app done")


if __name__ == "__main__":
    main()

import enum
import os
import uuid
from time import sleep
from tqdm import tqdm
from azure.kusto.data import KustoConnectionStringBuilder, ClientRequestProperties, KustoClient, DataFormat
from azure.kusto.data.exceptions import KustoClientError, KustoServiceError
from azure.kusto.ingest import IngestionProperties, BaseIngestClient, QueuedIngestClient, FileDescriptor, BlobDescriptor


class AuthenticationModeOptions(enum.Enum):
    """
    AuthenticationModeOptions - represents the different options to autenticate to the system
    """
    UserPrompt = "UserPrompt",
    ManagedIdentity = "ManagedIdentity",
    AppKey = "AppKey",
    AppCertificate = "AppCertificate"


class Utils:
    class Authentication:
        """
        Authentication module of Utils - in charge of authenticating the user with the system
        """

        @classmethod
        def generate_connection_string(cls, cluster_url: str, authentication_mode: AuthenticationModeOptions) -> KustoConnectionStringBuilder:
            """
            Generates Kusto Connection String based on given Authentication Mode.
            :param cluster_url: Cluster to connect to.
            :param authentication_mode: User Authentication Mode, Options: (UserPrompt|ManagedIdentity|AppKey|AppCertificate)
            :return: A connection string to be used when creating a Client
            """
            # Learn More: For additional information on how to authorize users and apps in Kusto,
            # see: https://docs.microsoft.com/azure/data-explorer/manage-database-permissions

            if authentication_mode == AuthenticationModeOptions.UserPrompt.name:
                # Prompt user for credentials
                return KustoConnectionStringBuilder.with_interactive_login(cluster_url)

            elif authentication_mode == AuthenticationModeOptions.ManagedIdentity.name:
                # Authenticate using a System-Assigned managed identity provided to an azure service, or using a User-Assigned managed identity.
                # For more information, see https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview
                return cls.create_managed_identity_connection_string(cluster_url)

            elif authentication_mode == AuthenticationModeOptions.AppKey.name:
                # Learn More: For information about how to procure an AAD Application,
                # see: https://docs.microsoft.com/azure/data-explorer/provision-azure-ad-app
                # TODO (config - optional): App ID & tenant, and App Key to authenticate with
                return KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster_url, os.environ.get("APP_ID"), os.environ.get("APP_KEY"),
                                                                                            os.environ.get("APP_TENANT"))

            elif authentication_mode == AuthenticationModeOptions.AppCertificate.name:
                return cls.create_application_certificate_connection_string(cluster_url)

            else:
                Utils.error_handler(f"Authentication mode '{authentication_mode}' is not supported")

        @classmethod
        def create_managed_identity_connection_string(cls, cluster_url: str) -> KustoConnectionStringBuilder:
            """
            Generates Kusto Connection String based on 'ManagedIdentity' Authentication Mode.
            :param cluster_url: Url of cluster to connect to
            :return: ManagedIdentity Kusto Connection String
            """
            # Connect using the system- or user-assigned managed identity (Azure service only)
            # TODO (config - optional): Managed identity client ID if you are using a user-assigned managed identity
            client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID")
            return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_url, client_id=client_id) if client_id \
                else KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_url)

        @classmethod
        def create_application_certificate_connection_string(cls, cluster_url: str) -> KustoConnectionStringBuilder:
            """
            Generates Kusto Connection String based on 'AppCertificate' Authentication Mode.
            :param cluster_url: Url of cluster to connect to
            :return: AppCertificate Kusto Connection String
            """

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
                Utils.error_handler(f"Failed to load PEM file from {private_key_pem_file_path}", ex)

            if public_cert_file_path:
                try:
                    with open(public_cert_file_path, "r") as cert_file:
                        public_certificate = cert_file.read()
                except Exception as ex:
                    Utils.error_handler(f"Failed to load public certificate file from {public_cert_file_path}", ex)

                return KustoConnectionStringBuilder.with_aad_application_certificate_sni_authentication(cluster_url, app_id, pem_certificate,
                                                                                                        public_certificate, cert_thumbprint, app_tenant)
            else:
                return KustoConnectionStringBuilder.with_aad_application_certificate_authentication(cluster_url, app_id, pem_certificate, cert_thumbprint,
                                                                                                    app_tenant)

    class Queries:
        """
        Queries module of Utils - in charge of querying the data - either with management queries, or data queries
        """
        MGMT_PREFIX = "."

        @classmethod
        def create_client_request_properties(cls, scope: str, timeout: str = None) -> ClientRequestProperties:
            """
            Creates a fitting ClientRequestProperties object, to be used when executing control commands or queries.
            :param scope: Working scope
            :param timeout: Requests default timeout
            :return: ClientRequestProperties object
            """
            client_request_properties = ClientRequestProperties()
            client_request_properties.client_request_id = f"{scope};{str(uuid.uuid4())}"
            client_request_properties.application = "sample_app.py"

            # Tip: Though uncommon, you can alter the request default command timeout using the below command, e.g. to set the timeout to 10 minutes, use "10m"
            if timeout:
                client_request_properties.set_option(ClientRequestProperties.request_timeout_option_name, timeout)

            return client_request_properties

        @classmethod
        def execute_command(cls, kusto_client: KustoClient, database_name: str, command: str) -> bool:
            """
            Executes a Command using a premade client
            :param kusto_client: Premade client to run Commands. can be either an adminClient or queryClient
            :param database_name: DB name
            :param command: The Command to execute
            :return: True on success, false otherwise
            """
            try:
                if command.startswith(cls.MGMT_PREFIX):
                    client_request_properties = cls.create_client_request_properties("Python_SampleApp_ControlCommand")
                else:
                    client_request_properties = cls.create_client_request_properties("Python_SampleApp_Query")

                result = kusto_client.execute(database_name, command, client_request_properties)
                print(f"Response from executed command '{command}':")
                for row in result.primary_results[0]:
                    print(row.to_list())

                return True

            except KustoClientError as ex:
                Utils.error_handler(f"Client error while trying to execute command '{command}' on database '{database_name}'", ex)
            except KustoServiceError as ex:
                Utils.error_handler(f"Server error while trying to execute command '{command}' on database '{database_name}'", ex)
            except Exception as ex:
                Utils.error_handler(f"Unknown error while trying to execute command '{command}' on database '{database_name}'", ex)

            return False

    class Ingestion:
        """
        Ingestion module of Utils - in charge of ingesting the given data - based on the configuration file.
        """

        @classmethod
        def create_ingestion_properties(cls, database_name: str, table_name: str, data_format: DataFormat, mapping_name: str) -> IngestionProperties:
            """
            Creates a fitting KustoIngestionProperties object, to be used when executing ingestion commands.
            :param database_name: DB name
            :param table_name: Table name
            :param data_format: Given data format
            :param mapping_name: Desired mapping name
            :return: IngestionProperties object
            """
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
        def ingest_from_file(cls, ingest_client: BaseIngestClient, database_name: str, table_name: str, file_path: str, data_format: DataFormat,
                             mapping_name: str = None) -> None:
            """
            Ingest Data from a given file path.
            :param ingest_client: Client to ingest data
            :param database_name: DB name
            :param table_name: Table name
            :param file_path: File path
            :param data_format: Given data format
            :param mapping_name: Desired mapping name
            """
            ingestion_properties = cls.create_ingestion_properties(database_name, table_name, data_format, mapping_name)

            # Tip 1: For optimal ingestion batching and performance,specify the uncompressed data size in the file descriptor instead of the default below of 0.
            # Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
            # Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere
            file_descriptor = FileDescriptor(file_path, size=0, source_id=uuid.uuid4())
            ingest_client.ingest_from_file(file_descriptor, ingestion_properties=ingestion_properties)

        @classmethod
        def ingest_from_blob(cls, ingest_client: QueuedIngestClient, database_name: str, table_name: str, blob_url: str, data_format: DataFormat,
                             mapping_name: str = None) -> None:
            """
            Ingest Data from a Blob.
            :param ingest_client: Client to ingest data
            :param database_name: DB name
            :param table_name: Table name
            :param blob_url: Blob Uri
            :param data_format: Given data format
            :param mapping_name: Desired mapping name
            """
            ingestion_properties = cls.create_ingestion_properties(database_name, table_name, data_format, mapping_name)

            # Tip 1: For optimal ingestion batching and performance,specify the uncompressed data size in the file descriptor instead of the default below of 0.
            # Otherwise, the service will determine the file size, requiring an additional s2s call, and may not be accurate for compressed files.
            # Tip 2: To correlate between ingestion operations in your applications and Kusto, set the source ID and log it somewhere
            blob_descriptor = BlobDescriptor(blob_url, size=0, source_id=str(uuid.uuid4()))
            ingest_client.ingest_from_blob(blob_descriptor, ingestion_properties=ingestion_properties)

        @classmethod
        def wait_for_ingestion_to_complete(cls, wait_for_ingest_seconds: int) -> None:
            """
            Halts the program for WaitForIngestSeconds, allowing the queued ingestion process to complete.
            :param wait_for_ingest_seconds: Sleep time to allow for queued ingestion to complete.
            """
            print(f"Sleeping {wait_for_ingest_seconds} seconds for queued ingestion to complete. Note: This may take longer depending on the file size "
                  f"and ingestion batching policy.")

            for x in tqdm(range(wait_for_ingest_seconds, 0, -1)):
                sleep(1)

    @staticmethod
    def error_handler(error: str, e: Exception = None) -> None:
        """
        Error handling function. Will mention the appropriate error message (and the exception itself if exists), and will quit the program.
        :param error: Appropriate error message received from calling function
        :param e: Thrown exception
        """
        print(f"Script failed with error: {error}")
        if e:
            print(f"Exception: {e}")

        exit(-1)

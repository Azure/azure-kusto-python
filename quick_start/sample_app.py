import enum
from typing import ClassVar


class AuthenticationModeOptions(enum.Enum):
    """
    AuthenticationModeOptions - represents the different options to autenticate to the system
    """
    UserPrompt = "UserPrompt",
    ManagedIdentity = "ManagedIdentity",
    AppKey = "AppKey",
    AppCertificate = "AppCertificate"


class KustoSampleApp:
    # TODO (config):
    #  If this quickstart app was downloaded from OneClick, kusto_sample_config.json should be pre-populated with your cluster's details
    #  If this quickstart app was downloaded from GitHub, edit kusto_sample_config.json and modify the cluster URL and database fields appropriately
    CONFIG_FILE_NAME = "kusto_sample_config.json"

    __step = 1
    wait_for_user: ClassVar[bool]


def main():
    print("Kusto sample app is starting...")

    print("Kusto sample app done")


if __name__ == "__main__":
    main()

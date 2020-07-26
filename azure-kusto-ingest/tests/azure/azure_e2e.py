# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import os
import subprocess
from azure.identity import ManagedIdentityCredential
from azure.core import credentials
from azure.kusto.data import KustoConnectionStringBuilder, KustoClient


def main():
    # Connection details
    cluster_name = os.environ.get("CLUSTER_NAME")
    cluster_uri = "https://" + cluster_name + ".kusto.windows.net"

    # MSI based auth
    user_msi_object_id = os.environ.get("MSI_OBJECT_ID")
    user_msi_client_id = os.environ.get("MSI_CLIENT_ID")

    # certificate based auth
    certificate_file = os.environ.get("CERTIFICATE_FILE")
    certificate_private_key = os.environ.get("CERTIFICATE_PEM_FILE")
    certificate_thumbprint = os.environ.get("CERTIFICATE_THUMBPRINT")
    certificate_authority = os.environ.get("CERTIFICATE_AUTHORITY")

    # App ID and Key based auth
    app_id = os.environ.get("APP_ID")
    app_key = os.environ.get("APP_KEY")

    ###############################################################################

    # Test MSI auth
    kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_uri)
    run_test("Connect with system MSI", lambda: ping_server(kcsb))

    if user_msi_client_id is not None:
        kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_uri, client_id=user_msi_client_id)
        run_test("Connect with client id MSI", lambda: ping_server(kcsb))

    if user_msi_object_id is not None:
        kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_uri, object_id=user_msi_object_id)
        run_test("Connect with object id MSI", lambda: ping_server(kcsb))

    # Test App Id and Key auth
    if app_key is not None and app_id is not None:
        kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster_uri, app_id, app_key)
        run_test("Connect with App Id & Key", lambda: ping_server(kcsb))

    # Test Certificate auth
    if app_id is not None and certificate_file is not None and certificate_private_key is not None and certificate_authority is not None:
        kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_authentication(cluster_uri, app_id, certificate_private_key,
                                                                                            certificate_thumbprint, certificate_authority)
        run_test("Connect with certificate", lambda: ping_server(kcsb))

        kcsb = KustoConnectionStringBuilder.with_aad_application_certificate_authentication(cluster_uri, app_id, certificate_private_key,
                                                                                            certificate_thumbprint, certificate_authority, certificate_file)
        run_test("Connect with certificate subject name and issuer", lambda: ping_server(kcsb))

    # Test manual options
    token = None
    try:
        token_provider = lambda: ManagedIdentityCredential().get_token(cluster_uri).token
        token = token_provider()

        kcsb = KustoConnectionStringBuilder.with_token_provider(cluster_uri, token_provider)
        run_test("Connect with token provider", lambda: ping_server(kcsb))

        kcsb = KustoConnectionStringBuilder.with_aad_user_token_authentication(cluster_uri, token)
        run_test("Connect with user provided token", lambda: ping_server(kcsb))
    except Exception as e:
        print("failed to get system MSI token. Skipping token & token-provider tests")
        print(e)

    # Test Az Cli
    az_installed = subprocess.run(["where", "az"], capture_output=True)
    if  az_installed.returncode == 0:
        print("Trying login with az-cli. This may fail if refresh token is expired")
        kcsb.with_az_cli_authentication(cluster_uri)
        run_test("Connect with az-cli", lambda: ping_server(kcsb))


def run_test(name: str, test: callable):
    try:
        print("Testing: " + name + "... ", end="")
        test()
        print("Succeeded")
    except Exception as e:
        print("Failed")
        print(e)


def ping_server(kcsb: KustoConnectionStringBuilder):
    client = KustoClient(kcsb)
    res = client.execute_mgmt("", ".show version")
    if res.errors_count > 0:
        raise Exception("calling '.show version' ended with errors:\n" + str.join("\n", res.get_exceptions()))


# Run the tests
main()

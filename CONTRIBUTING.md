# Contributing to Azure Python SDK

If you would like to become an active contributor to this project please
follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](https://azure.github.io/azure-sdk/python_documentation.html).

## Prerequisites
In order to run E2E tests, you need a Kusto database you have admin rights on.
A Kusto free cluster is the easiest way to acquire one.
You can cerate a free Kusto cluster here: https://dataexplorer.azure.com/home

Make sure you set streaming ingestion to enabled in the cluster's configuration:  
https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming?tabs=azure-portal%2Ccsharp

You should set then following environment vars where you run E2Es (in IDE run config, shell window, computer, etc).
```shell
ENGINE_CONNECTION_STRING=<Your cluster URI>
DM_CONNECTION_STRING=<Your ingest cluster URI> # Optional - if not set, will infer from ENGINE_CONNECTION_STRING
TEST_DATABASE=<The name of the database>
```

The E2E tests authenticate with DefaultAzureCredential, and will fall back to interactive login if needed.


For more information on DefaultAzureCredential, see:  
https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python

To run the optional `token_provider` tests, you will need to set the booleans at the top of the file `test_token_providers.py` and the following environment variables in addition to the previous ones:
```shell
# app key provider:
AZURE_CLIENT_ID=<Your client ID>
AZURE_CLIENT_SECRET=<Your client secret>
AZURE_TENANT_ID=<Your tenant ID>
# certificate provider:
CERT_THUMBPRINT=<Your certificate thumbprint>
CERT_PUBLIC_CERT_PATH=<Your certificate public cert path>
CERT_PEM_KEY_PATH=<Your certificate private key path>
# managed identity provider:
MSI_OBJECT_ID=<Your managed identity object ID>
MSI_CLIENT_ID=<Your managed identity client ID>
# user password provider:
USER_NAME=<Your user name>
USER_PASS=<Your user password>
USER_AUTH_ID=<Your user auth ID> # optional
```

## Requirements

In order to work on this project, we recommend using the dev requirements:

```bash
pip install -r dev_requirements.txt
```

Alternatively, you can use uv (a faster Python package installer and resolver):

```bash
pip install uv
uv pip install -r dev_requirements.txt
```

These including testing related packages as well as styling ([black](https://black.readthedocs.io/en/stable/))

## Building and Testing

This project uses [pytest](https://docs.pytest.org/en/latest/).


Since the tests use the package as a third-party, the easiest way to set it up is installing it in edit mode:

```bash
pip install -e ./azure-kusto-data ./azure-kusto-ingest
```

Or with uv:

```bash
uv pip install -e ./azure-kusto-data ./azure-kusto-ingest
```

After which, running tests is simple.

Just run:

```bash
pytest ./azure-kusto-data ./azure-kusto-ingest 
```

## Style

We use black, and allow for line-length of 160, so please run:

```bash
black --line-length=160 ./azure-kusto-data ./azure-kusto-ingest
```

## PRs
We welcome contributions. In order to make the PR process efficient, please follow the below checklist:

* **There is an issue open concerning the code added** - (either bug or enhancement).
    Preferably there is an agreed upon approach in the issue.
* **PR comment explains the changes done** - (This should be a TL;DR; as the rest of it should be documented in the related issue).
* **PR is concise** - try and avoid make drastic changes in a single PR. Split it into multiple changes if possible. If you feel a major change is needed, it is ok, but make sure commit history is clear and one of the maintainers can comfortably review both the code and the logic behind the change. 
* **Please provide any related information needed to understand the change** - docs, guidelines, use-case, best practices and so on. Opinions are accepted, but have to be backed up.
* **Checks should pass** - these including linting with black and running tests.

## Code of Conduct
This project's code of conduct can be found in the
[CODE_OF_CONDUCT.md file](https://github.com/Azure/azure-sdk-for-python/blob/master/CODE_OF_CONDUCT.md)
(v1.4.0 of the http://contributor-covenant.org/ CoC).

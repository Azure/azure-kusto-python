# Contributing to Azure Python SDK

If you would like to become an active contributor to this project please
follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](https://azure.github.io/azure-sdk/python_documentation.html).

## Prerequisites

### Cluster
In order to run E2E tests, you need a Kusto database you have admin rights on.
A Kusto free cluster is the easiest way to acquire one.
You can cerate a free Kusto cluster here: https://dataexplorer.azure.com/home

Make sure you set streaming ingestion to enabled in the cluster's configuration:  
https://learn.microsoft.com/en-us/azure/data-explorer/ingest-data-streaming?tabs=azure-portal%2Ccsharp


### Tools

The project uses [uv](https://docs.astral.sh/uv/) as the project manager.
You will need to install it in order to develop on this project.  

See [Installing uv](https://docs.astral.sh/uv/getting-started/installation/).

Additionally, the project uses [poethepoet](https://poethepoet.natn.io/index.html) as the task runner.

You can install it with uv:

```bash
uv tool install poethepoet

poe <command>
```

Or you can use `uvx` to use it directly without installing:

```bash
uvx poe <command>
```


### Environment Variables

Some tests require you to set environment variables to run.

The project supports a .env file, which you can create in the root of the project.  
See [.env.example](./.env.example) for an example of a .env file.

#### E2E Tests Environment Variables

```shell
ENGINE_CONNECTION_STRING=<Your cluster URI> # Format: https://<cluster_name>.<region>.kusto.windows.net
DM_CONNECTION_STRING=<Your ingest cluster URI> # Format: https://ingest-<cluster_name>.<region>.kusto.windows.net
TEST_DATABASE=<The name of the database>
```

The E2E tests authenticate with [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python).

It is recommended to use the [azure cli](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest) command `az login` to authenticate with your Azure account to run the tests.

#### Token Provider Tests

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


## Development

As mentioned before, this project uses `uv` and `poe` to manage the development environment and tasks.

To see the tasks available, you can run:

```bash
poe
```

### Setup

Setup the project by syncing `uv`:

```bash
poe sync
```

This will install the dependencies, set up the virtual environment, and install the tools needed for development.

### Testing

This project uses [pytest](https://docs.pytest.org/en/latest/).

To test the project, simply run:

```bash
poe test
```

To test without E2E tests, you can run:

```bash
poe test --no-e2e
```

### Style

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting.

Before commiting your changes, make sure the code is properly formatted, or the commit will be rejected.

```bash
    poe format # formats the code directly
    poe check-format # returns a diff of what needs to be formatted    
```

Also make sure to lint the code:

```bash
    poe lint
    
    # You can auto-fix some issues with:
    poe lint --fix
```

### Types

This project uses [basedpyright](https://docs.basedpyright.com/) for type checking.

To check the types, run:

```bash
poe types
```

Note that we have a `baseline.json` file that is used to ignore old type errors, as we gradually add types to the codebase.

## PRs
We welcome contributions. In order to make the PR process efficient, please follow the below checklist:

* **There is an issue open concerning the code added** - (either bug or enhancement).
    Preferably there is an agreed upon approach in the issue.
* **PR comment explains the changes done** - (This should be a TL;DR; as the rest of it should be documented in the related issue).
* **PR is concise** - try and avoid make drastic changes in a single PR. Split it into multiple changes if possible. If you feel a major change is needed, it is ok, but make sure commit history is clear and one of the maintainers can comfortably review both the code and the logic behind the change. 
* **Please provide any related information needed to understand the change** - docs, guidelines, use-case, best practices and so on. Opinions are accepted, but have to be backed up.
* **Checks should pass** - these including linting with black and running tests.

# Code of Conduct
This project's code of conduct can be found in the
[CODE_OF_CONDUCT.md file](https://github.com/Azure/azure-sdk-for-python/blob/master/CODE_OF_CONDUCT.md)
(v1.4.0 of the http://contributor-covenant.org/ CoC).

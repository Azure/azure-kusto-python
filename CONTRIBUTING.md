# Contributing to Azure Python SDK

If you would like to become an active contributor to this project please
follow the instructions provided in [Microsoft Azure Projects Contribution Guidelines](https://azure.github.io/azure-sdk/python_documentation.html).

## Requirements

In order to work on this project, we recommend using the dev requirements:

```bash
pip install -r dev_requirements.txt
```

These including testing related packages as well as styling ([black](https://black.readthedocs.io/en/stable/))

## Building and Testing

This project uses [pytest](https://docs.pytest.org/en/latest/).


Since the tests use the package as a third-party, the easiest way to set it up is installing it in edit mode:

```bash
pip install -e ./azure-kusto-data ./azure-kusto-ingest
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

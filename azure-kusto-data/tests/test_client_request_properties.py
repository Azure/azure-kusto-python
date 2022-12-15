# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License

from datetime import timedelta

from azure.kusto.data import ClientRequestProperties, KustoConnectionStringBuilder
from azure.kusto.data.client_base import ExecuteRequestParams


def test_properties():
    """positive tests"""
    defer = False
    timeout = timedelta(seconds=10)

    crp = ClientRequestProperties()
    crp.set_option(ClientRequestProperties.results_defer_partial_query_failures_option_name, defer)
    crp.set_option(ClientRequestProperties.request_timeout_option_name, timeout)

    result = crp.to_json()

    assert '"{0}": false'.format(crp.results_defer_partial_query_failures_option_name) in result
    assert '"{0}": "0:00:10"'.format(ClientRequestProperties.request_timeout_option_name) in result

    assert crp.client_request_id is None
    assert crp.application is None
    assert crp.user is None

    crp.client_request_id = "CRID"
    assert crp.client_request_id == "CRID"

    crp.application = "myApp"
    assert crp.application == "myApp"

    crp.user = "myUser"
    assert crp.user == "myUser"


def test_default_tracing_properties():
    kcsb = KustoConnectionStringBuilder("test")
    params = ExecuteRequestParams(
        "somedatabase",
        None,
        ClientRequestProperties(),
        "somequery",
        timedelta(seconds=10),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.application_for_tracing,
        kcsb.user_for_tracing,
        kcsb.client_version_for_tracing,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert len(params.request_headers["x-ms-client-application"]) > 0
    assert len(params.request_headers["x-ms-client-user"]) > 0
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")


def test_custom_kcsb_tracing_properties():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb.application_for_tracing = "myApp"
    kcsb.user_for_tracing = "myUser"
    kcsb.client_version_for_tracing = "myVersion"

    params = ExecuteRequestParams(
        "somedatabase",
        None,
        ClientRequestProperties(),
        "somequery",
        timedelta(seconds=10),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.application_for_tracing,
        kcsb.user_for_tracing,
        kcsb.client_version_for_tracing,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None

    assert params.request_headers["x-ms-client-application"] == "myApp"
    assert params.request_headers["x-ms-client-user"] == "myUser"
    assert params.request_headers["x-ms-client-version"] == "myVersion"


def test_custom_crp_tracing_properties():
    kcsb = KustoConnectionStringBuilder("test")
    crp = ClientRequestProperties()
    crp.application = "myApp2"
    crp.user = "myUser2"
    crp.client_version = "myVersion2"

    params = ExecuteRequestParams(
        "somedatabase",
        None,
        crp,
        "somequery",
        timedelta(seconds=10),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.application_for_tracing,
        kcsb.user_for_tracing,
        kcsb.client_version_for_tracing,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-client-application"] == "myApp2"
    assert params.request_headers["x-ms-client-user"] == "myUser2"
    assert params.request_headers["x-ms-client-version"] == "myVersion2"


def test_custom_crp_tracing_properties_override_kcsb():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb.application_for_tracing = "myApp"
    kcsb.user_for_tracing = "myUser"
    kcsb.client_version_for_tracing = "myVersion"
    crp = ClientRequestProperties()
    crp.application = "myApp2"
    crp.user = "myUser2"
    crp.client_version = "myVersion2"

    params = ExecuteRequestParams(
        "somedatabase",
        None,
        crp,
        "somequery",
        timedelta(seconds=10),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.application_for_tracing,
        kcsb.user_for_tracing,
        kcsb.client_version_for_tracing,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-client-application"] == "myApp2"
    assert params.request_headers["x-ms-client-user"] == "myUser2"
    assert params.request_headers["x-ms-client-version"] == "myVersion2"

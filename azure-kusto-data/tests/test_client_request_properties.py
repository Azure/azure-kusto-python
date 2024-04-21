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
    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        ClientRequestProperties(),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert len(params.request_headers["x-ms-app"]) > 0
    assert len(params.request_headers["x-ms-user"]) > 0
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")


def test_custom_kcsb_tracing_properties():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb.application_for_tracing = "myApp"
    kcsb.user_name_for_tracing = "myUser"

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        ClientRequestProperties(),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None

    assert params.request_headers["x-ms-app"] == "myApp"
    assert params.request_headers["x-ms-user"] == "myUser"


def test_custom_crp_tracing_properties():
    kcsb = KustoConnectionStringBuilder("test")
    crp = ClientRequestProperties()
    crp.application = "myApp2"
    crp.user = "myUser2"

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        crp,
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-app"] == "myApp2"
    assert params.request_headers["x-ms-user"] == "myUser2"
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")


def test_custom_crp_tracing_properties_override_kcsb():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb.application_for_tracing = "myApp"
    kcsb.user_name_for_tracing = "myUser"
    crp = ClientRequestProperties()
    crp.application = "myApp2"
    crp.user = "myUser2"

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        crp,
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-app"] == "myApp2"
    assert params.request_headers["x-ms-user"] == "myUser2"
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")


def test_set_connector_name_and_version():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb._set_connector_details("myConnector", "myVersion", send_user=False)
    crp = ClientRequestProperties()

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        ClientRequestProperties(),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-user"] == "NONE"
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")

    assert params.request_headers["x-ms-app"].startswith("Kusto.myConnector:myVersion|App.")


def test_set_connector_no_app_version():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb._set_connector_details("myConnector", "myVersion", app_name="myApp", send_user=True)
    crp = ClientRequestProperties()

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        ClientRequestProperties(),
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert len(params.request_headers["x-ms-user"]) > 0
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")

    assert params.request_headers["x-ms-app"].startswith("Kusto.myConnector:myVersion|App.")


def test_set_connector_full():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb._set_connector_details(
        "myConnector",
        "myVersion",
        app_name="myApp",
        app_version="myAppVersion",
        send_user=True,
        override_user="myUser",
        additional_fields=[("myField", "myValue")],
    )
    crp = ClientRequestProperties()

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        crp,
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-user"] == "myUser"
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")

    assert params.request_headers["x-ms-app"] == "Kusto.myConnector:myVersion|App.myApp:myAppVersion|myField:myValue"


def test_set_connector_escaped():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb._set_connector_details(
        "Café",
        "1 . 0",
        app_name=r"my|test\{}\app",
        app_version="s" * 1000,
        send_user=True,
        override_user="myUser",
        additional_fields=[("myField", "myValue")],
    )
    crp = ClientRequestProperties()

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        crp,
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-user"] == "myUser"
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")

    assert (
        params.request_headers["x-ms-app"] == "Kusto.Caf_:1_._0"
        "|App.my_test____app:ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss|myField:myValue"
    )


def test_kcsb_direct_escaped():
    kcsb = KustoConnectionStringBuilder("test")
    kcsb.application_for_tracing = "Café"
    kcsb.user_name_for_tracing = "user with spaces"
    crp = ClientRequestProperties()

    params = ExecuteRequestParams._from_query(
        "somequery",
        "somedatabase",
        crp,
        {},
        timedelta(seconds=10),
        timedelta(seconds=10),
        timedelta(seconds=10),
        kcsb.client_details,
    )

    assert params.request_headers["x-ms-client-request-id"] is not None
    assert params.request_headers["x-ms-user"] == "user_with_spaces"
    assert params.request_headers["x-ms-client-version"].startswith("Kusto.Python.Client:")

    assert params.request_headers["x-ms-app"] == "Caf_"

"""Base class for logging tests
"""
import os
import sys
import time
import random
import pandas
import logging

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat
from azure.kusto.logging import (
    KustoHandler,
)


class BaseTestKustoLogging:
    """Base class for logging tests."""

    @classmethod
    def setup_class(cls):
        """create the Kusto table and initialize kcsb info"""

        global has_one_test_failed

        has_one_test_failed = False

        cls.is_live_testing_ready = True

        engine_cs = os.environ.get("ENGINE_CONNECTION_STRING")
        app_id = os.environ.get("APP_ID")
        app_key = os.environ.get("APP_KEY")
        auth_id = os.environ.get("AUTH_ID")

        if engine_cs and app_id and app_key and auth_id:

            cls.kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(engine_cs, app_id, app_key, auth_id)

            cls.test_db = os.environ.get("TEST_DATABASE")
            cls.client = KustoClient(cls.kcsb)

            python_version = "_".join([str(v) for v in sys.version_info[:3]])
            cls.test_table = "python_test_{0}_{1}_{2}".format(python_version, str(int(time.time())), random.randint(1, 100000))

            with open("azure-kusto-logging/tests/createTable.kql") as table_template:
                tbl_create = table_template.read()
            cls.client.execute(cls.test_db, tbl_create.format(cls.test_table))

            timeout = 200
            csv_ingest_props = IngestionProperties(
                cls.test_db,
                cls.test_table,
                data_format=DataFormat.CSV,
                flush_immediately=True,
            )

            # Wait for the table to be able to ingest.
            streaming_ingest_client = KustoStreamingIngestClient(cls.kcsb)
            df = pandas.DataFrame.from_dict({"msg": ["Flush"]})

            while timeout > 0:
                time.sleep(1)
                timeout -= 1

                try:
                    streaming_ingest_client.ingest_from_dataframe(df, csv_ingest_props)
                    response = cls.client.execute(cls.test_db, "{} | where name == 'Flush' | count".format(cls.test_table))
                except KustoServiceError:
                    continue

                if response is not None:
                    row = response.primary_results[0][0]
                    actual = int(row["Count"])
                    # this is done to allow for data to arrive properly
                    if actual >= 1:
                        cls.is_live_testing_ready = True
                        break
        else:
            print("At least one env variable not found, live_testing_ready is false")
            cls.is_live_testing_ready = False

    @classmethod
    def teardown_class(cls):
        """cleanup table after testing (if successful)"""
        # logging.getLogger().removeHandler(cls.kh)

        global has_one_test_failed

        if not has_one_test_failed:
            cls.client.execute(cls.test_db, ".drop table {} ifexists".format(cls.test_table))
        logging.basicConfig(force=True)  # in order for the tests to chain

    @classmethod
    # assertions
    def assert_rows_added(cls, expected: int, level: int, timeout=60):
        actual = 0
        while timeout > 0:
            time.sleep(1)
            timeout -= 1

            try:
                response = cls.client.execute(
                    cls.test_db, "{} | where levelno=={} | where msg has 'Test' | where msg != 'Flush' | count".format(cls.test_table, level)
                )
            except KustoServiceError:
                continue

            if response is not None:
                row = response.primary_results[0][0]
                actual = int(row["Count"])
                # this is done to allow for data to arrive properly
                if actual >= expected:
                    break
        assert actual == expected, "Row count expected = {0}, while actual row count = {1}".format(expected, actual)

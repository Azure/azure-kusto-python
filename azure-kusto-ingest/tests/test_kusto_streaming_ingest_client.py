import pytest
import os
import unittest
import json
import responses
import io
from azure.kusto.ingest import KustoStreamingIngestClient, IngestionProperties, DataFormat


pandas_installed = False
try:
    import pandas

    pandas_installed = True
except:
    pass


def request_callback(request):
    response_status = 200
    response_headers = []
    response_body = {
        "Tables": [
            {
                "TableName": "Table_0",
                "Columns": [
                    {'ColumnName': 'ConsumedRecordsCount', 'DataType': 'Int64'},
                    {'ColumnName': 'UpdatePolicyStatus', 'DataType': 'String'},
                    {'ColumnName': 'UpdatePolicyFailureCode', 'DataType': 'String'},
                    {'ColumnName': 'UpdatePolicyFailureReason', 'DataType': 'String'}
                ],
                'Rows': [
                    [0, 'Inactive', 'Unknown', None]
                ]
            }
        ]
    }

    return response_status, response_headers, json.dumps(response_body)


class KustoStreamingIngestClientTests(unittest.TestCase):

    def setup_test(self):
        responses.add_callback(
            responses.POST,
            "https://ingest-somecluster.kusto.windows.net/v1/rest/ingest/database/table",
            callback=request_callback
        )

        self.ingest_client = KustoStreamingIngestClient("https://ingest-somecluster.kusto.windows.net")
        self.ingestion_properties = IngestionProperties(database="database", table="table", dataFormat=DataFormat.csv)

    @responses.activate
    def test_ingest_from_stream(self):
        self.setup_test()

        byte_sequence = b'56,56,56'
        byte_stream = io.BytesIO(byte_sequence)
        self.ingest_client.ingest_from_stream(byte_stream, ingestion_properties=self.ingestion_properties)

        str_sequence = '57,57,57'
        str_stream = io.StringIO(str_sequence)
        self.ingest_client.ingest_from_stream(str_stream, ingestion_properties=self.ingestion_properties)

    @responses.activate
    def test_ingest_from_file(self):
        self.setup_test()

        # ensure test can work when executed from within directories
        current_dir = os.getcwd()
        path_parts = ["azure-kusto-ingest", "tests", "input", "dataset.csv"]
        missing_path_parts = []
        for path_part in path_parts:
            if path_part not in current_dir:
                missing_path_parts.append(path_part)

        file_path = os.path.join(current_dir, *missing_path_parts)

        self.ingest_client.ingest_from_file(file_path, ingestion_properties=self.ingestion_properties)

    @responses.activate
    @pytest.mark.skipif(not pandas_installed, reason="requires pandas")
    def test_ingest_from_dataframe(self):
        self.setup_test()

        from pandas import DataFrame

        fields = ["id", "name", "value"]
        rows = [[1, "abc", 15.3], [2, "cde", 99.9]]
        df = DataFrame(data=rows, columns=fields)

        self.ingest_client.ingest_from_dataframe(df, ingestion_properties=self.ingestion_properties)


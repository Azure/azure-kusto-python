import os
import unittest

from azure.kusto.data._models import WellKnownDataSet
from azure.kusto.data.streaming_response import JsonTokenReader, ProgressiveDataSetEnumerator, FrameType
from tests.kusto_client_common import KustoClientTestsMixin


# todo:
#  - WellKnownDataSet

class TestStreamingQuery(unittest.TestCase, KustoClientTestsMixin):
    """Tests class for KustoClient API"""

    @staticmethod
    def open_json_file(file_name: str):
        return open(os.path.join(os.path.dirname(__file__), "input", file_name), "rb")

    def test_sanity(self):
        with self.open_json_file("deft.json") as f:
            reader = ProgressiveDataSetEnumerator(JsonTokenReader(f))

            for i in reader:
                if i["FrameType"] == FrameType.DataTable and i["TableKind"] == WellKnownDataSet.PrimaryResult.value:
                    self._assert_sanity_query_primary_results(i["Rows"])

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
import json
import os

from azure.kusto.data._models import KustoResultTable


def test_str_and_dates_smoke():
    with open(os.path.join(os.path.dirname(__file__), "input", "deft.json")) as f:
        data = f.read()
    json_table = json.loads(data)[2]

    result_table = KustoResultTable(json_table)
    assert len(str(result_table)) == 4537

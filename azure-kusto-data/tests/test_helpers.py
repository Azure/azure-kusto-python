# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License

import json
import os
import pytest

from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.data.response import KustoResponseDataSetV2


PANDAS = False
try:
    import pandas
    import numpy

    PANDAS = True
except:
    pass


class TestDataFrameFromResultsTable:
    """Tests the dataframe_from_result_table helper function"""

    @pytest.mark.skipif(not PANDAS, reason="requires pandas")
    def test_dataframe_from_result_table(self):
        """Test conversion of KustoResultTable to pandas.DataFrame, including fixes for certain column types"""

        with open(os.path.join(os.path.dirname(__file__), "input", "dataframe.json"), "r") as response_file:
            data = response_file.read()

        response = KustoResponseDataSetV2(json.loads(data))
        df = dataframe_from_result_table(response.primary_results[0])

        assert df.iloc[0].RecordName == "now"
        assert type(df.iloc[0].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert all(getattr(df.iloc[0].RecordTime, k) == v for k, v in {"year": 2021, "month": 12, "day": 22, "hour": 11, "minute": 43, "second": 00}.items())
        assert type(df.iloc[0].RecordBool) is numpy.bool_
        assert df.iloc[0].RecordBool == True
        assert type(df.iloc[0].RecordInt) is numpy.int32
        assert df.iloc[0].RecordInt == 5678
        assert type(df.iloc[0].RecordReal) is numpy.float64
        assert df.iloc[0].RecordReal == 3.14159

        # Kusto datetime(0000-01-01T00:00:00Z), which Pandas can't represent.
        assert df.iloc[1].RecordName == "earliest datetime"
        assert type(df.iloc[1].RecordTime) is pandas._libs.tslibs.nattype.NaTType
        assert pandas.isnull(df.iloc[1].RecordReal)

        # Kusto datetime(9999-12-31T23:59:59Z), which Pandas can't represent.
        assert df.iloc[2].RecordName == "latest datetime"
        assert type(df.iloc[2].RecordTime) is pandas._libs.tslibs.nattype.NaTType
        assert type(df.iloc[2].RecordReal) is numpy.float64
        assert df.iloc[2].RecordReal == numpy.inf

        # Pandas earliest datetime
        assert df.iloc[3].RecordName == "earliest pandas datetime"
        assert type(df.iloc[3].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert type(df.iloc[3].RecordReal) is numpy.float64
        assert df.iloc[3].RecordReal == -numpy.inf

        # Pandas latest datetime
        assert df.iloc[4].RecordName == "latest pandas datetime"
        assert type(df.iloc[4].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp

        # Kusto 600000000 ticks timedelta
        assert df.iloc[5].RecordName == "timedelta ticks"
        assert type(df.iloc[5].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert type(df.iloc[5].RecordOffset) is pandas._libs.tslibs.timestamps.Timedelta
        assert df.iloc[5].RecordOffset == pandas.to_timedelta("00:01:00")

        # Kusto timedelta(1.01:01:01.0) ==
        assert df.iloc[6].RecordName == "timedelta string"
        assert type(df.iloc[6].RecordTime) is pandas._libs.tslibs.timestamps.Timestamp
        assert type(df.iloc[6].RecordOffset) is pandas._libs.tslibs.timestamps.Timedelta
        assert df.iloc[6].RecordOffset == pandas.to_timedelta("1 days 01:01:01")

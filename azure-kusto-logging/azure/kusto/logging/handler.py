"""
Kusto Logging Handler
"""
import logging
import logging.handlers
import datetime
import time
import sys
import pandas

from azure.kusto.ingest import DataFormat
from azure.kusto.data.exceptions import KustoError
from azure.core.exceptions import AzureError

# .ingest  inline  into table  logs2 <| created = 1609876000.7634413
# .ingest inline into table logs2 <|
# , , , , , , , , , , , , , 1609876000.7634413,


class KustoHandler(logging.handlers.MemoryHandler):
    """A handler class which writes un-formatted logging records to Kusto."""

    def __init__(self, kcsb, database, table, data_format=DataFormat.CSV, useStreaming=False, capacity=8192, flushLevel=logging.ERROR, retries=[5, 30, 60]):
        """Constructor

        Args:
            kcsb (KustoConnectionStringBuilder): kusto connection string
            database (string): database name
            table (string): table name
            data_format (Dataformat, optional): Format for ingestion. Defaults to DataFormat.CSV.
            useStreaming (bool, optional): Use kusto streaming endpoint. Defaults to False.
            capacity (int, optional): Number of records before flushing. Defaults to 8192.
            flushLevel (int, optional): Miminal level to trigger the flush, even if the buffer is not full. Defaults to logging.ERROR.
            retries (list, optional): retries for ingestion error. Defaults to [5s, 30s, 60s]
        """
        super().__init__(capacity, flushLevel=flushLevel)
        from azure.kusto.ingest import (
            QueuedIngestClient,
            IngestionProperties,
            KustoStreamingIngestClient,
        )

        # in order to avoid recursive calls if level is DEBUG
        logging.getLogger("azure").propagate = False
        logging.getLogger("adal-python").propagate = False
        logging.getLogger("requests").propagate = False
        logging.getLogger("urllib3").propagate = False

        if useStreaming:
            self.client = KustoStreamingIngestClient(kcsb)
        else:
            self.client = QueuedIngestClient(kcsb)

        self.ingestion_properties = IngestionProperties(database, table, data_format=data_format)
        self.first_record = None

        self.retries = retries

        # x = logging.LogRecord(None, 1, None, 1, None, None, None)
        # self.ref_dict_keys = x.__dict__.keys()

    def emit(self, record):
        """
        Emit a record.
        Just add the record in the records list
        """

        if not self.buffer:
            self.first_record = record  # in case of error in flush, dump the first record.

        super().emit(record)

    def flush(self):
        """
        Flush the records in Kusto
        """
        if self.buffer:
            self.acquire()
            log_dict = [x.__dict__ for x in self.buffer].copy()
            # convert to iso datetime as Kusto truncate the milliseconds if a float is provided.
            for item in log_dict:
                item["created"] = datetime.datetime.utcfromtimestamp(item.get("created", 0)).isoformat()
            records_to_write = pandas.DataFrame.from_dict(log_dict, orient="columns")

            retries = self.retries.copy()
            retries.append(0)  # in order to ensure a try (or the last retry to go on)
            while retries:
                try:
                    self.client.ingest_from_dataframe(records_to_write, self.ingestion_properties)
                except Exception:  # Done on purpose : Objective is to recover whatever the exception is
                    waiting_time = retries.pop(0)
                    if retries:
                        print("Exception, retrying in {} seconds".format(waiting_time), file=sys.stderr)
                        time.sleep(waiting_time)
                    else:
                        logging.Handler.handleError(self, self.first_record)
                else:
                    break
            self.first_record = None
            self.buffer.clear()
            self.release()

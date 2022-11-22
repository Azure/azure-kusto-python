# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from typing import Tuple

from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import (
    QueuedIngestClient,
)

if __name__ == "__main__":
    # if status updates are required, something like this can be done
    import pprint
    import time
    from azure.kusto.ingest.status import KustoIngestStatusQueues

    with QueuedIngestClient(KustoConnectionStringBuilder.with_interactive_login("https://ingest-asafdev.westeurope.dev.kusto.windows.net")) as client:
        qs = KustoIngestStatusQueues(client)

        MAX_BACKOFF = 180

        backoff = 1
        while True:
            ################### NOTICE ####################
            # in order to get success status updates,
            # make sure ingestion properties set the
            # reportLevel=ReportLevel.FailuresAndSuccesses.
            if qs.success.is_empty() and qs.failure.is_empty():
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                print("No new messages. backing off for {} seconds".format(backoff))
                continue

            backoff = 1

            success_messages = qs.success.pop(10)
            failure_messages = qs.failure.pop(10)

            pprint.pprint("SUCCESS : {}".format(success_messages))
            pprint.pprint("FAILURE : {}".format(failure_messages))

            # you can of course separate them and dump them into a file for follow up investigations
            with open("successes.log", "w+") as sf:
                for sm in success_messages:
                    sf.write(str(sm))

            with open("failures.log", "w+") as ff:
                for fm in failure_messages:
                    ff.write(str(fm))

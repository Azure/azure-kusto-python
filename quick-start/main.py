"""
Quick-start script for issue #626 reproduction.
Creates a raw_ingestion table, ingests sample data, and queries it.

Usage:
    python main.py <cluster_url> <database>

Example:
    python main.py https://mycluster.centralus.kusto.windows.net mydb
"""

import argparse
import sys
import time

from azure.identity import DefaultAzureCredential
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder


TABLE_NAME = "raw_ingestion"

CREATE_TABLE_CMD = f""".create-merge table {TABLE_NAME} (
    truck_id: string,
    truck_type: string,
    tire_temp: real,
    event_timestamp: datetime,
    ingestion_timestamp: datetime
)"""

INGEST_CMD = f""".ingest inline into table {TABLE_NAME} <|
T100,HaulTruck,78.5,2026-03-14T08:00:00Z,2026-03-14T08:00:05Z
T101,HaulTruck,81.2,2026-03-14T08:01:00Z,2026-03-14T08:01:04Z
T102,Loader,74.9,2026-03-14T08:02:00Z,2026-03-14T08:02:06Z
T100,HaulTruck,79.1,2026-03-14T08:03:00Z,2026-03-14T08:03:05Z
T103,Drill,69.4,2026-03-14T08:04:00Z,2026-03-14T08:04:03Z
T101,HaulTruck,82.0,2026-03-14T08:05:00Z,2026-03-14T08:05:07Z
T104,Dozer,71.6,2026-03-14T08:06:00Z,2026-03-14T08:06:02Z
T102,Loader,75.3,2026-03-14T08:07:00Z,2026-03-14T08:07:04Z
T100,HaulTruck,80.4,2026-03-14T08:08:00Z,2026-03-14T08:08:05Z
T105,Excavator,77.8,2026-03-14T08:09:00Z,2026-03-14T08:09:06Z"""

QUERY = f"{TABLE_NAME} | limit 10"


def create_client(cluster_url: str) -> KustoClient:
    credential = DefaultAzureCredential()
    kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster_url, credential)
    return KustoClient(kcsb)


def create_table(client: KustoClient, database: str) -> None:
    print(f"Creating table '{TABLE_NAME}'...")
    client.execute_mgmt(database, CREATE_TABLE_CMD)
    print("Table created (or already exists).")


def ingest_data(client: KustoClient, database: str) -> None:
    print(f"Ingesting sample data into '{TABLE_NAME}'...")
    client.execute_mgmt(database, INGEST_CMD)
    print("Data ingested.")


def query_data(client: KustoClient, database: str) -> None:
    print(f"\nQuerying: {QUERY}")
    response = client.execute(database, QUERY)

    for table in response.primary_results:
        columns = [col.column_name for col in table.columns]
        print(f"\n{' | '.join(columns)}")
        print("-" * (len(columns) * 25))
        for row in table:
            print(" | ".join(str(row[col]) for col in columns))
    print(f"\nTotal rows returned: {len(response.primary_results[0])}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Quick-start: create table, ingest, and query ADX.")
    parser.add_argument("cluster_url", help="ADX cluster URL (e.g. https://mycluster.centralus.kusto.windows.net)")
    parser.add_argument("database", help="Database name")
    args = parser.parse_args()

    print(f"Cluster: {args.cluster_url}")
    print(f"Database: {args.database}")
    print()

    client = create_client(args.cluster_url)

    try:
        create_table(client, args.database)
        ingest_data(client, args.database)
        # Small delay for inline ingestion to settle
        time.sleep(2)
        query_data(client, args.database)
        drop_cmd = f".drop table {TABLE_NAME} ifexists"
        client.execute_mgmt(args.database, drop_cmd)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()

    print("\nDone.")


if __name__ == "__main__":
    main()

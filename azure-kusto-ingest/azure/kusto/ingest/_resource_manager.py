"""This module is serve as a cache to all resources needed by the kusto ingest client."""

from datetime import datetime, timedelta

from ._connection_string import _ConnectionString


class _IngestClientResources(object):
    def __init__(
        self,
        secured_ready_for_aggregation_queues=None,
        failed_ingestions_queues=None,
        successful_ingestions_queues=None,
        containers=None,
        status_tables=None,
    ):
        self.secured_ready_for_aggregation_queues = secured_ready_for_aggregation_queues
        self.failed_ingestions_queues = failed_ingestions_queues
        self.successful_ingestions_queues = successful_ingestions_queues
        self.containers = containers
        self.status_tables = status_tables

    def is_applicable(self):
        resources = [
            self.secured_ready_for_aggregation_queues,
            self.failed_ingestions_queues,
            self.failed_ingestions_queues,
            self.containers,
            self.status_tables,
        ]
        return all(resources)


class _ResourceManager(object):
    def __init__(self, kusto_client):
        self._kusto_client = kusto_client
        self._refresh_period = timedelta(hours=1)

        self._ingest_client_resources = None
        self._ingest_client_resources_last_update = None

        self._authorization_context = None
        self._authorization_context_last_update = None

    def _refresh_ingest_client_resources(self):
        if (
            not self._ingest_client_resources
            or (self._ingest_client_resources_last_update + self._refresh_period) <= datetime.utcnow()
            or not self._ingest_client_resources.is_applicable()
        ):
            self._ingest_client_resources = self._get_ingest_client_resources_from_service()
            self._ingest_client_resources_last_update = datetime.utcnow()

    def _get_resource_by_name(self, df, resource_name):
        resource = df[df["ResourceTypeName"] == resource_name].StorageRoot.map(_ConnectionString.parse).tolist()
        return resource

    def _get_ingest_client_resources_from_service(self):
        df = self._kusto_client.execute("NetDefaultDB", ".get ingestion resources").primary_results[0].to_dataframe()

        secured_ready_for_aggregation_queues = self._get_resource_by_name(df, "SecuredReadyForAggregationQueue")
        failed_ingestions_queues = self._get_resource_by_name(df, "FailedIngestionsQueue")
        successful_ingestions_queues = self._get_resource_by_name(df, "SuccessfulIngestionsQueue")
        containers = self._get_resource_by_name(df, "TempStorage")
        status_tables = self._get_resource_by_name(df, "IngestionsStatusTable")

        return _IngestClientResources(
            secured_ready_for_aggregation_queues,
            failed_ingestions_queues,
            successful_ingestions_queues,
            containers,
            status_tables,
        )

    def _refresh_authorization_context(self):
        if (
            not self._authorization_context
            or self._authorization_context.isspace()
            or (self._authorization_context_last_update + self._refresh_period) <= datetime.utcnow()
        ):
            self._authorization_context = self._get_authorization_context_from_service()
            self._authorization_context_last_update = datetime.utcnow()

    def _get_authorization_context_from_service(self):
        return self._kusto_client.execute("NetDefaultDB", ".get kusto identity token").primary_results[0][0][
            "AuthorizationContext"
        ]

    def get_ingestion_queues(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.secured_ready_for_aggregation_queues

    def get_failed_ingestions_queues(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.failed_ingestions_queues

    def get_successful_ingestions_queues(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.successful_ingestions_queues

    def get_containers(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.containers

    def get_ingestions_status_tables(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.status_tables

    def get_authorization_context(self):
        self._refresh_authorization_context()
        return self._authorization_context

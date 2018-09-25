"""This module is serve as a cache to all resources needed by the kusto ingest client."""

from datetime import datetime, timedelta
import re

_URI_FORMAT = re.compile("https://(\\w+).(queue|blob|table).core.windows.net/([\\w,-]+)\\?(.*)")


class _ResourceUri:
    def __init__(self, storage_account_name, object_type, object_name, sas):
        self.storage_account_name = storage_account_name
        self.object_type = object_type
        self.object_name = object_name
        self.sas = sas

    @classmethod
    def parse(cls, uri):
        """Parses uri into a ResourceUri object"""
        match = _URI_FORMAT.search(uri)
        return cls(match.group(1), match.group(2), match.group(3), match.group(4))

    def to_string(self):
        """Stringify the resource uri instance"""
        return "https://{0.storage_account_name}.{0.object_type}.core.windows.net/{0.object_name}?{0.sas}".format(self)


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

    def _get_resource_by_name(self, table, resource_name):
        return [_ResourceUri.parse(row["StorageRoot"]) for row in table if row["ResourceTypeName"] == resource_name]

    def _get_ingest_client_resources_from_service(self):
        table = self._kusto_client.execute("NetDefaultDB", ".get ingestion resources").primary_results[0]

        secured_ready_for_aggregation_queues = self._get_resource_by_name(table, "SecuredReadyForAggregationQueue")
        failed_ingestions_queues = self._get_resource_by_name(table, "FailedIngestionsQueue")
        successful_ingestions_queues = self._get_resource_by_name(table, "SuccessfulIngestionsQueue")
        containers = self._get_resource_by_name(table, "TempStorage")
        status_tables = self._get_resource_by_name(table, "IngestionsStatusTable")

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

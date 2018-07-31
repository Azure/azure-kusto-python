"""This module is serve as a cache to all resources needed by the kusto ingest client."""

from datetime import datetime, timedelta
import random

from .connection_string import _ConnectionString

class _IngestClientResources:
    def __init__(self,
                 secured_ready_for_aggregation_queues = None,
                 failed_ingestions_queues = None,
                 successful_ingestions_queues = None,
                 containers = None,
                 status_table = None
                 ):
        self.secured_ready_for_aggregation_queues = secured_ready_for_aggregation_queues
        self.failed_ingestions_queues = failed_ingestions_queues
        self.successful_ingestions_queues = successful_ingestions_queues
        self.containers = containers
        self.status_table = status_table
    
    def is_applicable(self):
        resources = [self.secured_ready_for_aggregation_queues, self.failed_ingestions_queues, self.failed_ingestions_queues, self.containers, self.status_table]
        return all(resources)

class _ResourceManager:
    def __init__(self, 
                 kusto_client):
        self._kusto_client = kusto_client
        self._cache_lifetime = timedelta(hours=5)

        self._ingest_client_resources = None
        self._ingest_client_resources_last_update = None

        self._authorization_context = None
        self._authorization_context_last_update = None

    def _refresh_ingest_client_resources(self):
        if not self._ingest_client_resources \
           or self._ingest_client_resources_last_update + self._cache_lifetime <= datetime.now() \
           or not self._ingest_client_resources.is_applicable():
                self._ingest_client_resources = self._get_ingest_client_resources_from_service()
                self._ingest_client_resources_last_update = datetime.now()  

    def _get_resource_by_name(self, df, resource_name):
        resource = df[df['ResourceTypeName'] == resource_name].StorageRoot.map(_ConnectionString.parse).tolist()
        return resource
    
    def _get_ingest_client_resources_from_service(self):
        df = self._kusto_client.execute("NetDefaultDB", ".get ingestion resources").to_dataframe()

        secured_ready_for_aggregation_queues = self._get_resource_by_name(df, 'SecuredReadyForAggregationQueue')
        failed_ingestions_queues = self._get_resource_by_name(df, 'FailedIngestionsQueue')
        successful_ingestions_queues = self._get_resource_by_name(df, 'SuccessfulIngestionsQueue')
        containers = self._get_resource_by_name(df, 'TempStorage')
        status_table = self._get_resource_by_name(df, 'IngestionsStatusTable')[0]

        return _IngestClientResources(secured_ready_for_aggregation_queues, failed_ingestions_queues, successful_ingestions_queues, containers, status_table)

    def _refresh_authorization_context(self):
        if not self._authorization_context \
           or self._authorization_context.isspace() \
           or self._authorization_context_last_update + self._cache_lifetime <= datetime.now():
                self._authorization_context = self._get_authorization_context_from_service()
                self._authorization_context_last_update = datetime.now()
    
    def _get_authorization_context_from_service(self):
        df = self._kusto_client.execute("NetDefaultDB", ".get kusto identity token").to_dataframe()
        return df['AuthorizationContext'].values[0]

    def get_ingestion_queues(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.secured_ready_for_aggregation_queues

    def get_failed_ingestions_queues(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.failed_ingestions_queues
    
    def get_successful_ingestions_queues(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.successful_ingestions_queues

    def get_container(self):
        self._refresh_ingest_client_resources()
        current_containers = self._ingest_client_resources.containers
        return random.choice(current_containers)
    
    def get_ingestions_status_table(self):
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.status_table
    
    def get_authorization_context(self):
        self._refresh_authorization_context()
        return self._authorization_context

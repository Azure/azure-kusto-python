# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

from tenacity import retry_if_exception_type, stop_after_attempt, Retrying, wait_random_exponential

from azure.kusto.data import KustoClient
from azure.kusto.data._models import KustoResultTable
from azure.kusto.data._telemetry import MonitoredActivity, Span
from azure.kusto.data.exceptions import KustoThrottlingError

_SHOW_VERSION = ".show version"
_SERVICE_TYPE_COLUMN_NAME = "ServiceType"


class _ResourceUri:
    def __init__(self, url: str):
        self.url = url
        self.parsed = urlparse(url)
        self.storage_account_name = self.parsed.netloc.split(".", 1)[0]
        self.object_name = self.parsed.path.lstrip("/")

    @property
    def account_uri(self) -> str:
        return f"{self.parsed.scheme}://{self.parsed.netloc}/?{self.parsed.query}"

    def __str__(self):
        return self.url


class _IngestClientResources:
    def __init__(
        self,
        secured_ready_for_aggregation_queues: List[_ResourceUri] = None,
        failed_ingestions_queues: List[_ResourceUri] = None,
        successful_ingestions_queues: List[_ResourceUri] = None,
        containers: List[_ResourceUri] = None,
        status_tables: List[_ResourceUri] = None,
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


class _ResourceManager:
    def __init__(self, kusto_client: KustoClient):
        self._kusto_client = kusto_client
        self._refresh_period = timedelta(hours=1)

        self._ingest_client_resources = None
        self._ingest_client_resources_last_update = None

        self._authorization_context = None
        self._authorization_context_last_update = None

        self.__set_throttling_settings()

    def close(self):
        self._kusto_client.close()

    def __set_throttling_settings(self, num_of_attempts: int = 4, max_seconds_per_retry: float = 30):
        self._retryer = Retrying(
            wait=wait_random_exponential(max=max_seconds_per_retry),
            retry=retry_if_exception_type(KustoThrottlingError),
            stop=stop_after_attempt(num_of_attempts),
            reraise=True,
        )

    def _refresh_ingest_client_resources(self):
        if (
            not self._ingest_client_resources
            or (self._ingest_client_resources_last_update + self._refresh_period) <= datetime.utcnow()
            or not self._ingest_client_resources.is_applicable()
        ):
            self._ingest_client_resources = self._get_ingest_client_resources_from_service()
            self._ingest_client_resources_last_update = datetime.utcnow()

    def _get_resource_by_name(self, table: KustoResultTable, resource_name: str):
        return [_ResourceUri(row["StorageRoot"]) for row in table if row["ResourceTypeName"] == resource_name]

    def _get_ingest_client_resources_from_service(self):
        # trace all calls to get ingestion resources
        def invoker():
            return MonitoredActivity.invoke(
                lambda: self._kusto_client.execute("NetDefaultDB", ".get ingestion resources"),
                name_of_span="_ResourceManager.get_ingestion_resources",
                tracing_attributes=Span.create_cluster_attributes(self._kusto_client._kusto_cluster),
            )

        result = self._retryer(invoker)
        table = result.primary_results[0]

        secured_ready_for_aggregation_queues = self._get_resource_by_name(table, "SecuredReadyForAggregationQueue")
        failed_ingestions_queues = self._get_resource_by_name(table, "FailedIngestionsQueue")
        successful_ingestions_queues = self._get_resource_by_name(table, "SuccessfulIngestionsQueue")
        containers = self._get_resource_by_name(table, "TempStorage")
        status_tables = self._get_resource_by_name(table, "IngestionsStatusTable")

        containers_by_storage_account = self.group_resources_by_storage_account(containers)
        containers_selected_with_round_robin = self.shuffle_and_select_with_round_robin(containers_by_storage_account)
        secured_ready_for_aggregation_queues_by_storage_account = self.group_resources_by_storage_account(secured_ready_for_aggregation_queues)
        queues_selected_with_round_robin = self.shuffle_and_select_with_round_robin(secured_ready_for_aggregation_queues_by_storage_account)

        return _IngestClientResources(
            queues_selected_with_round_robin, failed_ingestions_queues, successful_ingestions_queues, containers_selected_with_round_robin, status_tables
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
        # trace all calls to get identity token
        def invoker():
            return MonitoredActivity.invoke(
                lambda: self._kusto_client.execute("NetDefaultDB", ".get kusto identity token"),
                name_of_span="_ResourceManager.get_identity_token",
                tracing_attributes=Span.create_cluster_attributes(self._kusto_client._kusto_cluster),
            )

        result = self._retryer(invoker)
        return result.primary_results[0][0]["AuthorizationContext"]

    def get_ingestion_queues(self) -> List[_ResourceUri]:
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.secured_ready_for_aggregation_queues

    def get_failed_ingestions_queues(self) -> List[_ResourceUri]:
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.failed_ingestions_queues

    def get_successful_ingestions_queues(self) -> List[_ResourceUri]:
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.successful_ingestions_queues

    def get_containers(self) -> List[_ResourceUri]:
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.containers

    def get_ingestions_status_tables(self) -> List[_ResourceUri]:
        self._refresh_ingest_client_resources()
        return self._ingest_client_resources.status_tables

    def get_authorization_context(self):
        self._refresh_authorization_context()
        return self._authorization_context

    def retrieve_service_type(self):
        try:
            command_result = self._kusto_client.execute("NetDefaultDB", _SHOW_VERSION)
            return command_result.primary_results[0][0][_SERVICE_TYPE_COLUMN_NAME]
        except (TypeError, KeyError):
            return ""

    def set_proxy(self, proxy_url: str):
        self._kusto_client.set_proxy(proxy_url)

    def group_resources_by_storage_account(self, resources: list):
        resources_by_storage_account = {}
        for resource in resources:
            if resource.storage_account_name not in resources_by_storage_account:
                resources_by_storage_account[resource.storage_account_name] = list()
            resources_by_storage_account[resource.storage_account_name].append(resource)

        return resources_by_storage_account

    def shuffle_and_select_with_round_robin(self, original_dict: dict):
        dictionary_shuffled = self.shuffle_dictionary_keys_and_values(original_dict)
        result = []
        while True:
            if all(not lst for lst in dictionary_shuffled.values()):
                break

            for key, lst in dictionary_shuffled.items():
                if lst:
                    result.append(lst.pop(0))
        return result

    def shuffle_dictionary_keys_and_values(self, dictionary: dict):
        import random

        keys = list(dictionary.keys())
        random.shuffle(keys)

        shuffled_dict = {}
        for key in keys:
            shuffled_values = list(dictionary[key])
            random.shuffle(shuffled_values)
            shuffled_dict[key] = shuffled_values

        return shuffled_dict

    def report_resource_usage_result(self, storage_account_name, success_status):
        pass

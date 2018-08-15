from abc import ABCMeta, abstractmethod
import six

from azure.storage.table import TableService

from ._connection_string import _ConnectionString
from .ingestion_status import IngestionStatus
from .exceptions import NonexistentSourceIdException


@six.add_metaclass(ABCMeta)
class IKustoIngestionResult:
    """The results of a data ingestion operation into Kusto."""

    @abstractmethod
    def get_ingestion_status_by_source_id(self, source_id):
        """Retrieves the detailed ingestion status of the ingestion source with the given sourceId"""
        pass

    @abstractmethod
    def get_ingestion_status_collection(self):
        """Retrieves an iterator of the detailed ingestion status of all data ingestion operations into Kusto associated with this IKustoIngestionResult instance."""
        pass


class KustoIngestionResult(IKustoIngestionResult):
    def __init__(self, map_of_source_id_to_ingestion_status):
        self.map_of_source_id_to_ingestion_status = map_of_source_id_to_ingestion_status

    def get_ingestion_status_by_source_id(self, source_id):
        if source_id in self.map_of_source_id_to_ingestion_status:
            return self.map_of_source_id_to_ingestion_status[source_id]
        else:
            raise NonexistentSourceIdException()

    def get_ingestion_status_collection(self):
        return six.itervalues(self.map_of_source_id_to_ingestion_status)


class TableReportKustoIngestionResult(IKustoIngestionResult):
    """Returned when an ingestion status is reported to a table."""

    def __init__(self, map_of_source_id_to_ingestion_status_in_table):
        self.map_of_source_id_to_ingestion_status_in_table = (
            map_of_source_id_to_ingestion_status_in_table
        )

    def get_ingestion_status_by_source_id(self, source_id):
        if source_id in self.map_of_source_id_to_ingestion_status_in_table:
            ingestion_status = self._get_ingestion_status(
                self.map_of_source_id_to_ingestion_status_in_table[source_id]
            )
            return ingestion_status
        else:
            raise NonexistentSourceIdException()

    def get_ingestion_status_collection(self):
        return six.moves.map(
            self._get_ingestion_status,
            six.itervalues(self.map_of_source_id_to_ingestion_status_in_table),
        )

    def _get_ingestion_status(self, ingestion_status_in_table):
        ingestions_status_table = _ConnectionString.parse(
            ingestion_status_in_table["TableConnectionString"]
        )
        table_service = TableService(
            account_name=ingestions_status_table.storage_account_name,
            sas_token=ingestions_status_table.sas,
        )
        entity = table_service.get_entity(
            table_name=ingestions_status_table.object_name,
            partition_key=ingestion_status_in_table["PartitionKey"],
            row_key=ingestion_status_in_table["RowKey"],
        )
        return IngestionStatus(entity)

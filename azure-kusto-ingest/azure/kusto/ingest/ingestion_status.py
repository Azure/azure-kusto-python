from enum import Enum, IntEnum
from datetime import datetime

from azure.cosmosdb.table.models import Entity


class Status(Enum):
    """Represent the state of the data ingestion operation into Kusto."""
    
    Pending = "Pending"
    Succeeded = "Succeeded"
    Failed = "Failed"
    Queued = "Queued"
    Skipped = "Skipped"
    PartiallySucceeded = "PartiallySucceeded"


class IngestionStatus(Entity):
    """Represent the ingestion status."""
    
    @classmethod
    def create_ingestion_status(cls, ingestion_source_id, blob, ingestion_properties):
        ingestion_status = {}
        ingestion_status["Status"] = Status.Pending.value
        ingestion_status["PartitionKey"] = ingestion_source_id
        ingestion_status["RowKey"] = ingestion_source_id
        ingestion_status["IngestionSourceId"] = ingestion_source_id
        ingestion_status["IngestionSourcePath"] = blob.path
        ingestion_status["Database"] = ingestion_properties.database
        ingestion_status["Table"] = ingestion_properties.table
        ingestion_status["UpdatedOn"] = datetime.utcnow()
        
        ingestion_status["OperationId"] = None
        ingestion_status["ActivityId"] = None
        ingestion_status["ErrorCode"] = None
        ingestion_status["FailureStatus"] = None
        ingestion_status["Details"] = None
        ingestion_status["OriginatesFromUpdatePolicy"] = None
        
        return cls(ingestion_status)

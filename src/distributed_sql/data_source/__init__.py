"""Unified data-source scans."""

from distributed_sql.data_source.base import (
    CompoundPredicate,
    DataSource,
    FileScanTask,
    Predicate,
    PredicateOperator,
    ScanPlan,
    ScanPredicate,
    ScanRequest,
)
from distributed_sql.data_source.files import (
    AvroDataSource,
    CSVDataSource,
    ORCDataSource,
    ParquetDataSource,
    create_file_data_source,
    schema_to_arrow,
)
from distributed_sql.data_source.iceberg import IcebergDataSource
from distributed_sql.data_source.registry import (
    DataSourceRegistry,
    create_data_source_registry,
)

__all__ = [
    "AvroDataSource",
    "CSVDataSource",
    "CompoundPredicate",
    "DataSource",
    "DataSourceRegistry",
    "FileScanTask",
    "IcebergDataSource",
    "ORCDataSource",
    "ParquetDataSource",
    "Predicate",
    "PredicateOperator",
    "ScanPlan",
    "ScanPredicate",
    "ScanRequest",
    "create_data_source_registry",
    "create_file_data_source",
    "schema_to_arrow",
]

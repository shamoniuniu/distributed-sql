"""Extensible data-source adapter registry."""

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import ObjectStoreRouter
from distributed_sql.data_source.base import DataSource
from distributed_sql.data_source.files import (
    AvroDataSource,
    CSVDataSource,
    ORCDataSource,
    ParquetDataSource,
)
from distributed_sql.data_source.iceberg import IcebergDataSource


class DataSourceRegistry:
    def __init__(self) -> None:
        self._sources: dict[TableFormat, DataSource] = {}

    def register(self, table_format: TableFormat, source: DataSource) -> None:
        self._sources[table_format] = source

    def for_table(self, table: CatalogTable) -> DataSource:
        try:
            return self._sources[table.format]
        except KeyError as exc:
            raise ValueError(
                f"No data-source adapter is registered for {table.format.value!r}"
            ) from exc


def create_data_source_registry(stores: ObjectStoreRouter) -> DataSourceRegistry:
    registry = DataSourceRegistry()
    registry.register(TableFormat.CSV, CSVDataSource(stores))
    registry.register(TableFormat.PARQUET, ParquetDataSource(stores))
    registry.register(TableFormat.AVRO, AvroDataSource(stores))
    registry.register(TableFormat.ORC, ORCDataSource(stores))
    registry.register(TableFormat.ICEBERG, IcebergDataSource())
    return registry

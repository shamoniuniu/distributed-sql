"""Catalog and import API contracts."""

from datetime import datetime
from enum import StrEnum
from typing import Self

from pydantic import Field, JsonValue, model_validator

from distributed_sql.common.protocol import (
    Partition,
    PartitionStrategy,
    ProtocolModel,
    Schema,
    Statistics,
)


class TableFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    ICEBERG = "iceberg"


class NamespaceCreate(ProtocolModel):
    name: str = Field(min_length=1, max_length=255, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    properties: dict[str, JsonValue] = Field(default_factory=dict)


class NamespaceUpdate(ProtocolModel):
    properties: dict[str, JsonValue] = Field(default_factory=dict)


class Namespace(ProtocolModel):
    name: str
    properties: dict[str, JsonValue] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class NamespaceList(ProtocolModel):
    namespaces: list[Namespace]


class TableCreate(ProtocolModel):
    name: str = Field(min_length=1, max_length=255, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    schema_: Schema = Field(alias="schema")
    format: TableFormat
    location: str = Field(min_length=1)
    partition_strategy: PartitionStrategy = PartitionStrategy.UNKNOWN
    partition_keys: list[str] = Field(default_factory=list)
    properties: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def partition_keys_exist(self) -> Self:
        field_names = {field.name for field in self.schema_.fields}
        missing = set(self.partition_keys) - field_names
        if missing:
            raise ValueError(f"partition keys are not in schema: {sorted(missing)}")
        return self


class TableUpdate(ProtocolModel):
    schema_: Schema | None = Field(default=None, alias="schema")
    format: TableFormat | None = None
    location: str | None = Field(default=None, min_length=1)
    partition_strategy: PartitionStrategy | None = None
    partition_keys: list[str] | None = None
    properties: dict[str, JsonValue] | None = None


class CatalogTable(ProtocolModel):
    namespace: str
    name: str
    schema_: Schema = Field(alias="schema")
    format: TableFormat
    location: str
    partition_strategy: PartitionStrategy
    partition_keys: list[str] = Field(default_factory=list)
    properties: dict[str, JsonValue] = Field(default_factory=dict)
    partitions: list[Partition] = Field(default_factory=list)
    statistics: Statistics | None = None
    created_at: datetime
    updated_at: datetime


class TableList(ProtocolModel):
    tables: list[CatalogTable]


class ImportRequest(ProtocolModel):
    source_location: str = Field(min_length=1)
    source_format: TableFormat | None = None
    partition_count: int = Field(default=1, ge=1)
    partition_key: str | None = None


class ImportResult(ProtocolModel):
    table: CatalogTable
    manifest_location: str

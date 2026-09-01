"""CSV, Parquet, Avro, and ORC data-source adapters."""

from __future__ import annotations

import os
import sys
from collections.abc import Iterable, Iterator
from importlib.resources import files
from io import BytesIO
from typing import Any, cast

import pyarrow as pa
import pyarrow.csv as arrow_csv
import pyarrow.orc as arrow_orc
import pyarrow.parquet as arrow_parquet
from fastavro import reader as avro_reader

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import ObjectStoreRouter
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import DataType, Schema, SchemaField
from distributed_sql.data_source.base import DataSource, FileScanTask, ScanPlan, ScanRequest


class FileDataSource(DataSource):
    format: TableFormat

    def __init__(self, stores: ObjectStoreRouter) -> None:
        self._stores = stores

    def plan_scan(self, table: CatalogTable, request: ScanRequest) -> ScanPlan:
        if table.format is not self.format:
            raise ValueError(
                f"{type(self).__name__} cannot scan table format {table.format.value!r}"
            )
        schema = schema_to_arrow(table.schema_)
        _validate_columns(schema, request)
        if request.file_tasks is not None:
            tasks = request.file_tasks
        elif table.partitions:
            tasks = tuple(
                FileScanTask(
                    location=partition.location,
                    format=self.format,
                    length=partition.size_bytes,
                    record_count=partition.row_count,
                )
                for partition in table.partitions
            )
        else:
            tasks = (FileScanTask(location=table.location, format=self.format),)
        invalid = [task.location for task in tasks if task.format is not self.format]
        if invalid:
            raise ValueError(f"Scan tasks have the wrong format: {invalid}")
        return ScanPlan(
            schema=_projected_schema(schema, request.projection),
            file_tasks=tasks,
        )

    def scan(self, table: CatalogTable, request: ScanRequest) -> Iterator[pa.RecordBatch]:
        plan = self.plan_scan(table, request)
        source_schema = schema_to_arrow(table.schema_)
        for task in plan.file_tasks:
            try:
                payload = self._stores.for_location(task.location).read_bytes(task.location)
                tables = self._read_tables(payload, source_schema, request)
                for source in tables:
                    filtered = _filter_and_project(source, request)
                    yield from filtered.to_batches(max_chunksize=request.batch_size)
            except FileNotFoundError as exc:
                raise DistributedSQLError(
                    ErrorCode.NOT_FOUND,
                    f"Scan input {task.location!r} does not exist.",
                    status_code=404,
                    context={"location": task.location},
                ) from exc
            except (OSError, ValueError, pa.ArrowException) as exc:
                raise DistributedSQLError(
                    ErrorCode.INVALID_REQUEST,
                    f"Could not scan {self.format.value} input {task.location!r}.",
                    status_code=422,
                    context={"location": task.location, "format": self.format.value},
                ) from exc

    def _read_tables(
        self,
        payload: bytes,
        schema: pa.Schema,
        request: ScanRequest,
    ) -> Iterable[pa.Table]:
        raise NotImplementedError


class CSVDataSource(FileDataSource):
    format = TableFormat.CSV

    def _read_tables(
        self,
        payload: bytes,
        schema: pa.Schema,
        request: ScanRequest,
    ) -> Iterable[pa.Table]:
        columns = _required_columns(schema, request)
        yield arrow_csv.read_csv(
            pa.BufferReader(payload),
            convert_options=arrow_csv.ConvertOptions(
                column_types={name: schema.field(name).type for name in columns},
                include_columns=list(columns),
                null_values=[""],
                strings_can_be_null=True,
            ),
        )


class ParquetDataSource(FileDataSource):
    format = TableFormat.PARQUET

    def _read_tables(
        self,
        payload: bytes,
        schema: pa.Schema,
        request: ScanRequest,
    ) -> Iterable[pa.Table]:
        yield arrow_parquet.read_table(
            pa.BufferReader(payload),
            columns=list(_required_columns(schema, request)),
            filters=request.predicate.to_arrow() if request.predicate else None,
        )


class ORCDataSource(FileDataSource):
    format = TableFormat.ORC

    def _read_tables(
        self,
        payload: bytes,
        schema: pa.Schema,
        request: ScanRequest,
    ) -> Iterable[pa.Table]:
        if sys.platform == "win32":
            os.environ.setdefault("TZDIR", str(files("tzdata").joinpath("zoneinfo")))
        columns = _required_columns(schema, request)
        yield arrow_orc.ORCFile(pa.BufferReader(payload)).read(
            columns=list(columns)
        )


class AvroDataSource(FileDataSource):
    format = TableFormat.AVRO

    def _read_tables(
        self,
        payload: bytes,
        schema: pa.Schema,
        request: ScanRequest,
    ) -> Iterator[pa.Table]:
        columns = _required_columns(schema, request)
        selected_schema = pa.schema([schema.field(name) for name in columns])
        records = avro_reader(BytesIO(payload))
        chunk: list[dict[str, Any]] = []
        for record in records:
            row = cast(dict[str, Any], record)
            chunk.append({name: row.get(name) for name in columns})
            if len(chunk) == request.batch_size:
                yield pa.Table.from_pylist(chunk, schema=selected_schema)
                chunk = []
        if chunk:
            yield pa.Table.from_pylist(chunk, schema=selected_schema)


def create_file_data_source(
    table_format: TableFormat,
    stores: ObjectStoreRouter,
) -> FileDataSource:
    adapters: dict[TableFormat, type[FileDataSource]] = {
        TableFormat.CSV: CSVDataSource,
        TableFormat.PARQUET: ParquetDataSource,
        TableFormat.AVRO: AvroDataSource,
        TableFormat.ORC: ORCDataSource,
    }
    try:
        adapter = adapters[table_format]
    except KeyError as exc:
        raise ValueError(f"{table_format.value!r} is not a file data-source format") from exc
    return adapter(stores)


def schema_to_arrow(schema: Schema) -> pa.Schema:
    return pa.schema(
        [_field_to_arrow(field) for field in schema.fields],
        metadata={key: value for key, value in schema.metadata.items()} or None,
    )


def _field_to_arrow(field: SchemaField) -> pa.Field:
    primitive_types: dict[DataType, pa.DataType] = {
        DataType.NULL: pa.null(),
        DataType.BOOLEAN: pa.bool_(),
        DataType.INT32: pa.int32(),
        DataType.INT64: pa.int64(),
        DataType.FLOAT32: pa.float32(),
        DataType.FLOAT64: pa.float64(),
        DataType.STRING: pa.string(),
        DataType.BINARY: pa.binary(),
        DataType.DATE: pa.date32(),
    }
    if field.data_type in primitive_types:
        arrow_type = primitive_types[field.data_type]
    elif field.data_type is DataType.DECIMAL:
        precision = int(field.metadata.get("precision", "38"))
        scale = int(field.metadata.get("scale", "9"))
        arrow_type = pa.decimal128(precision, scale)
    elif field.data_type is DataType.TIMESTAMP:
        arrow_type = pa.timestamp(
            field.metadata.get("unit", "us"),
            tz=field.metadata.get("timezone"),
        )
    elif field.data_type is DataType.LIST:
        if len(field.children) != 1:
            raise ValueError(f"List field {field.name!r} must have exactly one child")
        arrow_type = pa.list_(_field_to_arrow(field.children[0]))
    elif field.data_type is DataType.STRUCT:
        arrow_type = pa.struct([_field_to_arrow(child) for child in field.children])
    else:
        raise ValueError(f"Unsupported data type: {field.data_type}")
    return pa.field(
        field.name,
        arrow_type,
        nullable=field.nullable,
        metadata={key: value for key, value in field.metadata.items()} or None,
    )


def _required_columns(schema: pa.Schema, request: ScanRequest) -> tuple[str, ...]:
    requested = _required_columns_from_request(request)
    return requested or tuple(schema.names)


def _required_columns_from_request(request: ScanRequest) -> tuple[str, ...]:
    names = list(request.projection or ())
    if request.predicate:
        names.extend(sorted(request.predicate.columns - set(names)))
    return tuple(names)


def _filter_and_project(table: pa.Table, request: ScanRequest) -> pa.Table:
    if request.predicate:
        table = table.filter(request.predicate.to_arrow())
    if request.projection is not None:
        table = table.select(request.projection)
    return table


def _projected_schema(schema: pa.Schema, projection: tuple[str, ...] | None) -> pa.Schema:
    if projection is None:
        return schema
    return pa.schema([schema.field(name) for name in projection], metadata=schema.metadata)


def _validate_columns(schema: pa.Schema, request: ScanRequest) -> None:
    requested = set(request.projection or ())
    if request.predicate:
        requested.update(request.predicate.columns)
    missing = requested - set(schema.names)
    if missing:
        raise ValueError(f"Scan columns are not in the table schema: {sorted(missing)}")

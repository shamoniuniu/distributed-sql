"""Partitioned data import with immutable files and atomic manifests."""

import hashlib
import json
import threading
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, cast
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import pyarrow as pa
import pyarrow.csv as arrow_csv
import pyarrow.orc as arrow_orc
import pyarrow.parquet as arrow_parquet

from distributed_sql.catalog.models import (
    CatalogTable,
    ImportRequest,
    ImportResult,
    TableFormat,
)
from distributed_sql.catalog.repository import SQLiteCatalog
from distributed_sql.catalog.storage import ObjectStoreRouter
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import (
    ColumnStatistics,
    Partition,
    PartitionStrategy,
    Statistics,
)


class DataImporter:
    def __init__(self, catalog: SQLiteCatalog, stores: ObjectStoreRouter) -> None:
        self._catalog = catalog
        self._stores = stores
        self._lock = threading.Lock()

    def import_table(
        self,
        namespace: str,
        table_name: str,
        request: ImportRequest,
    ) -> ImportResult:
        with self._lock:
            return self._import_table(namespace, table_name, request)

    def _import_table(
        self,
        namespace: str,
        table_name: str,
        request: ImportRequest,
    ) -> ImportResult:
        table_definition = self._catalog.get_table(namespace, table_name)
        source_format = request.source_format or self._format_from_location(
            request.source_location,
            table_definition.format,
        )
        source = self._read_source(request.source_location, source_format)
        self._validate_schema(table_definition, source)
        strategy, keys = self._partition_spec(table_definition, request)
        assignments = self._assign_partitions(
            source,
            request.partition_count,
            request.partition_key,
        )
        import_id = uuid4().hex
        written_locations: list[str] = []
        partitions: list[Partition] = []
        manifest_location = _join_location(table_definition.location, "_manifest.json")
        manifest_store = self._stores.for_location(manifest_location)
        previous_manifest = (
            manifest_store.read_bytes(manifest_location)
            if manifest_store.exists(manifest_location)
            else None
        )
        manifest_published = False
        try:
            for ordinal, indices in enumerate(assignments):
                partition_table = source.take(pa.array(indices, type=pa.int64()))
                payload = self._write_partition(partition_table, table_definition.format)
                location = _join_location(
                    table_definition.location,
                    f"data/{import_id}/part-{ordinal:05d}.{table_definition.format.value}",
                )
                self._stores.for_location(location).write_bytes(location, payload)
                written_locations.append(location)
                partitions.append(
                    Partition(
                        partition_id=f"{import_id}-{ordinal:05d}",
                        ordinal=ordinal,
                        location=location,
                        strategy=strategy,
                        keys=keys,
                        size_bytes=len(payload),
                        row_count=partition_table.num_rows,
                        checksum=hashlib.sha256(payload).hexdigest(),
                    )
                )

            statistics = collect_table_statistics(source, partitions)
            manifest = _manifest_bytes(
                namespace,
                table_name,
                table_definition,
                import_id,
                partitions,
                statistics,
            )
            manifest_store.publish_bytes(manifest_location, manifest)
            manifest_published = True
            updated = self._catalog.replace_import_metadata(
                namespace,
                table_name,
                strategy=strategy,
                partition_keys=keys,
                partitions=partitions,
                statistics=statistics,
            )
        except Exception:
            if manifest_published:
                if previous_manifest is None:
                    manifest_store.delete(manifest_location)
                else:
                    manifest_store.publish_bytes(manifest_location, previous_manifest)
            for location in written_locations:
                self._stores.for_location(location).delete(location)
            raise
        return ImportResult(table=updated, manifest_location=manifest_location)

    def _read_source(self, location: str, source_format: TableFormat) -> pa.Table:
        if source_format in {TableFormat.AVRO, TableFormat.ICEBERG}:
            raise DistributedSQLError(
                ErrorCode.INVALID_REQUEST,
                f"Import source format {source_format.value!r} is not supported by Task 2.",
                status_code=422,
                context={"format": source_format.value},
            )
        try:
            payload = self._stores.for_location(location).read_bytes(location)
            source = pa.BufferReader(payload)
            if source_format is TableFormat.CSV:
                return arrow_csv.read_csv(
                    source,
                    convert_options=arrow_csv.ConvertOptions(
                        null_values=[""],
                        strings_can_be_null=True,
                    ),
                )
            if source_format is TableFormat.PARQUET:
                return arrow_parquet.read_table(source)
            if source_format is TableFormat.ORC:
                return arrow_orc.ORCFile(source).read()
        except FileNotFoundError as exc:
            raise DistributedSQLError(
                ErrorCode.NOT_FOUND,
                f"Import source {location!r} does not exist.",
                status_code=404,
                context={"location": location},
            ) from exc
        except (pa.ArrowException, OSError, ValueError) as exc:
            raise DistributedSQLError(
                ErrorCode.INVALID_REQUEST,
                f"Could not read import source {location!r}.",
                status_code=422,
                context={"location": location, "format": source_format.value},
            ) from exc
        raise AssertionError(f"Unhandled source format: {source_format}")

    @staticmethod
    def _write_partition(table: pa.Table, table_format: TableFormat) -> bytes:
        if table_format in {TableFormat.AVRO, TableFormat.ICEBERG}:
            raise DistributedSQLError(
                ErrorCode.INVALID_REQUEST,
                f"Partition output format {table_format.value!r} is not supported by Task 2.",
                status_code=422,
                context={"format": table_format.value},
            )
        sink = pa.BufferOutputStream()
        if table_format is TableFormat.CSV:
            arrow_csv.write_csv(table, sink)
        elif table_format is TableFormat.PARQUET:
            arrow_parquet.write_table(table, sink)
        elif table_format is TableFormat.ORC:
            arrow_orc.write_table(table, sink)
        else:
            raise AssertionError(f"Unhandled output format: {table_format}")
        return cast(bytes, sink.getvalue().to_pybytes())

    @staticmethod
    def _validate_schema(table_definition: CatalogTable, source: pa.Table) -> None:
        expected = [field.name for field in table_definition.schema_.fields]
        actual = source.column_names
        if actual != expected:
            raise DistributedSQLError(
                ErrorCode.INVALID_REQUEST,
                "Import source columns do not match the Catalog schema.",
                status_code=422,
                context={"expected": expected, "actual": actual},
            )

    @staticmethod
    def _partition_spec(
        table_definition: CatalogTable,
        request: ImportRequest,
    ) -> tuple[PartitionStrategy, list[str]]:
        if request.partition_key is None:
            return PartitionStrategy.ROUND_ROBIN, []
        if request.partition_key not in {
            field.name for field in table_definition.schema_.fields
        }:
            raise DistributedSQLError(
                ErrorCode.INVALID_REQUEST,
                f"Partition key {request.partition_key!r} is not in the table schema.",
                status_code=422,
                context={"partition_key": request.partition_key},
            )
        return PartitionStrategy.HASH, [request.partition_key]

    @staticmethod
    def _assign_partitions(
        table: pa.Table,
        partition_count: int,
        partition_key: str | None,
    ) -> list[list[int]]:
        assignments: list[list[int]] = [[] for _ in range(partition_count)]
        if partition_key is None:
            for index in range(table.num_rows):
                assignments[index % partition_count].append(index)
            return assignments
        values = table[partition_key].combine_chunks()
        for index, value in enumerate(values):
            encoded = _stable_value(value.as_py())
            target = int.from_bytes(hashlib.sha256(encoded).digest()[:8], "big") % partition_count
            assignments[target].append(index)
        return assignments

    @staticmethod
    def _format_from_location(location: str, fallback: TableFormat) -> TableFormat:
        suffix = PurePosixPath(urlsplit(location).path).suffix.lower().lstrip(".")
        try:
            return TableFormat(suffix)
        except ValueError:
            return fallback


def _stable_value(value: Any) -> bytes:
    normalized = _json_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    return str(value)


def collect_table_statistics(
    table: pa.Table,
    partitions: list[Partition] | None = None,
    *,
    source: str = "import",
) -> Statistics:
    """Collect exact table and column statistics from an Arrow table."""

    actual_partitions = partitions or []
    columns: dict[str, ColumnStatistics] = {}
    for name in table.column_names:
        array = table[name].combine_chunks()
        values = [value.as_py() for value in array if value.is_valid]
        try:
            minimum = _json_value(min(values)) if values else None
            maximum = _json_value(max(values)) if values else None
        except TypeError:
            minimum = None
            maximum = None
        columns[name] = ColumnStatistics(
            column_name=name,
            null_count=array.null_count,
            distinct_count=len({_stable_value(value) for value in values}),
            min_value=minimum,
            max_value=maximum,
            average_size_bytes=(array.nbytes / len(array)) if len(array) else 0.0,
        )
    return Statistics(
        row_count=table.num_rows,
        size_bytes=(
            sum(partition.size_bytes or 0 for partition in actual_partitions)
            if actual_partitions
            else table.nbytes
        ),
        columns=columns,
        collected_at=datetime.now(UTC),
        source=source,
    )


def _manifest_bytes(
    namespace: str,
    table_name: str,
    table_definition: CatalogTable,
    import_id: str,
    partitions: list[Partition],
    statistics: Statistics,
) -> bytes:
    payload = {
        "version": 1,
        "import_id": import_id,
        "table": f"{namespace}.{table_name}",
        "format": table_definition.format.value,
        "schema": table_definition.schema_.model_dump(mode="json"),
        "partitions": [partition.model_dump(mode="json") for partition in partitions],
        "statistics": statistics.model_dump(mode="json"),
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


def _join_location(base: str, relative: str) -> str:
    parsed = urlsplit(base)
    if parsed.scheme in {"file", "s3"}:
        path = f"{parsed.path.rstrip('/')}/{relative}"
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return str(Path(base) / Path(*PurePosixPath(relative).parts))

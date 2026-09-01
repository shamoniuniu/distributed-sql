"""Iceberg adapter backed by PyIceberg snapshot and manifest planning."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, cast

import pyarrow as pa
from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table import FileScanTask as IcebergFileScanTask
from pyiceberg.table import StaticTable, Table

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.data_source.base import DataSource, FileScanTask, ScanPlan, ScanRequest

type IcebergTableLoader = Callable[[str, dict[str, str]], Table]


class IcebergDataSource(DataSource):
    def __init__(self, table_loader: IcebergTableLoader | None = None) -> None:
        self._table_loader = table_loader or StaticTable.from_metadata

    def plan_scan(self, table: CatalogTable, request: ScanRequest) -> ScanPlan:
        iceberg_table = self._load_table(table)
        scan = self._new_scan(iceberg_table, request)
        native_tasks = tuple(scan.plan_files())
        snapshot = iceberg_table.current_snapshot()
        manifests = tuple(snapshot.manifests(iceberg_table.io)) if snapshot else ()
        tasks = tuple(
            self._convert_task(task, iceberg_table.specs()[task.file.spec_id])
            for task in native_tasks
        )
        tasks = _select_tasks(tasks, request.file_tasks)
        return ScanPlan(
            schema=cast(pa.Schema, schema_to_pyarrow(scan.projection())),
            file_tasks=tasks,
            snapshot_id=snapshot.snapshot_id if snapshot else None,
            metadata={
                "metadata_location": iceberg_table.metadata_location,
                "schema_id": iceberg_table.schema().schema_id,
                "manifest_locations": tuple(manifest.manifest_path for manifest in manifests),
            },
        )

    def scan(self, table: CatalogTable, request: ScanRequest) -> Iterator[pa.RecordBatch]:
        try:
            iceberg_table = self._load_table(table)
            scan = self._new_scan(iceberg_table, request)
            native_tasks = tuple(scan.plan_files())
            selected = _select_native_tasks(native_tasks, request.file_tasks)
            arrow_scan = ArrowScan(
                scan.table_metadata,
                scan.io,
                scan.projection(),
                scan.row_filter,
                scan.case_sensitive,
                scan.limit,
            )
            for batch in arrow_scan.to_record_batches(selected):
                yield from pa.Table.from_batches([batch]).to_batches(
                    max_chunksize=request.batch_size
                )
        except (FileNotFoundError, OSError, ValueError) as exc:
            raise DistributedSQLError(
                ErrorCode.INVALID_REQUEST,
                f"Could not scan Iceberg table {table.namespace}.{table.name}.",
                status_code=422,
                context={"location": table.location, "format": TableFormat.ICEBERG.value},
            ) from exc

    def _load_table(self, table: CatalogTable) -> Table:
        if table.format is not TableFormat.ICEBERG:
            raise ValueError(
                f"{type(self).__name__} cannot scan table format {table.format.value!r}"
            )
        properties = {
            key: _property_value(value)
            for key, value in table.properties.items()
            if key != "metadata_location"
        }
        metadata_location = str(table.properties.get("metadata_location", table.location))
        return self._table_loader(metadata_location, properties)

    @staticmethod
    def _new_scan(table: Table, request: ScanRequest) -> Any:
        selected_fields = request.projection or ("*",)
        row_filter = request.predicate.to_iceberg() if request.predicate else "true"
        return table.scan(
            row_filter=row_filter,
            selected_fields=selected_fields,
            case_sensitive=True,
        )

    @staticmethod
    def _convert_task(
        task: IcebergFileScanTask,
        partition_spec: PartitionSpec,
    ) -> FileScanTask:
        file_format = TableFormat(task.file.file_format.value.lower())
        return FileScanTask(
            location=task.file.file_path,
            format=file_format,
            start=task.start,
            length=task.length,
            record_count=task.file.record_count,
            partition_values={
                field.name: task.file.partition[index]
                for index, field in enumerate(partition_spec.fields)
            },
            delete_files=tuple(delete.file_path for delete in task.delete_files),
        )


def _select_tasks(
    planned: tuple[FileScanTask, ...],
    requested: tuple[FileScanTask, ...] | None,
) -> tuple[FileScanTask, ...]:
    if requested is None:
        return planned
    keys = {(task.location, task.start, task.length) for task in requested}
    return tuple(
        task
        for task in planned
        if (task.location, task.start, task.length) in keys
    )


def _select_native_tasks(
    planned: tuple[IcebergFileScanTask, ...],
    requested: tuple[FileScanTask, ...] | None,
) -> tuple[IcebergFileScanTask, ...]:
    if requested is None:
        return planned
    keys = {(task.location, task.start, task.length) for task in requested}
    return tuple(
        task
        for task in planned
        if (task.file.file_path, task.start, task.length) in keys
    )


def _property_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str | int | float):
        return str(value)
    raise ValueError("Iceberg properties must be scalar values")

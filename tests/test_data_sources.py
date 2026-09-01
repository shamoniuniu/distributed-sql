from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pyarrow as pa
import pyarrow.csv as arrow_csv
import pyarrow.orc as arrow_orc
import pyarrow.parquet as arrow_parquet
import pytest
from fastavro import writer as avro_writer
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.table import Table as IcebergTable
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import DataType, PartitionStrategy, Schema, SchemaField
from distributed_sql.data_source import (
    FileScanTask,
    IcebergDataSource,
    Predicate,
    PredicateOperator,
    ScanRequest,
    create_data_source_registry,
)

ROWS = [
    {"order_id": 1, "region": "north", "amount": 10.5},
    {"order_id": 2, "region": "south", "amount": 20.0},
    {"order_id": 3, "region": None, "amount": 30.5},
    {"order_id": 4, "region": "west", "amount": 40.0},
]


def catalog_schema() -> Schema:
    return Schema(
        fields=[
            SchemaField(name="order_id", data_type=DataType.INT64),
            SchemaField(name="region", data_type=DataType.STRING),
            SchemaField(name="amount", data_type=DataType.FLOAT64),
        ]
    )


def catalog_table(table_format: TableFormat, location: str) -> CatalogTable:
    now = datetime.now(UTC)
    return CatalogTable(
        namespace="sales",
        name=f"orders_{table_format.value}",
        schema=catalog_schema(),
        format=table_format,
        location=location,
        partition_strategy=PartitionStrategy.SINGLE,
        created_at=now,
        updated_at=now,
    )


def write_file_formats(tmp_path: Path) -> dict[TableFormat, Path]:
    table = pa.Table.from_pylist(ROWS)
    locations = {
        table_format: tmp_path / f"orders.{table_format.value}"
        for table_format in (
            TableFormat.CSV,
            TableFormat.PARQUET,
            TableFormat.AVRO,
            TableFormat.ORC,
        )
    }
    with locations[TableFormat.CSV].open("wb") as output:
        arrow_csv.write_csv(table, output)
    arrow_parquet.write_table(table, locations[TableFormat.PARQUET])
    arrow_orc.write_table(table, locations[TableFormat.ORC])
    avro_schema = {
        "type": "record",
        "name": "order",
        "fields": [
            {"name": "order_id", "type": ["null", "long"]},
            {"name": "region", "type": ["null", "string"]},
            {"name": "amount", "type": ["null", "double"]},
        ],
    }
    with locations[TableFormat.AVRO].open("wb") as output:
        avro_writer(output, avro_schema, ROWS)
    return locations


def create_iceberg_table() -> tuple[IcebergTable, Path]:
    identifier = uuid4().hex
    warehouse = f"file:///tmp/distributed-sql-tests/{identifier}"
    catalog = InMemoryCatalog("test", warehouse=warehouse)
    catalog.create_namespace(("sales",))
    table = catalog.create_table(
        ("sales", "orders"),
        IcebergSchema(
            NestedField(1, "order_id", LongType()),
            NestedField(2, "region", StringType()),
            NestedField(3, "amount", DoubleType()),
        ),
    )
    table.append(pa.Table.from_pylist(ROWS))
    return table, Path("/tmp/distributed-sql-tests") / identifier


def scan_rows(
    source: Any,
    table: CatalogTable,
    request: ScanRequest,
) -> list[dict[str, object]]:
    batches = list(source.scan(table, request))
    assert batches
    assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    assert all(batch.num_rows <= request.batch_size for batch in batches)
    return cast(list[dict[str, object]], pa.Table.from_batches(batches).to_pylist())


def test_four_file_formats_apply_projection_predicate_and_batch_size(
    tmp_path: Path,
) -> None:
    locations = write_file_formats(tmp_path)
    stores = ObjectStoreRouter(LocalObjectStore())
    registry = create_data_source_registry(stores)
    request = ScanRequest(
        projection=("order_id", "region"),
        predicate=Predicate("amount", PredicateOperator.GREATER_THAN_OR_EQUAL, 20.0),
        batch_size=2,
    )
    expected = [
        {"order_id": 2, "region": "south"},
        {"order_id": 3, "region": None},
        {"order_id": 4, "region": "west"},
    ]

    results = {}
    for table_format, location in locations.items():
        table = catalog_table(table_format, str(location))
        source = registry.for_table(table)
        results[table_format] = scan_rows(source, table, request)
        assert source.plan_scan(table, request).schema.names == ["order_id", "region"]

    assert all(rows == expected for rows in results.values())


def test_file_task_pushdown_limits_the_scanned_files(tmp_path: Path) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    arrow_parquet.write_table(pa.Table.from_pylist(ROWS[:2]), first)
    arrow_parquet.write_table(pa.Table.from_pylist(ROWS[2:]), second)
    table = catalog_table(TableFormat.PARQUET, str(tmp_path))
    source = create_data_source_registry(
        ObjectStoreRouter(LocalObjectStore())
    ).for_table(table)
    selected_task = FileScanTask(str(second), TableFormat.PARQUET)
    request = ScanRequest(file_tasks=(selected_task,), batch_size=1)

    plan = source.plan_scan(table, request)

    assert plan.file_tasks == (selected_task,)
    assert [row["order_id"] for row in scan_rows(source, table, request)] == [3, 4]


def test_iceberg_current_snapshot_manifest_and_rows_match_file_formats(
    tmp_path: Path,
) -> None:
    locations = write_file_formats(tmp_path)
    iceberg_table, warehouse_path = create_iceberg_table()
    try:
        table = catalog_table(TableFormat.ICEBERG, iceberg_table.metadata_location)
        source = IcebergDataSource()
        request = ScanRequest(
            projection=("order_id", "region"),
            predicate=Predicate("amount", PredicateOperator.GREATER_THAN_OR_EQUAL, 20.0),
            batch_size=2,
        )

        plan = source.plan_scan(table, request)
        iceberg_rows = scan_rows(source, table, request)
        parquet_table = catalog_table(
            TableFormat.PARQUET,
            str(locations[TableFormat.PARQUET]),
        )
        parquet_rows = scan_rows(
            create_data_source_registry(
                ObjectStoreRouter(LocalObjectStore())
            ).for_table(parquet_table),
            parquet_table,
            request,
        )

        snapshot = iceberg_table.current_snapshot()
        assert snapshot is not None
        assert plan.snapshot_id == snapshot.snapshot_id
        assert plan.metadata["schema_id"] == iceberg_table.schema().schema_id
        assert plan.metadata["manifest_locations"]
        assert len(plan.file_tasks) == 1
        assert plan.file_tasks[0].format is TableFormat.PARQUET
        assert iceberg_rows == parquet_rows
    finally:
        shutil.rmtree(warehouse_path, ignore_errors=True)


def test_iceberg_passes_minio_s3_properties_to_pyiceberg() -> None:
    iceberg_table, warehouse_path = create_iceberg_table()
    received: list[tuple[str, dict[str, str]]] = []

    def load_table(location: str, properties: dict[str, str]) -> IcebergTable:
        received.append((location, properties))
        return iceberg_table

    try:
        table = catalog_table(
            TableFormat.ICEBERG,
            "s3://warehouse/sales/orders/metadata/current.metadata.json",
        ).model_copy(
            update={
                "properties": {
                    "s3.endpoint": "http://minio:9000",
                    "s3.access-key-id": "minio",
                    "s3.secret-access-key": "secret",
                    "s3.region": "us-east-1",
                }
            }
        )

        plan = IcebergDataSource(load_table).plan_scan(table, ScanRequest())

        assert plan.snapshot_id is not None
        assert received == [
            (
                table.location,
                {
                    "s3.endpoint": "http://minio:9000",
                    "s3.access-key-id": "minio",
                    "s3.secret-access-key": "secret",
                    "s3.region": "us-east-1",
                },
            )
        ]
    finally:
        shutil.rmtree(warehouse_path, ignore_errors=True)


@pytest.mark.parametrize("batch_size", [0, -1])
def test_scan_request_rejects_invalid_batch_size(batch_size: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        ScanRequest(batch_size=batch_size)

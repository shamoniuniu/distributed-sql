from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import DataType, PartitionStrategy, Schema, SchemaField
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import (
    ExecutionCancelled,
    ExecutionContext,
    FilterOperator,
    LocalExecutor,
    RecordBatchSource,
)
from distributed_sql.planner import Binder, Column, IsNull, TypeInfo


def make_table(
    tmp_path: Path,
    name: str,
    rows: list[dict[str, Any]],
    schema: Schema,
) -> CatalogTable:
    location = tmp_path / f"{name}.parquet"
    arrow_schema = pa.schema(
        [
            pa.field(
                field.name,
                {
                    DataType.INT64: pa.int64(),
                    DataType.STRING: pa.string(),
                }[field.data_type],
                nullable=field.nullable,
            )
            for field in schema.fields
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=arrow_schema), location)
    now = datetime.now(UTC)
    return CatalogTable(
        namespace="default",
        name=name,
        schema=schema,
        format=TableFormat.PARQUET,
        location=str(location),
        partition_strategy=PartitionStrategy.SINGLE,
        created_at=now,
        updated_at=now,
    )


def execute_sql(
    sql: str,
    tables: dict[str, CatalogTable],
    *,
    batch_size: int = 2,
) -> tuple[list[tuple[object, ...]], ExecutionContext]:
    schemas = {name: table.schema_ for name, table in tables.items()}
    plan = Binder(schemas).bind(sql)
    executor = LocalExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
    )
    context = ExecutionContext(batch_size=batch_size)
    table = executor.execute_table(plan, context)
    return [tuple(row.values()) for row in table.to_pylist()], context


def normalized(rows: list[tuple[object, ...]]) -> list[tuple[str, ...]]:
    return sorted(
        tuple("<NULL>" if value is None else repr(value) for value in row) for row in rows
    )


def test_scan_filter_project_limit_batches_and_metrics(tmp_path: Path) -> None:
    schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64, nullable=False),
            SchemaField(name="region", data_type=DataType.STRING),
            SchemaField(name="amount", data_type=DataType.INT64),
        ]
    )
    rows: list[dict[str, Any]] = [
        {"id": 1, "region": "east", "amount": 5},
        {"id": 2, "region": "west", "amount": 20},
        {"id": 3, "region": None, "amount": 30},
        {"id": 4, "region": "east", "amount": 40},
    ]
    table = make_table(tmp_path, "orders", rows, schema)

    actual, context = execute_sql(
        "SELECT id, amount + 1 AS adjusted FROM orders WHERE amount >= 20 LIMIT 2",
        {"default.orders": table},
    )

    assert actual == [(2, 21), (3, 31)]
    assert all(metric.elapsed_seconds >= 0 for metric in context.metrics.values())
    assert sum(metric.output_rows for metric in context.metrics.values()) > len(actual)
    limit_metric = next(
        metric for key, metric in context.metrics.items() if key.startswith("limit")
    )
    assert limit_metric.output_rows == 2


def test_hash_aggregate_matches_duckdb_for_null_and_empty_input(tmp_path: Path) -> None:
    schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64, nullable=False),
            SchemaField(name="region", data_type=DataType.STRING),
            SchemaField(name="amount", data_type=DataType.INT64),
        ]
    )
    rows: list[dict[str, Any]] = [
        {"id": 1, "region": "east", "amount": 10},
        {"id": 2, "region": "east", "amount": 20},
        {"id": 3, "region": "west", "amount": None},
        {"id": 4, "region": None, "amount": 5},
        {"id": 5, "region": None, "amount": 15},
    ]
    table = make_table(tmp_path, "orders", rows, schema)
    sql = """
        SELECT region, COUNT(amount) AS n, SUM(amount) AS total,
               AVG(amount) AS mean, MIN(amount) AS low, MAX(amount) AS high,
               COUNT(DISTINCT amount) AS distinct_amounts
        FROM orders WHERE id > 1 GROUP BY region
    """

    actual, _ = execute_sql(sql, {"default.orders": table})
    connection = duckdb.connect()
    connection.register("orders", pa.Table.from_pylist(rows))
    expected = connection.execute(sql).fetchall()

    assert normalized(actual) == normalized(expected)

    empty_sql = "SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders WHERE id < 0"
    actual_empty, _ = execute_sql(empty_sql, {"default.orders": table})
    assert actual_empty == connection.execute(empty_sql).fetchall() == [(0, None)]


@pytest.mark.parametrize("join_type", ["INNER", "LEFT", "RIGHT", "FULL OUTER"])
def test_hash_join_matches_duckdb_for_duplicates_nulls_and_padding(
    tmp_path: Path,
    join_type: str,
) -> None:
    left_rows: list[dict[str, Any]] = [
        {"id": 1, "value": "L1"},
        {"id": 2, "value": "L2a"},
        {"id": 2, "value": "L2b"},
        {"id": None, "value": "LN"},
        {"id": 4, "value": "L4"},
    ]
    right_rows: list[dict[str, Any]] = [
        {"id": 2, "label": "R2a"},
        {"id": 2, "label": "R2b"},
        {"id": 3, "label": "R3"},
        {"id": None, "label": "RN"},
    ]
    left_schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="value", data_type=DataType.STRING),
        ]
    )
    right_schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="label", data_type=DataType.STRING),
        ]
    )
    tables = {
        "default.left_items": make_table(tmp_path, "left_items", left_rows, left_schema),
        "default.right_items": make_table(tmp_path, "right_items", right_rows, right_schema),
    }
    sql = f"""
        SELECT l.id AS left_id, l.value AS left_value,
               r.id AS right_id, r.label AS right_label
        FROM left_items l {join_type} JOIN right_items r ON l.id = r.id
    """

    actual, _ = execute_sql(sql, tables)
    connection = duckdb.connect()
    connection.register("left_items", pa.Table.from_pylist(left_rows))
    connection.register("right_items", pa.Table.from_pylist(right_rows))
    expected = connection.execute(sql).fetchall()

    assert normalized(actual) == normalized(expected)


def test_operator_cancellation_stops_record_batch_iteration() -> None:
    batch = pa.RecordBatch.from_pylist([{"value": 1}, {"value": None}])
    source = RecordBatchSource("source", [batch, batch])
    predicate = IsNull(Column("value", "", TypeInfo(DataType.INT64)))
    operator = FilterOperator("filter", source, predicate)
    context = ExecutionContext(batch_size=1)
    iterator = operator.execute(context)

    first = next(iterator)
    assert first.to_pylist() == [{"value": None}]
    context.cancellation.cancel()
    with pytest.raises(ExecutionCancelled):
        next(iterator)

    assert context.metrics["filter"].input_batches == 1
    assert context.metrics["source"].output_batches == 1

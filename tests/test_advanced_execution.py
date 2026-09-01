from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa
import pytest

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import DataType, Schema, SchemaField
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import (
    CancellationToken,
    InMemorySorter,
    LocalExecutor,
)
from distributed_sql.planner import Binder, SortExpression
from distributed_sql.planner.expressions import SQLValue
from tests.test_execution import make_table

type MutableRow = dict[str, SQLValue]


class TrackingSorter:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int]] = []
        self._delegate = InMemorySorter()

    def sort(
        self,
        rows: Sequence[MutableRow],
        order_by: Sequence[SortExpression],
        cancellation: CancellationToken,
    ) -> list[MutableRow]:
        self.calls.append((len(rows), len(order_by)))
        return self._delegate.sort(rows, order_by, cancellation)


@pytest.fixture
def advanced_rows() -> list[dict[str, Any]]:
    return [
        {"id": 1, "region": "east", "amount": 10},
        {"id": 2, "region": "east", "amount": 10},
        {"id": 3, "region": "east", "amount": None},
        {"id": 4, "region": "west", "amount": 5},
        {"id": 5, "region": None, "amount": 20},
    ]


@pytest.fixture
def advanced_schema() -> Schema:
    return Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64, nullable=False),
            SchemaField(name="region", data_type=DataType.STRING),
            SchemaField(name="amount", data_type=DataType.INT64),
        ]
    )


def execute_advanced(
    sql: str,
    table: CatalogTable,
    sorter: TrackingSorter | None = None,
) -> list[tuple[object, ...]]:
    plan = Binder({"default.items": table.schema_}).bind(sql)
    executor = LocalExecutor(
        {"default.items": table},
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
        sorter=sorter,
    )
    result = executor.execute_table(plan)
    return [tuple(row.values()) for row in result.to_pylist()]


def duckdb_rows(
    sql: str,
    rows: list[dict[str, Any]],
    schema: Schema,
) -> list[tuple[object, ...]]:
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
    connection = duckdb.connect()
    connection.register("items", pa.Table.from_pylist(rows, schema=arrow_schema))
    return cast(list[tuple[object, ...]], connection.execute(sql).fetchall())


def test_order_having_and_replaceable_sorter_match_duckdb(
    tmp_path: Path,
    advanced_rows: list[dict[str, Any]],
    advanced_schema: Schema,
) -> None:
    table = make_table(tmp_path, "items", advanced_rows, advanced_schema)
    sql = """
        SELECT region, SUM(amount) AS total
        FROM items
        GROUP BY region
        HAVING SUM(amount) >= 10
        ORDER BY total DESC NULLS FIRST, region NULLS LAST
    """
    sorter = TrackingSorter()

    actual = execute_advanced(sql, table, sorter)

    assert actual == duckdb_rows(sql, advanced_rows, advanced_schema)
    assert sorter.calls == [(2, 2)]


def test_ranking_windows_match_duckdb_for_partition_order_peers_and_null(
    tmp_path: Path,
    advanced_rows: list[dict[str, Any]],
    advanced_schema: Schema,
) -> None:
    table = make_table(tmp_path, "items", advanced_rows, advanced_schema)
    sql = """
        SELECT id, region, amount,
               ROW_NUMBER() OVER (
                   PARTITION BY region ORDER BY amount ASC NULLS LAST, id
               ) AS row_num,
               RANK() OVER (
                   PARTITION BY region ORDER BY amount ASC NULLS LAST
               ) AS rank_num,
               DENSE_RANK() OVER (
                   PARTITION BY region ORDER BY amount ASC NULLS LAST
               ) AS dense_rank_num
        FROM items
        ORDER BY id
    """

    assert execute_advanced(sql, table) == duckdb_rows(sql, advanced_rows, advanced_schema)


def test_aggregate_windows_and_rows_frames_match_duckdb(
    tmp_path: Path,
    advanced_rows: list[dict[str, Any]],
    advanced_schema: Schema,
) -> None:
    table = make_table(tmp_path, "items", advanced_rows, advanced_schema)
    sql = """
        SELECT id,
               SUM(amount) OVER (
                   PARTITION BY region ORDER BY id
                   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
               ) AS running_sum,
               AVG(amount) OVER (
                   PARTITION BY region ORDER BY id
                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
               ) AS running_avg,
               MIN(amount) OVER (
                   PARTITION BY region ORDER BY id
                   ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING
               ) AS forward_min,
               MAX(amount) OVER (PARTITION BY region) AS partition_max,
               COUNT(amount) OVER (PARTITION BY region) AS partition_count
        FROM items
        ORDER BY id
    """

    assert execute_advanced(sql, table) == duckdb_rows(sql, advanced_rows, advanced_schema)


def test_grouping_sets_count_distinct_null_and_empty_input_match_duckdb(
    tmp_path: Path,
    advanced_rows: list[dict[str, Any]],
    advanced_schema: Schema,
) -> None:
    table = make_table(tmp_path, "items", advanced_rows, advanced_schema)
    sql = """
        SELECT region, COUNT(DISTINCT amount) AS distinct_amounts,
               COUNT(amount) AS present, SUM(amount) AS total
        FROM items
        GROUP BY GROUPING SETS ((region), ())
        ORDER BY region NULLS LAST, present
    """
    empty_sql = """
        SELECT region, COUNT(DISTINCT amount) AS distinct_amounts,
               COUNT(amount) AS present, SUM(amount) AS total
        FROM items
        WHERE id < 0
        GROUP BY GROUPING SETS ((region), ())
        ORDER BY region NULLS LAST
    """

    assert execute_advanced(sql, table) == duckdb_rows(sql, advanced_rows, advanced_schema)
    assert execute_advanced(empty_sql, table) == duckdb_rows(
        empty_sql, advanced_rows, advanced_schema
    )

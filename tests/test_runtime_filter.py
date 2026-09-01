from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import (
    DataType,
    PartitionStrategy,
    Schema,
    SchemaField,
    Statistics,
)
from distributed_sql.data_source import DataSourceRegistry, create_data_source_registry
from distributed_sql.execution import (
    BloomFilter,
    DistributedExecutor,
    ExecutionContext,
    LocalExecutor,
    LogicalWorker,
    RuntimeFilter,
    ShuffleStore,
    materialize_exchanges,
)
from distributed_sql.optimizer import CostBasedOptimizer
from distributed_sql.planner import Binder
from distributed_sql.planner.logical import Join, Project

SCHEMA = Schema(fields=[SchemaField(name="id", data_type=DataType.INT64)])


def _table(
    root: Path,
    name: str,
    rows: list[dict[str, Any]],
) -> CatalogTable:
    location = root / f"{name}.parquet"
    table = pa.Table.from_pylist(rows, schema=pa.schema([pa.field("id", pa.int64())]))
    pq.write_table(table, location)
    now = datetime.now(UTC)
    return CatalogTable(
        namespace="default",
        name=name,
        schema=SCHEMA,
        format=TableFormat.PARQUET,
        location=str(location),
        partition_strategy=PartitionStrategy.SINGLE,
        statistics=Statistics(
            row_count=len(rows),
            size_bytes=table.nbytes,
            source="test",
        ),
        created_at=now,
        updated_at=now,
    )


def _registry() -> DataSourceRegistry:
    return create_data_source_registry(ObjectStoreRouter(LocalObjectStore()))


def test_runtime_filter_round_trip_merge_and_no_false_negatives() -> None:
    first = RuntimeFilter.create(2, 4)
    second = RuntimeFilter.create(2, 4)
    first.add((1, "a"))
    first.add((2, "b"))
    second.add((3, "c"))
    first.merge(second)

    restored = RuntimeFilter.from_bytes(first.to_bytes())

    assert all(restored.might_contain(value) for value in ((1, "a"), (2, "b"), (3, "c")))
    assert restored.ranges[0].minimum == 1
    assert restored.ranges[0].maximum == 3
    assert not restored.might_contain((None, "a"))


def test_bloom_filter_false_positives_are_tolerated() -> None:
    bloom = BloomFilter(bit_count=8, hash_count=1, bits=bytearray([0xFF]), item_count=1)

    assert bloom.might_contain((999,))


def test_local_runtime_filter_records_scan_reduction_and_preserves_result(
    tmp_path: Path,
) -> None:
    tables = {
        "default.probe": _table(tmp_path, "probe", [{"id": value} for value in range(100)]),
        "default.build": _table(tmp_path, "build", [{"id": 2}, {"id": 4}]),
    }
    plan = Binder(tables).bind(
        "SELECT p.id FROM probe p JOIN build b ON p.id = b.id"
    )
    context = ExecutionContext(batch_size=16)
    executor = LocalExecutor(tables, _registry())

    result = executor.execute_table(plan, context)

    assert sorted(row["id"] for row in result.to_pylist()) == [2, 4]
    scan_metrics = [
        metric
        for name, metric in context.metrics.items()
        if name.startswith("scan") and metric.runtime_filters_applied
    ]
    assert len(scan_metrics) == 1
    assert scan_metrics[0].input_rows == 100
    assert scan_metrics[0].output_rows == 2
    assert scan_metrics[0].runtime_filter_rows_filtered == 98


@pytest.mark.parametrize("join_type", ["LEFT", "FULL OUTER"])
def test_runtime_filter_does_not_filter_outer_join_preserved_probe(
    tmp_path: Path,
    join_type: str,
) -> None:
    tables = {
        "default.probe": _table(tmp_path, f"probe-{join_type}", [{"id": 1}, {"id": 2}]),
        "default.build": _table(tmp_path, f"build-{join_type}", [{"id": 2}]),
    }
    plan = Binder(tables).bind(
        f"SELECT p.id FROM probe p {join_type} JOIN build b ON p.id = b.id"
    )
    context = ExecutionContext()

    result = LocalExecutor(tables, _registry()).execute_table(plan, context)

    assert sorted(row["id"] for row in result.to_pylist()) == [1, 2]
    assert all(
        metric.runtime_filters_applied == 0
        for name, metric in context.metrics.items()
        if name.startswith("scan")
    )


def test_left_join_can_filter_non_preserved_right_probe(tmp_path: Path) -> None:
    tables = {
        "default.build": _table(tmp_path, "left-build", [{"id": 1}, {"id": 2}]),
        "default.probe": _table(
            tmp_path,
            "right-probe",
            [{"id": value} for value in range(2, 100)],
        ),
    }
    plan = Binder(tables).bind(
        "SELECT b.id FROM build b LEFT JOIN probe p ON b.id = p.id"
    )
    assert isinstance(plan, Project)
    assert isinstance(plan.input, Join)
    plan = replace(plan, input=replace(plan.input, build_side="left"))
    context = ExecutionContext()

    result = LocalExecutor(tables, _registry()).execute_table(plan, context)

    assert sorted(row["id"] for row in result.to_pylist()) == [1, 2]
    filtered_scan = [
        metric
        for name, metric in context.metrics.items()
        if name.startswith("scan") and metric.runtime_filters_applied
    ]
    assert len(filtered_scan) == 1
    assert filtered_scan[0].input_rows == 98
    assert filtered_scan[0].output_rows == 1


@pytest.mark.asyncio
async def test_distributed_runtime_filter_uses_cbo_build_side_and_preserves_result(
    tmp_path: Path,
) -> None:
    tables = {
        "default.probe": _table(
            tmp_path,
            "distributed-probe",
            [{"id": value} for value in range(100)],
        ),
        "default.build": _table(
            tmp_path,
            "distributed-build",
            [{"id": 2}, {"id": 4}],
        ),
    }
    optimized = CostBasedOptimizer(tables, broadcast_threshold_bytes=0).optimize(
        Binder(tables).bind("SELECT p.id FROM probe p JOIN build b ON p.id = b.id")
    )
    assert optimized.join_decisions[0].build_side == "right"
    physical = materialize_exchanges(
        optimized.optimized_plan,
        optimized.join_decisions,
        partition_count=2,
    )
    executor = DistributedExecutor(
        tables,
        _registry(),
        [LogicalWorker("worker-1", 1), LogicalWorker("worker-2", 1)],
        ShuffleStore(str(tmp_path / "shuffle"), ObjectStoreRouter(LocalObjectStore())),
    )

    result = await executor.execute("runtime-filter-query", physical)

    assert sorted(row["id"] for row in result.table.to_pylist()) == [2, 4]
    assert result.runtime_filter_metrics.input_rows == 102
    assert result.runtime_filter_metrics.output_rows == 4
    assert result.runtime_filter_metrics.filtered_rows == 98
    assert result.runtime_filter_metrics.filters_applied > 0

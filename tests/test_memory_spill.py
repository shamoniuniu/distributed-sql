from __future__ import annotations

import errno
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import DataType, Schema, SchemaField
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import (
    CancellationToken,
    DistributedExecutor,
    ExecutionContext,
    InMemorySorter,
    LocalExecutor,
    LogicalWorker,
    MemoryAccount,
    MemoryLimitExceeded,
    ShuffleStore,
    materialize_exchanges,
)
from distributed_sql.planner import Binder, SortExpression
from distributed_sql.planner.expressions import SQLValue
from tests.test_distributed_execution import _partitioned_table
from tests.test_execution import make_table

type MutableRow = dict[str, SQLValue]


class InterruptingSorter:
    def __init__(self, outcome: str) -> None:
        self.outcome = outcome
        self.calls = 0
        self.delegate = InMemorySorter()

    def sort(
        self,
        rows: Sequence[MutableRow],
        order_by: Sequence[SortExpression],
        cancellation: CancellationToken,
    ) -> list[MutableRow]:
        self.calls += 1
        if self.calls > 1:
            if self.outcome == "cancel":
                cancellation.cancel()
                cancellation.check()
            raise RuntimeError("sort failed")
        return self.delegate.sort(rows, order_by, cancellation)


def _execute(
    tmp_path: Path,
    sql: str,
    tables: dict[str, Any],
    *,
    memory_limit_bytes: int = 1_024,
) -> tuple[pa.Table, ExecutionContext]:
    plan = Binder({name: table.schema_ for name, table in tables.items()}).bind(sql)
    context = ExecutionContext(
        batch_size=7,
        memory_limit_bytes=memory_limit_bytes,
        temp_root=tmp_path / "spill",
        query_id="query-test",
        task_id="task-test",
    )
    executor = LocalExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
    )
    return executor.execute_table(plan, context), context


def test_memory_accounts_enforce_task_and_query_limits() -> None:
    query = MemoryAccount("query", 100)
    first = query.child("task-1", 60)
    second = query.child("task-2", 80)

    first.reserve(60)
    with pytest.raises(MemoryLimitExceeded):
        first.reserve(1)
    with pytest.raises(MemoryLimitExceeded):
        second.reserve(41)

    first.release(60)
    second.reserve(80)
    second.release(80)
    assert query.current_bytes == first.current_bytes == second.current_bytes == 0
    assert query.peak_bytes == 80


def test_external_merge_sort_spills_and_cleans_successfully(tmp_path: Path) -> None:
    schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="payload", data_type=DataType.STRING),
        ]
    )
    rows = [{"id": value, "payload": "x" * 80} for value in reversed(range(60))]
    table = make_table(tmp_path, "sort_items", rows, schema)

    result, context = _execute(
        tmp_path,
        "SELECT id, payload FROM sort_items ORDER BY id",
        {"default.sort_items": table},
    )

    assert [row["id"] for row in result.to_pylist()] == list(range(60))
    assert context.spill_metrics.external_sort_runs > 1
    assert context.spill_metrics.spill_bytes > 0
    assert context.spill_metrics.peak_memory_bytes <= 1_024
    assert not (tmp_path / "spill" / "query-test").exists()


def test_sort_aggregate_spills_with_high_cardinality(tmp_path: Path) -> None:
    schema = Schema(
        fields=[
            SchemaField(name="group_id", data_type=DataType.INT64),
            SchemaField(name="amount", data_type=DataType.INT64),
        ]
    )
    rows = [{"group_id": value % 30, "amount": value} for value in range(120)]
    table = make_table(tmp_path, "aggregate_items", rows, schema)

    result, context = _execute(
        tmp_path,
        """
        SELECT group_id, COUNT(*) AS n, SUM(amount) AS total
        FROM aggregate_items GROUP BY group_id ORDER BY group_id
        """,
        {"default.aggregate_items": table},
        memory_limit_bytes=768,
    )

    assert result.num_rows == 30
    assert sum(row["n"] for row in result.to_pylist()) == 120
    assert context.spill_metrics.sort_aggregate_runs > 0
    assert context.spill_metrics.spill_bytes > 0
    assert not (tmp_path / "spill" / "query-test").exists()


def test_partition_hash_join_falls_back_to_sort_merge(tmp_path: Path) -> None:
    schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="payload", data_type=DataType.STRING),
        ]
    )
    left_rows = [{"id": value, "payload": f"left-{value:03d}" * 8} for value in range(30)]
    right_rows = [{"id": value, "payload": f"right-{value:03d}" * 8} for value in range(15, 45)]
    tables = {
        "default.left_items": make_table(tmp_path, "left_items", left_rows, schema),
        "default.right_items": make_table(tmp_path, "right_items", right_rows, schema),
    }

    result, context = _execute(
        tmp_path,
        """
        SELECT l.id, l.payload AS left_payload, r.payload AS right_payload
        FROM left_items l JOIN right_items r ON l.id = r.id ORDER BY l.id
        """,
        tables,
        memory_limit_bytes=512,
    )

    assert result.num_rows == 15
    assert context.spill_metrics.hash_partitions > 0
    assert context.spill_metrics.sort_merge_fallbacks > 0
    assert context.spill_metrics.peak_memory_bytes <= 512
    assert not (tmp_path / "spill" / "query-test").exists()


@pytest.mark.parametrize(
    ("outcome", "error_type"),
    [
        pytest.param("failure", RuntimeError, id="failure"),
        pytest.param("cancel", RuntimeError, id="cancellation"),
        pytest.param("disk-full", DistributedSQLError, id="disk-full"),
    ],
)
def test_temp_files_cleaned_after_failure_cancellation_and_disk_full(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    outcome: str,
    error_type: type[Exception],
) -> None:
    table_name = f"cleanup_{outcome.replace('-', '_')}"
    schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="payload", data_type=DataType.STRING),
        ]
    )
    rows = [{"id": value, "payload": "x" * 80} for value in reversed(range(30))]
    table = make_table(tmp_path, table_name, rows, schema)
    plan = Binder({f"default.{table_name}": table.schema_}).bind(
        f"SELECT id FROM {table_name} ORDER BY id"
    )
    sorter = InterruptingSorter(outcome)
    context = ExecutionContext(
        batch_size=5,
        memory_limit_bytes=512,
        temp_root=tmp_path / "spill",
        query_id=f"query-{outcome}",
        task_id="task",
    )
    executor = LocalExecutor(
        {f"default.{table_name}": table},
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
        sorter=sorter,
    )

    def disk_full(*_args: object, **_kwargs: object) -> None:
        raise OSError(errno.ENOSPC, "No space left on device")

    if outcome == "disk-full":
        monkeypatch.setattr("distributed_sql.execution.memory.pq.write_table", disk_full)
    with pytest.raises(error_type) as error:
        executor.execute_table(plan, context)

    if outcome == "disk-full":
        assert isinstance(error.value, DistributedSQLError)
        assert error.value.code is ErrorCode.RESOURCE_EXHAUSTED
    assert not (tmp_path / "spill" / f"query-{outcome}").exists()


@pytest.mark.asyncio
async def test_distributed_spill_metrics_include_query_and_task_totals(
    tmp_path: Path,
) -> None:
    schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="payload", data_type=DataType.STRING),
        ]
    )
    rows = [{"id": value, "payload": "x" * 80} for value in reversed(range(40))]
    table = _partitioned_table(tmp_path, "distributed_sort", rows, schema)
    tables = {"default.distributed_sort": table}
    plan = Binder(tables).bind("SELECT id, payload FROM distributed_sort ORDER BY id")
    physical = materialize_exchanges(plan, (), partition_count=2)
    executor = DistributedExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
        [LogicalWorker("worker-1", 1)],
        ShuffleStore(str(tmp_path / "shuffle"), ObjectStoreRouter(LocalObjectStore())),
        memory_limit_bytes=768,
        temp_root=tmp_path / "spill",
    )

    result = await executor.execute("distributed-spill", physical)

    assert [row["id"] for row in result.table.to_pylist()] == list(range(40))
    assert result.spill_metrics.external_sort_runs > 1
    assert result.shuffle_metrics.spill_bytes == result.spill_metrics.spill_bytes
    assert any(metric.spill_bytes > 0 for metric in result.task_spill_metrics.values())
    assert not (tmp_path / "spill" / "distributed-spill").exists()


@pytest.mark.asyncio
async def test_distributed_large_partition_uses_sort_aggregate_spill(
    tmp_path: Path,
) -> None:
    schema = Schema(
        fields=[
            SchemaField(name="group_id", data_type=DataType.INT64),
            SchemaField(name="amount", data_type=DataType.INT64),
        ]
    )
    rows = [{"group_id": value % 20, "amount": value} for value in range(80)]
    table = _partitioned_table(tmp_path, "distributed_aggregate", rows, schema)
    tables = {"default.distributed_aggregate": table}
    plan = Binder(tables).bind(
        "SELECT group_id, SUM(amount) AS total FROM distributed_aggregate GROUP BY group_id"
    )
    physical = materialize_exchanges(plan, (), partition_count=2)
    executor = DistributedExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
        [LogicalWorker("worker-1", 1)],
        ShuffleStore(str(tmp_path / "shuffle"), ObjectStoreRouter(LocalObjectStore())),
        memory_limit_bytes=512,
        temp_root=tmp_path / "spill",
    )

    result = await executor.execute("distributed-aggregate-spill", physical)

    assert result.table.num_rows == 20
    assert result.spill_metrics.sort_aggregate_runs > 0
    assert not (tmp_path / "spill" / "distributed-aggregate-spill").exists()

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import (
    DataType,
    Partition,
    PartitionStrategy,
    Schema,
    SchemaField,
    Stage,
    Task,
    TaskState,
)
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import (
    DistributedExecutor,
    Exchange,
    LogicalWorker,
    ShuffleStore,
    StageGraph,
    StagePlanner,
    TaskScheduler,
    materialize_exchanges,
)
from distributed_sql.optimizer import CostBasedOptimizer, JoinStrategy
from distributed_sql.planner import Binder, Join, LogicalPlan, Project


def _partitioned_table(
    root: Path,
    name: str,
    rows: list[dict[str, Any]],
    schema: Schema,
    *,
    partition_count: int = 2,
    partition_key: str | None = None,
) -> CatalogTable:
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
    buckets: list[list[dict[str, Any]]] = [[] for _ in range(partition_count)]
    for index, row in enumerate(rows):
        if partition_key is None:
            target = index % partition_count
        else:
            encoded = json.dumps(
                [row[partition_key]],
                ensure_ascii=True,
                separators=(",", ":"),
            ).encode()
            target = int.from_bytes(hashlib.sha256(encoded).digest()[:8], "big")
            target %= partition_count
        buckets[target].append(row)
    partitions = []
    for ordinal, bucket in enumerate(buckets):
        location = root / name / f"part-{ordinal:05d}.parquet"
        location.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(bucket, schema=arrow_schema)
        pq.write_table(table, location)
        partitions.append(
            Partition(
                partition_id=f"{name}-{ordinal}",
                ordinal=ordinal,
                location=str(location),
                strategy=(
                    PartitionStrategy.HASH
                    if partition_key
                    else PartitionStrategy.ROUND_ROBIN
                ),
                keys=[partition_key] if partition_key else [],
                row_count=table.num_rows,
                size_bytes=location.stat().st_size,
            )
        )
    now = datetime.now(UTC)
    return CatalogTable(
        namespace="default",
        name=name,
        schema=schema,
        format=TableFormat.PARQUET,
        location=str(root / name),
        partition_strategy=(
            PartitionStrategy.HASH if partition_key else PartitionStrategy.ROUND_ROBIN
        ),
        partition_keys=[partition_key] if partition_key else [],
        partitions=partitions,
        created_at=now,
        updated_at=now,
    )


def _join_node(plan: LogicalPlan) -> Join:
    while isinstance(plan, Project):
        plan = plan.input
    assert isinstance(plan, Join)
    return plan


def _task(stage: Stage, ordinal: int) -> Task:
    return Task(
        task_id=f"{stage.stage_id}-task-{ordinal}",
        query_id=stage.query_id,
        stage_id=stage.stage_id,
        partition=Partition(
            partition_id=f"{stage.stage_id}-partition-{ordinal}",
            ordinal=ordinal,
            location="",
        ),
    )


@pytest.mark.parametrize(
    ("strategy", "left_exchange", "right_exchange"),
    [
        (JoinStrategy.REUSE, False, False),
        (JoinStrategy.REPARTITION_LEFT, True, False),
        (JoinStrategy.REPARTITION_RIGHT, False, True),
        (JoinStrategy.REPARTITION_BOTH, True, True),
    ],
)
def test_join_strategy_materializes_expected_exchanges(
    tmp_path: Path,
    strategy: JoinStrategy,
    left_exchange: bool,
    right_exchange: bool,
) -> None:
    schema = Schema(fields=[SchemaField(name="id", data_type=DataType.INT64)])
    tables = {
        "default.a": _partitioned_table(tmp_path, "a", [{"id": 1}], schema),
        "default.b": _partitioned_table(tmp_path, "b", [{"id": 1}], schema),
    }
    plan = Binder(tables).bind("SELECT a.id FROM a JOIN b ON a.id = b.id")
    result = CostBasedOptimizer(tables, broadcast_threshold_bytes=0).optimize(plan)
    decision = replace(result.join_decisions[0], strategy=strategy)

    physical = materialize_exchanges(result.optimized_plan, (decision,), partition_count=2)
    assert isinstance(physical, Project)
    join = _join_node(physical)

    assert isinstance(join.left, Exchange) is left_exchange
    assert isinstance(join.right, Exchange) is right_exchange
    graph = StagePlanner("query-plan").plan(physical)
    expected_stages = 1 + int(left_exchange) + int(right_exchange)
    assert len(graph.stages) == expected_stages
    assert len(graph.stage(graph.root_stage_id).dependency_stage_ids) == (
        int(left_exchange) + int(right_exchange)
    )


def test_shuffle_attempt_isolation_atomic_manifest_metrics_and_integrity(
    tmp_path: Path,
) -> None:
    stores = ObjectStoreRouter(LocalObjectStore())
    shuffle = ShuffleStore(str(tmp_path / "shuffle"), stores)
    table = pa.table({"key": [1, 2, 1, None], "value": ["a", "b", "c", "d"]})
    expression = Binder(
        {
            "default.t": _partitioned_table(
                tmp_path,
                "t",
                table.to_pylist(),
                Schema(
                    fields=[
                        SchemaField(name="key", data_type=DataType.INT64),
                        SchemaField(name="value", data_type=DataType.STRING),
                    ]
                ),
            )
        }
    ).bind("SELECT key FROM t")
    assert isinstance(expression, Project)
    key = expression.expressions[0].expression

    first = shuffle.write(
        query_id="q",
        stage_id="s",
        task_id="t",
        attempt_id="a0",
        table=table,
        partition_count=2,
        keys=(key,),
    )
    second = shuffle.write(
        query_id="q",
        stage_id="s",
        task_id="t",
        attempt_id="a1",
        table=table,
        partition_count=2,
        keys=(key,),
    )

    assert first.files[0].location != second.files[0].location
    assert Path(shuffle.manifest_location("q", "s", "t", "a0")).is_file()
    combined = sum(
        shuffle.read_partition([first], ordinal)[0].num_rows for ordinal in range(2)
    )
    assert combined == table.num_rows
    assert first.metrics.records_written == table.num_rows
    assert first.metrics.bytes_written > 0
    Path(first.files[0].location).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="size mismatch|checksum mismatch"):
        shuffle.read_partition([first], 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("strategy", "left_partitioned", "right_partitioned"),
    [
        (JoinStrategy.REUSE, True, True),
        (JoinStrategy.REPARTITION_LEFT, False, True),
        (JoinStrategy.REPARTITION_RIGHT, True, False),
        (JoinStrategy.REPARTITION_BOTH, False, False),
        (JoinStrategy.BROADCAST, False, False),
    ],
)
async def test_all_join_distribution_strategies_execute_correctly(
    tmp_path: Path,
    strategy: JoinStrategy,
    left_partitioned: bool,
    right_partitioned: bool,
) -> None:
    schema = Schema(fields=[SchemaField(name="id", data_type=DataType.INT64)])
    tables = {
        "default.a": _partitioned_table(
            tmp_path,
            "strategy-a",
            [{"id": 1}, {"id": 2}, {"id": 4}],
            schema,
            partition_key="id" if left_partitioned else None,
        ),
        "default.b": _partitioned_table(
            tmp_path,
            "strategy-b",
            [{"id": 2}, {"id": 3}, {"id": 4}],
            schema,
            partition_key="id" if right_partitioned else None,
        ),
    }
    optimized = CostBasedOptimizer(tables, broadcast_threshold_bytes=0).optimize(
        Binder(tables).bind("SELECT a.id FROM a JOIN b ON a.id = b.id")
    )
    decision = replace(optimized.join_decisions[0], strategy=strategy)
    physical = materialize_exchanges(
        optimized.optimized_plan,
        (decision,),
        partition_count=2,
    )
    executor = DistributedExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
        [LogicalWorker("worker-1", 1), LogicalWorker("worker-2", 1)],
        ShuffleStore(
            str(tmp_path / f"shuffle-{strategy.value}"),
            ObjectStoreRouter(LocalObjectStore()),
        ),
    )

    result = await executor.execute(f"query-{strategy.value}", physical)

    assert sorted(row["id"] for row in result.table.to_pylist()) == [2, 4]
    if strategy is JoinStrategy.REUSE:
        assert result.shuffle_metrics.records_written == 0
    else:
        assert result.shuffle_metrics.records_written > 0


@pytest.mark.asyncio
async def test_scheduler_honors_dependencies_slots_state_and_cancellation() -> None:
    workers = [LogicalWorker("worker-1", 1), LogicalWorker("worker-2", 1)]
    scheduler = TaskScheduler(workers)
    from distributed_sql.common.protocol import PlanNode, PlanNodeType

    protocol_plan = PlanNode(node_id="output", node_type=PlanNodeType.OUTPUT)
    first = Stage(stage_id="s1", query_id="q", plan=protocol_plan, partition_count=2)
    second = Stage(
        stage_id="s2",
        query_id="q",
        plan=protocol_plan,
        dependency_stage_ids=["s1"],
    )
    graph = StageGraph("s2", (first, second), (_task(first, 0), _task(first, 1), _task(second, 0)))
    events: list[str] = []

    async def first_runner(attempt_id: str, _token: object) -> str:
        events.append(f"start:{attempt_id}")
        await asyncio.sleep(0.01)
        events.append(f"done:{attempt_id}")
        return attempt_id

    def second_runner(attempt_id: str, _token: object) -> str:
        assert len([item for item in events if item.startswith("done:")]) == 2
        return attempt_id

    runners = {
        graph.tasks[0].task_id: first_runner,
        graph.tasks[1].task_id: first_runner,
        graph.tasks[2].task_id: second_runner,
    }
    result = await scheduler.run(graph, runners)

    assert all(task.state is TaskState.SUCCEEDED for task in result.tasks.values())
    assert {outcome.worker_id for outcome in result.outcomes.values()} == {
        "worker-1",
        "worker-2",
    }
    assert all(value <= 1 for value in result.max_running_by_worker.values())

    cancel_stage = Stage(
        stage_id="cancel-stage",
        query_id="cancel-query",
        plan=protocol_plan,
    )
    cancel_task = _task(cancel_stage, 0)
    started = asyncio.Event()
    release = asyncio.Event()

    async def cancellable(_attempt_id: str, token: object) -> None:
        from distributed_sql.execution import CancellationToken

        started.set()
        await release.wait()
        assert isinstance(token, CancellationToken)
        token.check()

    pending = asyncio.create_task(
        scheduler.run(
            StageGraph(cancel_stage.stage_id, (cancel_stage,), (cancel_task,)),
            {cancel_task.task_id: cancellable},
        )
    )
    await started.wait()
    scheduler.cancel("cancel-query")
    release.set()
    canceled = await pending
    assert canceled.tasks[cancel_task.task_id].state is TaskState.CANCELED


@pytest.mark.asyncio
async def test_multi_worker_join_aggregate_limit_and_partition_ownership(
    tmp_path: Path,
) -> None:
    order_schema = Schema(
        fields=[
            SchemaField(name="customer_id", data_type=DataType.INT64),
            SchemaField(name="amount", data_type=DataType.INT64),
        ]
    )
    customer_schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="region", data_type=DataType.STRING),
        ]
    )
    tables = {
        "default.orders": _partitioned_table(
            tmp_path,
            "orders",
            [
                {"customer_id": 1, "amount": 10},
                {"customer_id": 2, "amount": 20},
                {"customer_id": 1, "amount": 30},
                {"customer_id": 3, "amount": 40},
            ],
            order_schema,
        ),
        "default.customers": _partitioned_table(
            tmp_path,
            "customers",
            [
                {"id": 1, "region": "east"},
                {"id": 2, "region": "west"},
                {"id": 3, "region": "east"},
            ],
            customer_schema,
        ),
    }
    sql = """
        SELECT c.region, SUM(o.amount) AS total
        FROM orders o JOIN customers c ON o.customer_id = c.id
        GROUP BY c.region
        LIMIT 2
    """
    optimized = CostBasedOptimizer(tables, broadcast_threshold_bytes=0).optimize(
        Binder(tables).bind(sql)
    )
    physical = materialize_exchanges(
        optimized.optimized_plan,
        optimized.join_decisions,
        partition_count=2,
    )
    workers = [LogicalWorker("worker-1", 1), LogicalWorker("worker-2", 1)]
    executor = DistributedExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
        workers,
        ShuffleStore(str(tmp_path / "shuffle"), ObjectStoreRouter(LocalObjectStore())),
    )

    result = await executor.execute("query-integration", physical)

    assert sorted(result.table.to_pylist(), key=lambda row: row["region"]) == [
        {"region": "east", "total": 80},
        {"region": "west", "total": 20},
    ]
    assigned_workers = {
        outcome.worker_id
        for schedule in result.schedules
        for outcome in schedule.outcomes.values()
    }
    assert assigned_workers == {"worker-1", "worker-2"}
    assert result.shuffle_metrics.records_written > 0
    assert result.shuffle_metrics.records_read > 0
    assert result.shuffle_metrics.partition_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(
            """
            SELECT id, region,
                   ROW_NUMBER() OVER (
                       PARTITION BY region ORDER BY amount NULLS LAST, id
                   ) AS row_num,
                   SUM(amount) OVER (
                       PARTITION BY region ORDER BY id
                       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                   ) AS running_total
            FROM advanced_items
            ORDER BY id
            """,
            id="分区窗口与全局排序",
        ),
        pytest.param(
            """
            SELECT region, COUNT(DISTINCT amount) AS distinct_amounts,
                   SUM(amount) AS total
            FROM advanced_items
            GROUP BY GROUPING SETS ((region), ())
            ORDER BY region NULLS LAST, total
            """,
            id="分组集与去重聚合",
        ),
    ],
)
async def test_distributed_advanced_sql_matches_duckdb(
    tmp_path: Path,
    sql: str,
) -> None:
    rows: list[dict[str, Any]] = [
        {"id": 1, "region": "east", "amount": 10},
        {"id": 2, "region": "east", "amount": 10},
        {"id": 3, "region": "east", "amount": None},
        {"id": 4, "region": "west", "amount": 5},
        {"id": 5, "region": None, "amount": 20},
    ]
    schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64, nullable=False),
            SchemaField(name="region", data_type=DataType.STRING),
            SchemaField(name="amount", data_type=DataType.INT64),
        ]
    )
    table = _partitioned_table(tmp_path, "advanced_items", rows, schema)
    tables = {"default.advanced_items": table}
    physical = materialize_exchanges(Binder(tables).bind(sql), (), partition_count=2)
    executor = DistributedExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
        [LogicalWorker("worker-1", 1), LogicalWorker("worker-2", 1)],
        ShuffleStore(
            str(tmp_path / "advanced-shuffle"),
            ObjectStoreRouter(LocalObjectStore()),
        ),
    )
    connection = duckdb.connect()
    connection.register("advanced_items", pa.Table.from_pylist(rows))

    result = await executor.execute("query-advanced", physical)

    assert [tuple(row.values()) for row in result.table.to_pylist()] == connection.execute(
        sql
    ).fetchall()
    assert result.shuffle_metrics.records_written > 0

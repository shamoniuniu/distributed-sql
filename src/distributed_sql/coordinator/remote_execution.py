"""Partitioned execution whose data operators run in remote Worker processes."""

from __future__ import annotations

import base64
import pickle
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.catalog.storage import ObjectStoreRouter
from distributed_sql.common.protocol import (
    ArtifactReference,
    Partition,
    RemoteTaskMetrics,
    RemoteTaskOperation,
    RemoteTaskResult,
    SerializedPlan,
    Stage,
    Task,
)
from distributed_sql.coordinator.remote import RemoteTaskCommand, RemoteWorker
from distributed_sql.data_source import DataSourceRegistry, ScanRequest
from distributed_sql.execution.distributed import DataPartition, DistributedResult
from distributed_sql.execution.memory import SpillMetrics
from distributed_sql.execution.operators import ExecutionCancelled
from distributed_sql.execution.physical import Exchange, PhysicalPlan, StageGraph, StagePlanner
from distributed_sql.execution.runtime_filter import RuntimeFilterMetrics
from distributed_sql.execution.scheduler import (
    RetryPolicy,
    ScheduleResult,
    TaskScheduler,
    WorkerLeaseRegistry,
)
from distributed_sql.execution.shuffle import ShuffleManifest, ShuffleMetrics
from distributed_sql.planner.logical import Join, Scan


@dataclass(frozen=True, slots=True)
class _RemotePartition:
    ordinal: int
    artifact: ArtifactReference
    worker_id: str


class RemoteDistributedExecutor:
    """Coordinate partition Tasks while Workers execute every data operation."""

    def __init__(
        self,
        tables: Mapping[str, CatalogTable],
        data_sources: DataSourceRegistry,
        workers: list[RemoteWorker],
        stores: ObjectStoreRouter,
        runtime_root: str | Path,
        *,
        registry: WorkerLeaseRegistry | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._tables = {name.casefold(): table for name, table in tables.items()}
        self._data_sources = data_sources
        self._stores = stores
        self._runtime_root = str(runtime_root)
        self._scheduler = TaskScheduler(
            workers,
            registry=registry,
            retry_policy=retry_policy,
        )
        self._schedules: list[ScheduleResult] = []
        self._shuffle_metrics = ShuffleMetrics()
        self._spill_metrics = SpillMetrics()
        self._task_spill_metrics: dict[str, SpillMetrics] = {}
        self._next_stage = 0
        self._cancelled_queries: set[str] = set()

    def cancel(self, query_id: str) -> None:
        self._cancelled_queries.add(query_id)
        self._scheduler.cancel(query_id)

    async def execute(self, query_id: str, plan: PhysicalPlan) -> DistributedResult:
        self._schedules.clear()
        self._shuffle_metrics = ShuffleMetrics()
        self._spill_metrics = SpillMetrics()
        self._task_spill_metrics = {}
        self._next_stage = 0
        self._cancelled_queries.discard(query_id)
        stage_graph = StagePlanner(query_id).plan(plan)
        remote = await self._execute_node(query_id, plan)
        ordered = sorted(remote, key=lambda item: item.ordinal)
        tables = [self._read_parquet(item.artifact.location) for item in ordered]
        table = (
            pa.concat_tables(tables) if tables else pa.Table.from_batches([], schema=pa.schema([]))
        )
        return DistributedResult(
            table=table,
            partitions=tuple(
                DataPartition(item.ordinal, value, item.worker_id)
                for item, value in zip(ordered, tables, strict=True)
            ),
            stage_graph=stage_graph,
            schedules=tuple(self._schedules),
            shuffle_metrics=self._shuffle_metrics,
            runtime_filter_metrics=RuntimeFilterMetrics(),
            spill_metrics=self._spill_metrics,
            task_spill_metrics=dict(self._task_spill_metrics),
        )

    async def _execute_node(
        self,
        query_id: str,
        plan: PhysicalPlan,
    ) -> tuple[_RemotePartition, ...]:
        if isinstance(plan, Scan):
            return await self._scan(query_id, plan)
        if isinstance(plan, Exchange):
            source = await self._execute_node(query_id, plan.input)
            return await self._exchange(query_id, plan, source)
        if isinstance(plan, Join):
            left = await self._execute_node(query_id, plan.left)
            right = await self._execute_node(query_id, plan.right)
            count = max(len(left), len(right))
            if len(left) not in {1, count} or len(right) not in {1, count}:
                raise ValueError("Join inputs have incompatible partition counts")
            commands = []
            for ordinal in range(count):
                left_item = left[0] if len(left) == 1 else left[ordinal]
                right_item = right[0] if len(right) == 1 else right[ordinal]
                commands.append(
                    self._command(
                        query_id,
                        plan.node_id,
                        RemoteTaskOperation.JOIN,
                        {
                            "plan": _serialize_plan(plan),
                            "left_location": left_item.artifact.location,
                            "right_location": right_item.artifact.location,
                        },
                    )
                )
            return await self._run(query_id, plan, commands)
        child = await self._execute_node(query_id, plan.children[0])
        commands = [
            self._command(
                query_id,
                plan.node_id,
                RemoteTaskOperation.UNARY,
                {
                    "plan": _serialize_plan(plan),
                    "input_location": item.artifact.location,
                },
            )
            for item in child
        ]
        return await self._run(query_id, plan, commands)

    async def _scan(
        self,
        query_id: str,
        plan: Scan,
    ) -> tuple[_RemotePartition, ...]:
        table = self._tables[plan.table_name.casefold()]
        source = self._data_sources.for_table(table)
        request = ScanRequest(projection=tuple(field.name for field in plan.output_schema.fields))
        commands = [
            self._command(
                query_id,
                plan.node_id,
                RemoteTaskOperation.SCAN,
                {
                    "plan": _serialize_plan(plan),
                    "table": table.model_dump(mode="json", by_alias=True),
                    "file_task": {
                        "location": file_task.location,
                        "format": file_task.format.value,
                        "start": file_task.start,
                        "length": file_task.length,
                        "record_count": file_task.record_count,
                        "partition_values": file_task.partition_values,
                        "delete_files": list(file_task.delete_files),
                    },
                },
            )
            for file_task in source.plan_scan(table, request).file_tasks
        ]
        return await self._run(query_id, plan, commands)

    async def _exchange(
        self,
        query_id: str,
        exchange: Exchange,
        source: tuple[_RemotePartition, ...],
    ) -> tuple[_RemotePartition, ...]:
        stage_id = self._stage_id(exchange.node_id)
        writes = [
            RemoteTaskCommand(
                task_id=f"{stage_id}-write-{item.ordinal:05d}",
                query_id=query_id,
                stage_id=stage_id,
                operation=RemoteTaskOperation.SHUFFLE_WRITE,
                payload={
                    "source_location": item.artifact.location,
                    "shuffle_root": _join_location(self._runtime_root, "shuffle"),
                    "partition_count": exchange.partition_count,
                    "keys": [key.sql() for key in exchange.keys],
                    "broadcast": exchange.strategy.value == "broadcast",
                },
                output_root=_join_location(self._runtime_root, "results"),
            )
            for item in source
        ]
        write_results = await self._run_commands(query_id, exchange, writes)
        manifests = [
            ShuffleManifest.model_validate(raw)
            for result in write_results
            for raw in result.shuffle_manifests
        ]
        for manifest in manifests:
            self._shuffle_metrics = self._shuffle_metrics.add(manifest.metrics)
        reads = [
            RemoteTaskCommand(
                task_id=f"{stage_id}-read-{partition:05d}",
                query_id=query_id,
                stage_id=f"{stage_id}-read",
                operation=RemoteTaskOperation.SHUFFLE_READ,
                payload={
                    "shuffle_root": _join_location(self._runtime_root, "shuffle"),
                    "partition": partition,
                    "manifests": [manifest.model_dump(mode="json") for manifest in manifests],
                },
                output_root=_join_location(self._runtime_root, "results"),
            )
            for partition in range(exchange.partition_count)
        ]
        results = await self._run_commands(query_id, exchange, reads)
        for result in results:
            metrics = result.metrics
            self._shuffle_metrics = self._shuffle_metrics.add(
                ShuffleMetrics(
                    records_read=_metric_int(metrics, "shuffle_records_read"),
                    bytes_read=_metric_int(metrics, "shuffle_bytes_read"),
                    partition_count=1,
                    read_seconds=_metric_float(metrics, "shuffle_read_seconds"),
                )
            )
        return _partitions(results)

    async def _run(
        self,
        query_id: str,
        plan: PhysicalPlan,
        commands: list[RemoteTaskCommand],
    ) -> tuple[_RemotePartition, ...]:
        return _partitions(await self._run_commands(query_id, plan, commands))

    async def _run_commands(
        self,
        query_id: str,
        plan: PhysicalPlan,
        commands: list[RemoteTaskCommand],
    ) -> list[RemoteTaskResult]:
        if query_id in self._cancelled_queries:
            raise ExecutionCancelled("Query execution was cancelled.")
        if not commands:
            return []
        stage_id = commands[0].stage_id
        normalized = [
            replace(
                command,
                stage_id=stage_id,
                task_id=f"{stage_id}-task-{ordinal:05d}",
            )
            for ordinal, command in enumerate(commands)
        ]
        stage = Stage(
            stage_id=stage_id,
            query_id=query_id,
            plan=plan.to_protocol(),
            partition_count=len(commands),
        )
        tasks = tuple(
            Task(
                task_id=command.task_id,
                query_id=query_id,
                stage_id=stage_id,
                partition=Partition(
                    partition_id=f"{command.task_id}-partition",
                    ordinal=ordinal,
                    location="",
                ),
            )
            for ordinal, command in enumerate(normalized)
        )
        schedule = await self._scheduler.run(
            StageGraph(stage_id, (stage,), tasks),
            {task.task_id: command for task, command in zip(tasks, normalized, strict=True)},
        )
        self._schedules.append(schedule)
        if query_id in self._cancelled_queries:
            raise ExecutionCancelled("Query execution was cancelled.")
        results = []
        for task in tasks:
            value = schedule.outcomes[task.task_id].value
            if not isinstance(value, RemoteTaskResult):
                raise RuntimeError("Worker returned an invalid remote Task result")
            metrics = RemoteTaskMetrics.model_validate(value.metrics)
            spill = SpillMetrics(
                spill_bytes=metrics.spill_bytes,
                spill_files=metrics.spill_files,
                spill_count=metrics.spill_count,
                peak_memory_bytes=metrics.peak_memory_bytes,
                external_sort_runs=metrics.external_sort_runs,
                hash_partitions=metrics.hash_partitions,
                sort_merge_fallbacks=metrics.sort_merge_fallbacks,
                sort_aggregate_runs=metrics.sort_aggregate_runs,
            )
            self._spill_metrics = self._spill_metrics.add(spill)
            self._task_spill_metrics[task.task_id] = spill
            results.append(value)
        return results

    def _command(
        self,
        query_id: str,
        name: str,
        operation: RemoteTaskOperation,
        payload: dict[str, object],
    ) -> RemoteTaskCommand:
        stage_id = self._stage_id(name)
        return RemoteTaskCommand(
            task_id=f"{stage_id}-task-00000",
            query_id=query_id,
            stage_id=stage_id,
            operation=operation,
            payload=payload,  # type: ignore[arg-type]
            output_root=_join_location(self._runtime_root, "results"),
        )

    def _stage_id(self, name: str) -> str:
        stage_id = f"remote-{self._next_stage:03d}-{name}"
        self._next_stage += 1
        return stage_id

    def _read_parquet(self, location: str) -> pa.Table:
        payload = self._stores.for_location(location).read_bytes(location)
        return pq.read_table(pa.BufferReader(payload))


def _serialize_plan(plan: PhysicalPlan) -> dict[str, object]:
    return SerializedPlan(
        payload=base64.b64encode(pickle.dumps(plan, protocol=5)).decode("ascii")
    ).model_dump(mode="json")


def _join_location(base: str, *parts: str) -> str:
    parsed = urlsplit(base)
    relative = PurePosixPath(*parts).as_posix()
    if parsed.scheme in {"file", "s3"}:
        path = f"{parsed.path.rstrip('/')}/{relative}"
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return str(Path(base).joinpath(*parts).resolve())


def _partitions(results: list[RemoteTaskResult]) -> tuple[_RemotePartition, ...]:
    partitions = []
    for ordinal, result in enumerate(results):
        if result.artifact is None:
            raise RuntimeError("Worker did not materialize a Task result")
        partitions.append(_RemotePartition(ordinal, result.artifact, result.worker_id))
    return tuple(partitions)


def _metric_int(metrics: Mapping[str, object], name: str) -> int:
    value = metrics.get(name, 0)
    return value if isinstance(value, int) else 0


def _metric_float(metrics: Mapping[str, object], name: str) -> float:
    value = metrics.get(name, 0.0)
    return float(value) if isinstance(value, int | float) else 0.0

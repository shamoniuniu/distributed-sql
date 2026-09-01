"""Partition-oriented execution over explicit in-process logical Workers."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import cast

import pyarrow as pa

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.common.protocol import DataType, Schema, SchemaField
from distributed_sql.data_source import DataSourceRegistry, FileScanTask, ScanRequest
from distributed_sql.planner.expressions import AggregateFunction, Expression, SQLValue
from distributed_sql.planner.logical import (
    Aggregate,
    Filter,
    GroupingSets,
    Join,
    Limit,
    Order,
    Project,
    Scan,
    Window,
)

from .engine import _expression_sources, _hash_join_keys, _runtime_names
from .memory import (
    DEFAULT_MEMORY_LIMIT_BYTES,
    MemoryAccount,
    SpillMetrics,
    default_temp_root,
)
from .operators import (
    BatchOperator,
    CancellationToken,
    ExecutionContext,
    FilterOperator,
    GroupingSetsOperator,
    HashAggregateOperator,
    HashJoinOperator,
    InMemorySorter,
    LimitOperator,
    OrderOperator,
    ProjectOperator,
    RecordBatchSource,
    SortAggregateOperator,
    Sorter,
    WindowOperator,
    arrow_schema_for_aggregate,
    arrow_schema_for_grouping_sets,
    arrow_schema_for_window,
)
from .physical import Exchange, PhysicalPlan, StageGraph, StagePlanner
from .runtime_filter import (
    RuntimeFilter,
    RuntimeFilterBinding,
    RuntimeFilterMetrics,
    apply_runtime_filters,
    runtime_filter_is_safe,
)
from .scheduler import (
    LogicalWorker,
    RetryPolicy,
    ScheduleResult,
    TaskRunner,
    TaskScheduler,
    WorkerLeaseRegistry,
)
from .shuffle import ShuffleManifest, ShuffleMetrics, ShuffleStore


@dataclass(frozen=True, slots=True)
class DataPartition:
    ordinal: int
    table: pa.Table
    worker_id: str


@dataclass(frozen=True, slots=True)
class DistributedResult:
    table: pa.Table
    partitions: tuple[DataPartition, ...]
    stage_graph: StageGraph
    schedules: tuple[ScheduleResult, ...]
    shuffle_metrics: ShuffleMetrics
    runtime_filter_metrics: RuntimeFilterMetrics
    spill_metrics: SpillMetrics
    task_spill_metrics: Mapping[str, SpillMetrics]

    @property
    def partition_owners(self) -> dict[int, str]:
        return {partition.ordinal: partition.worker_id for partition in self.partitions}


class DistributedExecutor:
    """Execute physical plans without pretending logical Workers are remote."""

    def __init__(
        self,
        tables: Mapping[str, CatalogTable],
        data_sources: DataSourceRegistry,
        workers: list[LogicalWorker],
        shuffle: ShuffleStore,
        *,
        registry: WorkerLeaseRegistry | None = None,
        retry_policy: RetryPolicy | None = None,
        sorter: Sorter | None = None,
        memory_limit_bytes: int = DEFAULT_MEMORY_LIMIT_BYTES,
        temp_root: Path | None = None,
    ) -> None:
        if memory_limit_bytes <= 0:
            raise ValueError("memory_limit_bytes must be positive")
        self._tables = {name.casefold(): table for name, table in tables.items()}
        self._data_sources = data_sources
        self._scheduler = TaskScheduler(
            workers,
            registry=registry,
            retry_policy=retry_policy,
        )
        self._shuffle = shuffle
        self._sorter = sorter or InMemorySorter()
        self._memory_limit_bytes = memory_limit_bytes
        self._temp_root = temp_root or default_temp_root()
        self._schedules: list[ScheduleResult] = []
        self._shuffle_metrics = ShuffleMetrics()
        self._runtime_filter_metrics = RuntimeFilterMetrics()
        self._spill_metrics = SpillMetrics()
        self._task_spill_metrics: dict[str, SpillMetrics] = {}
        self._query_memory: MemoryAccount | None = None
        self._next_stage = 0

    def cancel(self, query_id: str) -> None:
        self._scheduler.cancel(query_id)

    async def execute(self, query_id: str, plan: PhysicalPlan) -> DistributedResult:
        self._schedules.clear()
        self._shuffle_metrics = ShuffleMetrics()
        self._runtime_filter_metrics = RuntimeFilterMetrics()
        self._spill_metrics = SpillMetrics()
        self._task_spill_metrics = {}
        self._query_memory = MemoryAccount(query_id, self._memory_limit_bytes)
        self._next_stage = 0
        graph = StagePlanner(query_id).plan(plan)
        partitions = await self._execute_node(query_id, plan, ())
        ordered = tuple(sorted(partitions, key=lambda item: item.ordinal))
        table = _concat([partition.table for partition in ordered], plan.output_schema)
        self._spill_metrics.peak_memory_bytes = max(
            self._spill_metrics.peak_memory_bytes,
            self._query_memory.peak_bytes,
        )
        self._shuffle_metrics = self._shuffle_metrics.model_copy(
            update={"spill_bytes": self._spill_metrics.spill_bytes}
        )
        return DistributedResult(
            table,
            ordered,
            graph,
            tuple(self._schedules),
            self._shuffle_metrics,
            self._runtime_filter_metrics,
            self._spill_metrics,
            dict(self._task_spill_metrics),
        )

    async def _execute_node(
        self,
        query_id: str,
        plan: PhysicalPlan,
        runtime_filters: tuple[RuntimeFilterBinding, ...],
    ) -> tuple[DataPartition, ...]:
        if isinstance(plan, Exchange):
            source = await self._execute_node(query_id, plan.input, runtime_filters)
            return await self._exchange(query_id, plan, source)
        if isinstance(plan, Scan):
            return await self._scan(query_id, plan, runtime_filters)
        if isinstance(plan, Aggregate) and isinstance(plan.input, Exchange) and _can_partial(plan):
            return await self._partial_aggregate(query_id, plan, plan.input, runtime_filters)
        if isinstance(plan, Join):
            left_keys, right_keys = _hash_join_keys(plan)
            if runtime_filter_is_safe(plan.join_type, plan.build_side):
                build_plan = plan.left if plan.build_side == "left" else plan.right
                probe_plan = plan.right if plan.build_side == "left" else plan.left
                build_keys = left_keys if plan.build_side == "left" else right_keys
                probe_keys = right_keys if plan.build_side == "left" else left_keys
                build = await self._execute_node(query_id, build_plan, runtime_filters)
                runtime_filter = _distributed_runtime_filter(build, build_keys)
                binding = RuntimeFilterBinding(probe_keys, runtime_filter)
                probe = await self._execute_node(
                    query_id,
                    probe_plan,
                    (*runtime_filters, binding),
                )
                if plan.build_side == "left":
                    left, right = build, probe
                else:
                    left, right = probe, build
            else:
                left, right = await asyncio.gather(
                    self._execute_node(query_id, plan.left, runtime_filters),
                    self._execute_node(query_id, plan.right, runtime_filters),
                )
            count = max(len(left), len(right))
            if len(left) not in {1, count} or len(right) not in {1, count}:
                raise ValueError("Join inputs have incompatible partition counts")
            runners: list[TaskRunner] = []
            for ordinal in range(count):
                left_table = left[0].table if len(left) == 1 else left[ordinal].table
                right_table = right[0].table if len(right) == 1 else right[ordinal].table
                runners.append(self._join_runner(query_id, plan, left_table, right_table))
            return await self._run_partition_tasks(query_id, plan.node_id, runners)
        child = await self._execute_node(query_id, plan.children[0], runtime_filters)
        runners = [
            self._unary_runner(query_id, plan, partition.table)
            for partition in sorted(child, key=lambda item: item.ordinal)
        ]
        return await self._run_partition_tasks(query_id, plan.node_id, runners)

    async def _partial_aggregate(
        self,
        query_id: str,
        plan: Aggregate,
        exchange: Exchange,
        runtime_filters: tuple[RuntimeFilterBinding, ...],
    ) -> tuple[DataPartition, ...]:
        source = await self._execute_node(query_id, exchange.input, runtime_filters)
        if any(item.table.nbytes > self._memory_limit_bytes // 2 for item in source):
            shuffled = await self._exchange(query_id, exchange, source)
            return await self._run_partition_tasks(
                query_id,
                f"{plan.node_id}-spill-final",
                [self._unary_runner(query_id, plan, item.table) for item in shuffled],
            )
        partial = await self._run_partition_tasks(
            query_id,
            f"{plan.node_id}-partial",
            [_partial_aggregate_runner(plan, item.table) for item in source],
        )
        shuffled = await self._exchange(query_id, exchange, partial)
        return await self._run_partition_tasks(
            query_id,
            f"{plan.node_id}-final",
            [_final_aggregate_runner(plan, item.table) for item in shuffled],
        )

    async def _scan(
        self,
        query_id: str,
        plan: Scan,
        runtime_filters: tuple[RuntimeFilterBinding, ...],
    ) -> tuple[DataPartition, ...]:
        table = self._tables[plan.table_name.casefold()]
        source = self._data_sources.for_table(table)
        scan_filters = _scan_runtime_filters(plan.alias, runtime_filters)
        file_tasks = source.plan_scan(
            table,
            ScanRequest(projection=tuple(field.name for field in plan.output_schema.fields)),
        ).file_tasks
        runners: list[TaskRunner] = []
        for file_task in file_tasks:

            def run(
                _attempt_id: str,
                _cancellation: object,
                task: FileScanTask = file_task,
            ) -> tuple[pa.Table, RuntimeFilterMetrics]:
                request = ScanRequest(
                    projection=tuple(field.name for field in plan.output_schema.fields),
                    file_tasks=(task,),
                )
                batches: list[pa.RecordBatch] = []
                metrics = RuntimeFilterMetrics()
                for batch in source.scan(table, request):
                    metrics.input_batches += 1
                    metrics.input_rows += batch.num_rows
                    batch = batch.rename_columns(
                        [f"{plan.alias}.{name}" for name in batch.schema.names]
                    )
                    batch, applied = apply_runtime_filters(batch, scan_filters)
                    metrics.filters_applied += applied
                    if batch.num_rows:
                        metrics.output_batches += 1
                        metrics.output_rows += batch.num_rows
                        batches.append(batch)
                schema = pa.schema(
                    [
                        pa.field(
                            f"{plan.alias}.{field.name}",
                            batch_type,
                            nullable=field.nullable,
                        )
                        for field, batch_type in zip(
                            plan.output_schema.fields,
                            source.plan_scan(table, request).schema.types,
                            strict=True,
                        )
                    ]
                )
                scanned = (
                    pa.Table.from_batches(batches).cast(schema)
                    if batches
                    else pa.Table.from_batches([], schema=schema)
                )
                return scanned, metrics

            runners.append(run)
        values = await self._run_values(query_id, self._stage_id(plan.node_id), runners)
        partitions: list[DataPartition] = []
        for ordinal, worker_id, value in values:
            result_table, metrics = cast(tuple[pa.Table, RuntimeFilterMetrics], value)
            self._runtime_filter_metrics = self._runtime_filter_metrics.add(metrics)
            partitions.append(DataPartition(ordinal, result_table, worker_id))
        return tuple(partitions)

    async def _exchange(
        self,
        query_id: str,
        exchange: Exchange,
        source: tuple[DataPartition, ...],
    ) -> tuple[DataPartition, ...]:
        stage_id = self._stage_id(exchange.node_id)
        runners: list[TaskRunner] = []
        for source_partition in source:

            def write(
                attempt_id: str,
                _cancellation: object,
                partition: DataPartition = source_partition,
            ) -> ShuffleManifest:
                return self._shuffle.write(
                    query_id=query_id,
                    stage_id=stage_id,
                    task_id=f"{stage_id}-source-{partition.ordinal:05d}",
                    attempt_id=attempt_id,
                    table=partition.table,
                    partition_count=exchange.partition_count,
                    keys=exchange.keys,
                    broadcast=exchange.strategy.value == "broadcast",
                )

            runners.append(write)
        outcomes = await self._run_values(query_id, stage_id, runners)
        manifests = [cast(ShuffleManifest, value) for _, _, value in outcomes]
        for manifest in manifests:
            self._shuffle_metrics = self._shuffle_metrics.add(manifest.metrics)
        read_runners: list[TaskRunner] = []
        for ordinal in range(exchange.partition_count):

            def read(
                _attempt_id: str,
                _cancellation: object,
                target: int = ordinal,
            ) -> tuple[pa.Table, ShuffleMetrics]:
                return self._shuffle.read_partition(manifests, target)

            read_runners.append(read)
        values = await self._run_values(
            query_id,
            self._stage_id(f"{stage_id}-read"),
            read_runners,
        )
        partitions: list[DataPartition] = []
        for ordinal, worker_id, value in values:
            table, metrics = cast(tuple[pa.Table, ShuffleMetrics], value)
            self._shuffle_metrics = self._shuffle_metrics.add(metrics)
            partitions.append(DataPartition(ordinal, table, worker_id))
        return tuple(partitions)

    def _unary_runner(
        self,
        query_id: str,
        plan: PhysicalPlan,
        table: pa.Table,
    ) -> TaskRunner:
        def run(attempt_id: str, cancellation: object) -> _OperatorResult:
            context = self._task_context(query_id, attempt_id, cancellation)
            context.cancellation = cast(CancellationToken, cancellation)
            source = RecordBatchSource(
                f"{plan.node_id}-input",
                table.to_batches(max_chunksize=context.batch_size),
            )
            operator: BatchOperator
            if isinstance(plan, Project):
                from distributed_sql.data_source import schema_to_arrow

                operator = ProjectOperator(
                    plan.node_id, source, plan.expressions, schema_to_arrow(plan.output_schema)
                )
            elif isinstance(plan, Filter):
                operator = FilterOperator(plan.node_id, source, plan.predicate)
            elif isinstance(plan, Limit):
                operator = LimitOperator(plan.node_id, source, plan.count)
            elif isinstance(plan, Order):
                operator = OrderOperator(
                    plan.node_id,
                    source,
                    plan.order_by,
                    table.schema,
                    self._sorter,
                )
            elif isinstance(plan, Aggregate):
                if plan.group_by:
                    operator = SortAggregateOperator(
                        plan.node_id,
                        source,
                        plan.group_by,
                        plan.aggregates,
                        table.schema,
                        arrow_schema_for_aggregate(plan.group_by, plan.aggregates),
                    )
                else:
                    operator = HashAggregateOperator(
                        plan.node_id,
                        source,
                        plan.group_by,
                        plan.aggregates,
                        arrow_schema_for_aggregate(plan.group_by, plan.aggregates),
                    )
            elif isinstance(plan, GroupingSets):
                operator = GroupingSetsOperator(
                    plan.node_id,
                    source,
                    plan.grouping_sets,
                    plan.aggregates,
                    arrow_schema_for_grouping_sets(plan.grouping_sets, plan.aggregates),
                )
            elif isinstance(plan, Window):
                operator = WindowOperator(
                    plan.node_id,
                    source,
                    plan.expressions,
                    arrow_schema_for_window(table.schema, plan.expressions),
                    self._sorter,
                )
            else:
                raise ValueError(f"Unsupported distributed operator: {type(plan).__name__}")
            try:
                value = _operator_table(operator, context, plan.output_schema)
            finally:
                context.close()
            return _OperatorResult(
                attempt_id.rpartition("-attempt-")[0],
                value,
                context.spill_metrics,
            )

        return run

    def _join_runner(
        self,
        query_id: str,
        plan: Join,
        left: pa.Table,
        right: pa.Table,
    ) -> TaskRunner:
        def run(attempt_id: str, cancellation: object) -> _OperatorResult:
            from distributed_sql.data_source import schema_to_arrow

            context = self._task_context(query_id, attempt_id, cancellation)
            left_keys, right_keys = _hash_join_keys(plan)
            operator = HashJoinOperator(
                plan.node_id,
                RecordBatchSource("left-input", left.to_batches()),
                RecordBatchSource("right-input", right.to_batches()),
                left_keys,
                right_keys,
                plan.join_type,
                schema_to_arrow(plan.output_schema),
                _runtime_names(plan.left),
                _runtime_names(plan.right),
                build_side=plan.build_side,
            )
            try:
                value = _operator_table(operator, context, plan.output_schema)
            finally:
                context.close()
            return _OperatorResult(
                attempt_id.rpartition("-attempt-")[0],
                value,
                context.spill_metrics,
            )

        return run

    async def _run_partition_tasks(
        self,
        query_id: str,
        name: str,
        runners: list[TaskRunner],
    ) -> tuple[DataPartition, ...]:
        values = await self._run_values(query_id, self._stage_id(name), runners)
        partitions: list[DataPartition] = []
        for ordinal, worker_id, value in values:
            if isinstance(value, _OperatorResult):
                self._spill_metrics = self._spill_metrics.add(value.spill_metrics)
                self._task_spill_metrics[value.task_id] = value.spill_metrics
                table = value.table
            else:
                table = cast(pa.Table, value)
            partitions.append(DataPartition(ordinal, table, worker_id))
        return tuple(partitions)

    async def _run_values(
        self,
        query_id: str,
        stage_id: str,
        runners: list[TaskRunner],
    ) -> list[tuple[int, str, object]]:
        from distributed_sql.common.protocol import Partition, PlanNode, PlanNodeType, Stage, Task

        stage = Stage(
            stage_id=stage_id,
            query_id=query_id,
            plan=PlanNode(node_id=stage_id, node_type=PlanNodeType.OUTPUT),
            partition_count=len(runners),
        )
        tasks = tuple(
            Task(
                task_id=f"{stage_id}-task-{ordinal:05d}",
                query_id=query_id,
                stage_id=stage_id,
                partition=Partition(
                    partition_id=f"{stage_id}-partition-{ordinal:05d}",
                    ordinal=ordinal,
                    location="",
                ),
            )
            for ordinal in range(len(runners))
        )
        graph = StageGraph(stage_id, (stage,), tasks)
        result = await self._scheduler.run(
            graph,
            {task.task_id: runner for task, runner in zip(tasks, runners, strict=True)},
        )
        self._schedules.append(result)
        return [
            (
                task.partition.ordinal,
                result.outcomes[task.task_id].worker_id,
                result.outcomes[task.task_id].value,
            )
            for task in tasks
        ]

    def _stage_id(self, name: str) -> str:
        value = f"runtime-{self._next_stage:03d}-{name}"
        self._next_stage += 1
        return value

    def _task_context(
        self,
        query_id: str,
        task_id: str,
        cancellation: object,
    ) -> ExecutionContext:
        assert self._query_memory is not None
        return ExecutionContext(
            cancellation=cast(CancellationToken, cancellation),
            memory_limit_bytes=self._memory_limit_bytes,
            temp_root=self._temp_root,
            query_id=query_id,
            task_id=task_id,
            query_memory=self._query_memory,
        )


@dataclass(frozen=True, slots=True)
class _OperatorResult:
    task_id: str
    table: pa.Table
    spill_metrics: SpillMetrics


def _operator_table(
    operator: BatchOperator,
    context: ExecutionContext,
    schema: object,
) -> pa.Table:
    from distributed_sql.common.protocol import Schema
    from distributed_sql.data_source import schema_to_arrow

    assert isinstance(schema, Schema)
    batches = list(operator.execute(context))
    if batches:
        return pa.Table.from_batches(batches)
    return pa.Table.from_batches([], schema=schema_to_arrow(schema))


def _concat(tables: list[pa.Table], schema: object) -> pa.Table:
    from distributed_sql.common.protocol import Schema
    from distributed_sql.data_source import schema_to_arrow

    assert isinstance(schema, Schema)
    if tables:
        return pa.concat_tables(tables)
    return pa.Table.from_batches([], schema_to_arrow(schema))


def _can_partial(plan: Aggregate) -> bool:
    return all(
        isinstance(item.expression, AggregateFunction)
        and not item.expression.distinct
        and item.expression.name in {"count", "sum", "avg", "min", "max"}
        for item in plan.aggregates
    )


def _partial_aggregate_runner(plan: Aggregate, table: pa.Table) -> TaskRunner:
    def run(_attempt_id: str, cancellation: CancellationToken) -> pa.Table:
        groups: dict[tuple[SQLValue, ...], list[dict[str, object]]] = {}
        if not plan.group_by:
            groups[()] = _new_partial_states(plan)
        for raw_row in table.to_pylist():
            cancellation.check()
            row = cast(Mapping[str, SQLValue], raw_row)
            key = tuple(expression.evaluate(row) for expression in plan.group_by)
            states = groups.setdefault(key, _new_partial_states(plan))
            for item, state in zip(plan.aggregates, states, strict=True):
                function = cast(AggregateFunction, item.expression)
                value = function.arguments[0].evaluate(row) if function.arguments else 1
                if value is None:
                    continue
                state["count"] = cast(int, state["count"]) + 1
                if function.name in {"sum", "avg"}:
                    state["value"] = _add_numbers(state["value"], value)
                elif function.name == "min":
                    current = state["value"]
                    if current is None or value < current:  # type: ignore[operator]
                        state["value"] = value
                elif function.name == "max":
                    current = state["value"]
                    if current is None or value > current:  # type: ignore[operator]
                        state["value"] = value
        rows: list[dict[str, object]] = []
        for key, states in groups.items():
            output_row: dict[str, object] = {
                expression.sql(): value
                for expression, value in zip(plan.group_by, key, strict=True)
            }
            for index, state in enumerate(states):
                output_row[f"__partial_{index}_count"] = state["count"]
                output_row[f"__partial_{index}_value"] = state["value"]
            rows.append(output_row)
        return pa.Table.from_pylist(rows, schema=_partial_schema(plan))

    return run


def _final_aggregate_runner(plan: Aggregate, table: pa.Table) -> TaskRunner:
    def run(_attempt_id: str, cancellation: CancellationToken) -> pa.Table:
        groups: dict[tuple[SQLValue, ...], list[dict[str, object]]] = {}
        if not plan.group_by:
            groups[()] = _new_partial_states(plan)
        for raw_row in table.to_pylist():
            cancellation.check()
            row = cast(Mapping[str, SQLValue], raw_row)
            key = tuple(row.get(expression.sql()) for expression in plan.group_by)
            states = groups.setdefault(key, _new_partial_states(plan))
            for index, state in enumerate(states):
                count = cast(int, row[f"__partial_{index}_count"])
                partial_value = row[f"__partial_{index}_value"]
                state["count"] = cast(int, state["count"]) + count
                function = cast(AggregateFunction, plan.aggregates[index].expression)
                if function.name in {"sum", "avg"}:
                    state["value"] = _add_numbers(state["value"], partial_value)
                elif function.name == "min" and partial_value is not None:
                    current = state["value"]
                    if current is None or partial_value < current:  # type: ignore[operator]
                        state["value"] = partial_value
                elif function.name == "max" and partial_value is not None:
                    current = state["value"]
                    if current is None or partial_value > current:  # type: ignore[operator]
                        state["value"] = partial_value
        rows: list[dict[str, object]] = []
        for key, states in groups.items():
            output_row: dict[str, object] = {
                expression.sql(): value
                for expression, value in zip(plan.group_by, key, strict=True)
            }
            for item, state in zip(plan.aggregates, states, strict=True):
                function = cast(AggregateFunction, item.expression)
                count = cast(int, state["count"])
                if function.name == "count":
                    output_value: object = count
                elif function.name == "avg":
                    total = cast(int | float | Decimal, state["value"])
                    output_value = total / count if count else None
                else:
                    output_value = state["value"] if count else None
                output_row[function.sql()] = output_value
            rows.append(output_row)
        return pa.Table.from_pylist(
            rows,
            schema=arrow_schema_for_aggregate(plan.group_by, plan.aggregates),
        )

    return run


def _new_partial_states(plan: Aggregate) -> list[dict[str, object]]:
    return [{"count": 0, "value": None} for _ in plan.aggregates]


def _partial_schema(plan: Aggregate) -> pa.Schema:
    from distributed_sql.data_source import schema_to_arrow

    fields = [
        SchemaField(
            name=expression.sql(),
            data_type=expression.type_info.data_type,
            nullable=expression.type_info.nullable,
        )
        for expression in plan.group_by
    ]
    for index, item in enumerate(plan.aggregates):
        function = cast(AggregateFunction, item.expression)
        fields.extend(
            [
                SchemaField(
                    name=f"__partial_{index}_count",
                    data_type=DataType.INT64,
                    nullable=False,
                ),
                SchemaField(
                    name=f"__partial_{index}_value",
                    data_type=(
                        DataType.INT64 if function.name == "count" else function.type_info.data_type
                    ),
                ),
            ]
        )
    return schema_to_arrow(Schema(fields=fields))


def _add_numbers(current: object, value: object) -> int | float | Decimal:
    if current is None:
        return cast(int | float | Decimal, value)
    left = cast(int | float | Decimal, current)
    right = cast(int | float | Decimal, value)
    if isinstance(left, Decimal) or isinstance(right, Decimal):
        return Decimal(str(left)) + Decimal(str(right))
    return left + right


def _distributed_runtime_filter(
    partitions: tuple[DataPartition, ...],
    expressions: tuple[Expression, ...],
) -> RuntimeFilter:
    row_count = sum(partition.table.num_rows for partition in partitions)
    runtime_filter = RuntimeFilter.create(len(expressions), max(row_count, 1))
    for partition in partitions:
        for raw_row in partition.table.to_pylist():
            row = cast(Mapping[str, SQLValue], raw_row)
            runtime_filter.add(tuple(expression.evaluate(row) for expression in expressions))
    # This round trip is the Worker control-plane boundary.
    return RuntimeFilter.from_bytes(runtime_filter.to_bytes())


def _scan_runtime_filters(
    alias: str,
    bindings: tuple[RuntimeFilterBinding, ...],
) -> tuple[RuntimeFilterBinding, ...]:
    source = alias.casefold()
    return tuple(
        binding
        for binding in bindings
        if all(_expression_sources(expression) == {source} for expression in binding.expressions)
    )

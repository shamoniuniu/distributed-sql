"""Single-worker PyArrow batch operators."""

from __future__ import annotations

import heapq
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from functools import cmp_to_key
from itertools import chain
from pathlib import Path
from threading import Event
from time import perf_counter
from typing import Protocol, cast

import pyarrow as pa
import pyarrow.parquet as pq

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.common.protocol import Schema, SchemaField
from distributed_sql.data_source import DataSource, ScanRequest, schema_to_arrow
from distributed_sql.planner.expressions import (
    AggregateFunction,
    Expression,
    ScalarFunction,
    SortExpression,
    SQLValue,
    WindowFrame,
    WindowFunction,
)
from distributed_sql.planner.logical import (
    AggregateExpression,
    NamedExpression,
    NamedWindowExpression,
)

from .memory import (
    DEFAULT_MEMORY_LIMIT_BYTES,
    MemoryAccount,
    SpillMetrics,
    TempFileManager,
    default_temp_root,
    estimate_row_size,
)
from .runtime_filter import (
    RuntimeFilter,
    RuntimeFilterBinding,
    RuntimeFilterChannel,
    apply_runtime_filters,
)

type BatchIterator = Iterator[pa.RecordBatch]
type MutableRow = dict[str, SQLValue]


class ExecutionCancelled(RuntimeError):
    """Raised cooperatively when an execution context is cancelled."""


class CancellationToken:
    def __init__(self) -> None:
        self._event = Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def check(self) -> None:
        if self.cancelled:
            raise ExecutionCancelled("Execution was cancelled.")


@dataclass(slots=True)
class OperatorMetrics:
    input_rows: int = 0
    input_batches: int = 0
    output_rows: int = 0
    output_batches: int = 0
    elapsed_seconds: float = 0.0
    runtime_filters_applied: int = 0
    runtime_filter_rows_filtered: int = 0
    spill_bytes: int = 0
    spill_files: int = 0
    peak_memory_bytes: int = 0


@dataclass(slots=True)
class ExecutionContext:
    batch_size: int = 65_536
    cancellation: CancellationToken = field(default_factory=CancellationToken)
    metrics: dict[str, OperatorMetrics] = field(default_factory=dict)
    memory_limit_bytes: int = DEFAULT_MEMORY_LIMIT_BYTES
    temp_root: Path = field(default_factory=default_temp_root)
    query_id: str = "local-query"
    task_id: str = "local-task"
    query_memory: MemoryAccount | None = None
    memory_account: MemoryAccount = field(init=False)
    temp_files: TempFileManager = field(init=False)
    spill_metrics: SpillMetrics = field(default_factory=SpillMetrics)

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.memory_limit_bytes <= 0:
            raise ValueError("memory_limit_bytes must be positive")
        query_memory = self.query_memory or MemoryAccount(
            self.query_id,
            self.memory_limit_bytes,
        )
        self.query_memory = query_memory
        self.memory_account = query_memory.child(
            self.task_id,
            self.memory_limit_bytes,
        )
        self.temp_files = TempFileManager(self.temp_root, self.query_id, self.task_id)

    def metric_for(self, operator_id: str) -> OperatorMetrics:
        return self.metrics.setdefault(operator_id, OperatorMetrics())

    def check_cancelled(self) -> None:
        self.cancellation.check()

    def close(self) -> None:
        if self.memory_account.current_bytes:
            self.memory_account.release(self.memory_account.current_bytes)
        self.spill_metrics.peak_memory_bytes = max(
            self.spill_metrics.peak_memory_bytes,
            self.memory_account.peak_bytes,
        )
        self.temp_files.cleanup()


class BatchOperator(ABC):
    def __init__(self, operator_id: str) -> None:
        self.operator_id = operator_id

    @abstractmethod
    def execute(self, context: ExecutionContext) -> BatchIterator:
        raise NotImplementedError


class Sorter(Protocol):
    """Replaceable row sorting boundary used by order and window operators."""

    def sort(
        self,
        rows: Sequence[MutableRow],
        order_by: Sequence[SortExpression],
        cancellation: CancellationToken,
    ) -> list[MutableRow]: ...


class InMemorySorter:
    def sort(
        self,
        rows: Sequence[MutableRow],
        order_by: Sequence[SortExpression],
        cancellation: CancellationToken,
    ) -> list[MutableRow]:
        def compare(left: MutableRow, right: MutableRow) -> int:
            cancellation.check()
            for item in order_by:
                result = _compare_values(
                    item.expression.evaluate(left),
                    item.expression.evaluate(right),
                    item,
                )
                if result:
                    return result
            return 0

        return sorted(rows, key=cmp_to_key(compare))


def _compare_values(
    left: SQLValue,
    right: SQLValue,
    order: SortExpression,
) -> int:
    if left is None and right is None:
        return 0
    if left is None:
        return -1 if order.nulls_first else 1
    if right is None:
        return 1 if order.nulls_first else -1
    result = (left > right) - (left < right)  # type: ignore[operator]
    return result if order.ascending else -result


class RecordBatchSource(BatchOperator):
    """In-memory leaf used by tests and local operator composition."""

    def __init__(self, operator_id: str, batches: Iterable[pa.RecordBatch]) -> None:
        super().__init__(operator_id)
        self._batches = batches

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        try:
            for batch in self._batches:
                context.check_cancelled()
                metric.output_batches += 1
                metric.output_rows += batch.num_rows
                yield batch
        finally:
            metric.elapsed_seconds += perf_counter() - started


class ScanOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        source: DataSource,
        table: CatalogTable,
        request: ScanRequest,
        *,
        alias: str | None = None,
        runtime_filters: Sequence[RuntimeFilterBinding] = (),
    ) -> None:
        super().__init__(operator_id)
        self._source = source
        self._table = table
        self._request = request
        self._alias = alias
        self._runtime_filters = tuple(runtime_filters)

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        try:
            for batch in self._source.scan(self._table, self._request):
                context.check_cancelled()
                input_rows = batch.num_rows
                if self._alias:
                    batch = batch.rename_columns(
                        [f"{self._alias}.{name}" for name in batch.schema.names]
                    )
                batch, applied = apply_runtime_filters(batch, self._runtime_filters)
                metric.input_batches += 1
                metric.input_rows += input_rows
                metric.runtime_filters_applied += applied
                metric.runtime_filter_rows_filtered += input_rows - batch.num_rows
                if not batch.num_rows:
                    continue
                metric.output_batches += 1
                metric.output_rows += batch.num_rows
                yield batch
        finally:
            metric.elapsed_seconds += perf_counter() - started


class ProjectOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        input_operator: BatchOperator,
        expressions: Sequence[NamedExpression],
        output_schema: pa.Schema,
    ) -> None:
        super().__init__(operator_id)
        self._input = input_operator
        self._expressions = expressions
        self._output_schema = output_schema

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        try:
            for batch in self._input.execute(context):
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                rows = [
                    {
                        item.name: item.expression.evaluate(cast(Mapping[str, SQLValue], row))
                        for item in self._expressions
                    }
                    for row in batch.to_pylist()
                ]
                for output in _batches_from_rows(rows, self._output_schema, context.batch_size):
                    metric.output_batches += 1
                    metric.output_rows += output.num_rows
                    yield output
        finally:
            metric.elapsed_seconds += perf_counter() - started


class FilterOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        input_operator: BatchOperator,
        predicate: Expression,
    ) -> None:
        super().__init__(operator_id)
        self._input = input_operator
        self._predicate = predicate

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        try:
            for batch in self._input.execute(context):
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                rows = [
                    row
                    for row in batch.to_pylist()
                    if self._predicate.evaluate(cast(Mapping[str, SQLValue], row)) is True
                ]
                for output in _batches_from_rows(rows, batch.schema, context.batch_size):
                    metric.output_batches += 1
                    metric.output_rows += output.num_rows
                    yield output
        finally:
            metric.elapsed_seconds += perf_counter() - started


class LimitOperator(BatchOperator):
    def __init__(self, operator_id: str, input_operator: BatchOperator, count: int) -> None:
        super().__init__(operator_id)
        if count < 0:
            raise ValueError("limit count cannot be negative")
        self._input = input_operator
        self._count = count

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        remaining = self._count
        try:
            if remaining == 0:
                return
            for batch in self._input.execute(context):
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                output = batch.slice(0, remaining)
                if output.num_rows:
                    metric.output_batches += 1
                    metric.output_rows += output.num_rows
                    yield output
                remaining -= output.num_rows
                if remaining == 0:
                    break
        finally:
            metric.elapsed_seconds += perf_counter() - started


class OrderOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        input_operator: BatchOperator,
        order_by: Sequence[SortExpression],
        output_schema: pa.Schema,
        sorter: Sorter,
    ) -> None:
        super().__init__(operator_id)
        self._input = input_operator
        self._order_by = order_by
        self._output_schema = output_schema
        self._sorter = sorter

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        try:
            rows = _external_sorted_rows(
                self._input.execute(context),
                self._output_schema,
                self._order_by,
                self._sorter,
                context,
                metric,
                spill_kind="external_sort",
            )
            output_rows: list[MutableRow] = []
            for row in rows:
                output_rows.append(row)
                if len(output_rows) < context.batch_size:
                    continue
                batch = pa.RecordBatch.from_pylist(output_rows, schema=self._output_schema)
                metric.output_batches += 1
                metric.output_rows += batch.num_rows
                yield batch
                output_rows = []
            for batch in _batches_from_rows(
                output_rows,
                self._output_schema,
                context.batch_size,
            ):
                metric.output_batches += 1
                metric.output_rows += batch.num_rows
                yield batch
        finally:
            metric.elapsed_seconds += perf_counter() - started


def _external_sorted_rows(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    order_by: Sequence[SortExpression],
    sorter: Sorter,
    context: ExecutionContext,
    metric: OperatorMetrics,
    *,
    spill_kind: str,
) -> Iterator[MutableRow]:
    rows: list[MutableRow] = []
    reserved = 0
    runs: list[Path] = []

    def spill_run() -> None:
        nonlocal rows, reserved
        if not rows:
            return
        ordered = sorter.sort(rows, order_by, context.cancellation)
        path = context.temp_files.write_table(
            pa.Table.from_pylist(ordered, schema=schema),
            spill_kind,
        )
        size = path.stat().st_size
        runs.append(path)
        context.spill_metrics.spill_bytes += size
        context.spill_metrics.spill_files += 1
        context.spill_metrics.spill_count += 1
        if spill_kind == "external_sort":
            context.spill_metrics.external_sort_runs += 1
        else:
            context.spill_metrics.sort_aggregate_runs += 1
        metric.spill_bytes += size
        metric.spill_files += 1
        if reserved:
            context.memory_account.release(reserved)
        rows = []
        reserved = 0

    try:
        for batch in batches:
            context.check_cancelled()
            metric.input_batches += 1
            metric.input_rows += batch.num_rows
            for raw_row in batch.to_pylist():
                context.check_cancelled()
                row = cast(MutableRow, raw_row)
                charge = estimate_row_size(cast(dict[str, object], row))
                if not context.memory_account.try_reserve(charge):
                    spill_run()
                    if not context.memory_account.try_reserve(charge):
                        rows.append(row)
                        spill_run()
                        continue
                reserved += charge
                rows.append(row)
        if not runs:
            yield from sorter.sort(rows, order_by, context.cancellation)
            return
        spill_run()

        def compare(left: MutableRow, right: MutableRow) -> int:
            context.check_cancelled()
            for item in order_by:
                result = _compare_values(
                    item.expression.evaluate(left),
                    item.expression.evaluate(right),
                    item,
                )
                if result:
                    return result
            return 0

        key = cmp_to_key(compare)
        iterators = [_iter_parquet_rows(path, context) for path in runs]
        yield from heapq.merge(*iterators, key=key)
    finally:
        if reserved:
            context.memory_account.release(reserved)
        metric.peak_memory_bytes = max(
            metric.peak_memory_bytes,
            context.memory_account.peak_bytes,
        )
        context.spill_metrics.peak_memory_bytes = max(
            context.spill_metrics.peak_memory_bytes,
            context.memory_account.peak_bytes,
        )


def _iter_parquet_rows(
    path: Path,
    context: ExecutionContext,
) -> Iterator[MutableRow]:
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=1):
        context.check_cancelled()
        yield cast(MutableRow, batch.to_pylist()[0])


@dataclass(slots=True)
class _AggregateState:
    function: AggregateFunction
    count: int = 0
    total: int | float | Decimal = 0
    value: SQLValue = None
    distinct_values: set[SQLValue] = field(default_factory=set)

    def add(self, row: Mapping[str, SQLValue]) -> None:
        argument = self.function.arguments[0].evaluate(row) if self.function.arguments else 1
        if argument is None:
            return
        if self.function.distinct:
            if argument in self.distinct_values:
                return
            self.distinct_values.add(argument)
        self.count += 1
        if self.function.name in {"sum", "avg"}:
            number = cast(int | float | Decimal, argument)
            if isinstance(self.total, Decimal) or isinstance(number, Decimal):
                self.total = Decimal(str(self.total)) + Decimal(str(number))
            else:
                self.total += number
        elif self.function.name == "min":
            if self.value is None or argument < self.value:  # type: ignore[operator]
                self.value = argument
        elif self.function.name == "max" and (
            self.value is None or argument > self.value  # type: ignore[operator]
        ):
            self.value = argument

    def finish(self) -> SQLValue:
        if self.function.name == "count":
            return self.count
        if self.function.name == "sum":
            return self.total if self.count else None
        if self.function.name == "avg":
            return self.total / self.count if self.count else None
        return self.value


class HashAggregateOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        input_operator: BatchOperator,
        group_by: Sequence[Expression],
        aggregates: Sequence[AggregateExpression],
        output_schema: pa.Schema,
    ) -> None:
        super().__init__(operator_id)
        self._input = input_operator
        self._group_by = group_by
        self._aggregates = aggregates
        self._output_schema = output_schema

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        groups: dict[tuple[SQLValue, ...], list[_AggregateState]] = {}
        try:
            if not self._group_by:
                groups[()] = self._new_states()
            for batch in self._input.execute(context):
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                for raw_row in batch.to_pylist():
                    context.check_cancelled()
                    row = cast(Mapping[str, SQLValue], raw_row)
                    key = tuple(expression.evaluate(row) for expression in self._group_by)
                    states = groups.setdefault(key, self._new_states())
                    for state in states:
                        state.add(row)
            rows = []
            for key, states in groups.items():
                row = {
                    expression.sql(): value
                    for expression, value in zip(self._group_by, key, strict=True)
                }
                row.update(
                    {
                        item.expression.sql(): state.finish()
                        for item, state in zip(self._aggregates, states, strict=True)
                    }
                )
                rows.append(row)
            for batch in _batches_from_rows(rows, self._output_schema, context.batch_size):
                metric.output_batches += 1
                metric.output_rows += batch.num_rows
                yield batch
        finally:
            metric.elapsed_seconds += perf_counter() - started

    def _new_states(self) -> list[_AggregateState]:
        states: list[_AggregateState] = []
        for item in self._aggregates:
            if not isinstance(item.expression, AggregateFunction):
                raise ValueError("Hash aggregate requires aggregate function expressions.")
            states.append(_AggregateState(item.expression))
        return states


class SortAggregateOperator(HashAggregateOperator):
    """Streaming aggregate over externally sorted grouping keys."""

    def __init__(
        self,
        operator_id: str,
        input_operator: BatchOperator,
        group_by: Sequence[Expression],
        aggregates: Sequence[AggregateExpression],
        input_schema: pa.Schema,
        output_schema: pa.Schema,
    ) -> None:
        super().__init__(
            operator_id,
            input_operator,
            group_by,
            aggregates,
            output_schema,
        )
        self._input_schema = input_schema

    def execute(self, context: ExecutionContext) -> BatchIterator:
        if not self._group_by:
            yield from super().execute(context)
            return
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        order_by = tuple(SortExpression(expression) for expression in self._group_by)
        current_key: tuple[SQLValue, ...] | None = None
        states: list[_AggregateState] | None = None
        output_rows: list[MutableRow] = []
        try:
            rows = _external_sorted_rows(
                self._input.execute(context),
                self._input_schema,
                order_by,
                InMemorySorter(),
                context,
                metric,
                spill_kind="sort_aggregate",
            )
            for row in rows:
                context.check_cancelled()
                key = tuple(expression.evaluate(row) for expression in self._group_by)
                if states is not None and key != current_key:
                    output_rows.append(self._finish_group(current_key, states))
                    if len(output_rows) >= context.batch_size:
                        yield from self._emit_groups(output_rows, metric, context)
                        output_rows = []
                    states = None
                if states is None:
                    current_key = key
                    states = self._new_states()
                for state in states:
                    state.add(row)
            if states is not None:
                output_rows.append(self._finish_group(current_key, states))
            yield from self._emit_groups(output_rows, metric, context)
        finally:
            metric.elapsed_seconds += perf_counter() - started

    def _finish_group(
        self,
        key: tuple[SQLValue, ...] | None,
        states: list[_AggregateState],
    ) -> MutableRow:
        assert key is not None
        row: MutableRow = {
            expression.sql(): value for expression, value in zip(self._group_by, key, strict=True)
        }
        row.update(
            {
                item.expression.sql(): state.finish()
                for item, state in zip(self._aggregates, states, strict=True)
            }
        )
        return row

    def _emit_groups(
        self,
        rows: list[MutableRow],
        metric: OperatorMetrics,
        context: ExecutionContext,
    ) -> BatchIterator:
        for batch in _batches_from_rows(rows, self._output_schema, context.batch_size):
            metric.output_batches += 1
            metric.output_rows += batch.num_rows
            yield batch


class GroupingSetsOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        input_operator: BatchOperator,
        grouping_sets: Sequence[Sequence[Expression]],
        aggregates: Sequence[AggregateExpression],
        output_schema: pa.Schema,
    ) -> None:
        super().__init__(operator_id)
        self._input = input_operator
        self._grouping_sets = grouping_sets
        self._aggregates = aggregates
        self._output_schema = output_schema

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        input_rows: list[MutableRow] = []
        try:
            for batch in self._input.execute(context):
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                input_rows.extend(cast(list[MutableRow], batch.to_pylist()))
            rows: list[MutableRow] = []
            grouping_columns = _unique_expressions(self._grouping_sets)
            for grouping_set in self._grouping_sets:
                groups: dict[tuple[SQLValue, ...], list[_AggregateState]] = {}
                if not grouping_set:
                    groups[()] = self._new_states()
                for row in input_rows:
                    context.check_cancelled()
                    key = tuple(expression.evaluate(row) for expression in grouping_set)
                    states = groups.setdefault(key, self._new_states())
                    for state in states:
                        state.add(row)
                for key, states in groups.items():
                    output: MutableRow = {expression.sql(): None for expression in grouping_columns}
                    output.update(
                        {
                            expression.sql(): value
                            for expression, value in zip(grouping_set, key, strict=True)
                        }
                    )
                    output.update(
                        {
                            item.expression.sql(): state.finish()
                            for item, state in zip(self._aggregates, states, strict=True)
                        }
                    )
                    rows.append(output)
            for batch in _batches_from_rows(rows, self._output_schema, context.batch_size):
                metric.output_batches += 1
                metric.output_rows += batch.num_rows
                yield batch
        finally:
            metric.elapsed_seconds += perf_counter() - started

    def _new_states(self) -> list[_AggregateState]:
        return [
            _AggregateState(cast(AggregateFunction, item.expression)) for item in self._aggregates
        ]


class WindowOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        input_operator: BatchOperator,
        expressions: Sequence[NamedWindowExpression],
        output_schema: pa.Schema,
        sorter: Sorter,
    ) -> None:
        super().__init__(operator_id)
        self._input = input_operator
        self._expressions = expressions
        self._output_schema = output_schema
        self._sorter = sorter

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        rows: list[MutableRow] = []
        try:
            for batch in self._input.execute(context):
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                rows.extend(cast(list[MutableRow], batch.to_pylist()))
            for item in self._expressions:
                self._evaluate(item.expression, rows, context)
            for output in _batches_from_rows(rows, self._output_schema, context.batch_size):
                metric.output_batches += 1
                metric.output_rows += output.num_rows
                yield output
        finally:
            metric.elapsed_seconds += perf_counter() - started

    def _evaluate(
        self,
        window: WindowFunction,
        rows: list[MutableRow],
        context: ExecutionContext,
    ) -> None:
        partitions: dict[tuple[SQLValue, ...], list[MutableRow]] = {}
        for row in rows:
            key = tuple(expression.evaluate(row) for expression in window.partition_by)
            partitions.setdefault(key, []).append(row)
        for partition in partitions.values():
            ordered = (
                self._sorter.sort(partition, window.order_by, context.cancellation)
                if window.order_by
                else list(partition)
            )
            peer_keys = [
                tuple(item.expression.evaluate(row) for item in window.order_by) for row in ordered
            ]
            dense_rank = 0
            previous_peer: tuple[SQLValue, ...] | None = None
            for index, row in enumerate(ordered):
                context.check_cancelled()
                function = window.function
                if isinstance(function, ScalarFunction):
                    if function.name == "row_number":
                        value: SQLValue = index + 1
                    else:
                        peer = peer_keys[index]
                        if index == 0 or peer != previous_peer:
                            dense_rank += 1
                        if function.name == "dense_rank":
                            value = dense_rank
                        elif peer != previous_peer:
                            value = index + 1
                        else:
                            value = ordered[index - 1][window.sql()]
                        previous_peer = peer
                else:
                    start, end = _window_bounds(window.frame, index, len(ordered), peer_keys)
                    state = _AggregateState(cast(AggregateFunction, function))
                    for frame_row in ordered[start:end]:
                        state.add(frame_row)
                    value = state.finish()
                row[window.sql()] = value


def _window_bounds(
    frame: WindowFrame | None,
    index: int,
    size: int,
    peer_keys: Sequence[tuple[SQLValue, ...]],
) -> tuple[int, int]:
    if frame is None:
        if not peer_keys or not peer_keys[0]:
            return 0, size
        end = index + 1
        while end < size and peer_keys[end] == peer_keys[index]:
            end += 1
        return 0, end
    return (
        _frame_boundary(frame.start, index, size, is_end=False),
        _frame_boundary(frame.end, index, size, is_end=True),
    )


def _frame_boundary(boundary: str, index: int, size: int, *, is_end: bool) -> int:
    if boundary == "UNBOUNDED PRECEDING":
        return 0
    if boundary == "UNBOUNDED FOLLOWING":
        return size
    if boundary == "CURRENT ROW":
        return index + int(is_end)
    amount_text, direction = boundary.split()
    amount = int(amount_text)
    position = index - amount if direction == "PRECEDING" else index + amount
    if is_end:
        position += 1
    return min(max(position, 0), size)


def _unique_expressions(
    grouping_sets: Sequence[Sequence[Expression]],
) -> tuple[Expression, ...]:
    result: dict[str, Expression] = {}
    for grouping_set in grouping_sets:
        for expression in grouping_set:
            result.setdefault(expression.sql(), expression)
    return tuple(result.values())


class HashJoinOperator(BatchOperator):
    def __init__(
        self,
        operator_id: str,
        left: BatchOperator,
        right: BatchOperator,
        left_keys: Sequence[Expression],
        right_keys: Sequence[Expression],
        join_type: str,
        output_schema: pa.Schema,
        left_columns: Sequence[str],
        right_columns: Sequence[str],
        *,
        build_side: str = "right",
        runtime_filter_channel: RuntimeFilterChannel | None = None,
    ) -> None:
        super().__init__(operator_id)
        if join_type not in {"inner", "left", "right", "full"}:
            raise ValueError(f"Unsupported hash join type: {join_type}")
        if not left_keys or len(left_keys) != len(right_keys):
            raise ValueError("Hash join requires matching non-empty equi-key lists")
        if build_side not in {"left", "right"}:
            raise ValueError("Hash join build_side must be left or right")
        self._left = left
        self._right = right
        self._left_keys = left_keys
        self._right_keys = right_keys
        self._join_type = join_type
        self._output_schema = output_schema
        self._left_columns = list(left_columns)
        self._right_columns = list(right_columns)
        self._build_side = build_side
        self._runtime_filter_channel = runtime_filter_channel

    def execute(self, context: ExecutionContext) -> BatchIterator:
        metric = context.metric_for(self.operator_id)
        started = perf_counter()
        build_is_left = self._build_side == "left"
        build_operator = self._left if build_is_left else self._right
        probe_operator = self._right if build_is_left else self._left
        build_keys = self._left_keys if build_is_left else self._right_keys
        probe_keys = self._right_keys if build_is_left else self._left_keys
        build_columns = self._left_columns if build_is_left else self._right_columns
        probe_columns = self._right_columns if build_is_left else self._left_columns
        build_rows: list[MutableRow] = []
        hash_table: dict[tuple[SQLValue, ...], list[int]] = {}
        matched_build: set[int] = set()
        output_rows: list[MutableRow] = []
        probe_names: list[str] = []
        reserved = 0
        try:
            build_batches = iter(build_operator.execute(context))
            for batch in build_batches:
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                raw_rows = batch.to_pylist()
                for row_index, raw_row in enumerate(raw_rows):
                    row = cast(MutableRow, raw_row)
                    charge = estimate_row_size(cast(dict[str, object], row))
                    if not context.memory_account.try_reserve(charge):
                        remaining = chain(
                            build_rows,
                            (cast(MutableRow, item) for item in raw_rows[row_index:]),
                            _rows_from_batches(build_batches, context, metric),
                        )
                        if reserved:
                            context.memory_account.release(reserved)
                            reserved = 0
                        yield from self._execute_partitioned(
                            remaining,
                            batch.schema,
                            probe_operator,
                            build_keys,
                            probe_keys,
                            build_is_left,
                            metric,
                            context,
                        )
                        return
                    reserved += charge
                    index = len(build_rows)
                    build_rows.append(row)
                    key = _join_key(build_keys, row)
                    if key is not None:
                        hash_table.setdefault(key, []).append(index)
            if self._runtime_filter_channel is not None:
                runtime_filter = RuntimeFilter.create(
                    len(build_keys),
                    max(len(build_rows), 1),
                )
                for row in build_rows:
                    key = _join_key(build_keys, row)
                    if key is not None:
                        runtime_filter.add(key)
                self._runtime_filter_channel.publish(runtime_filter)
            build_names = list(build_rows[0]) if build_rows else build_columns
            for batch in probe_operator.execute(context):
                context.check_cancelled()
                metric.input_batches += 1
                metric.input_rows += batch.num_rows
                probe_names = batch.schema.names
                for raw_row in batch.to_pylist():
                    context.check_cancelled()
                    probe_row = cast(MutableRow, raw_row)
                    key = _join_key(probe_keys, probe_row)
                    matches = [] if key is None else hash_table.get(key, [])
                    if matches:
                        for build_index in matches:
                            matched_build.add(build_index)
                            build_row = build_rows[build_index]
                            output_rows.append(
                                build_row | probe_row if build_is_left else probe_row | build_row
                            )
                    elif _side_is_preserved(self._join_type, "right" if build_is_left else "left"):
                        null_build = dict.fromkeys(build_names)
                        output_rows.append(
                            null_build | probe_row if build_is_left else probe_row | null_build
                        )
                    if len(output_rows) >= context.batch_size:
                        yield from self._emit(output_rows, metric, context)
                        output_rows = []
            if not probe_names:
                probe_names = probe_columns
            if _side_is_preserved(self._join_type, self._build_side):
                null_probe = dict.fromkeys(probe_names)
                for index, build_row in enumerate(build_rows):
                    if index not in matched_build:
                        output_rows.append(
                            build_row | null_probe if build_is_left else null_probe | build_row
                        )
                        if len(output_rows) >= context.batch_size:
                            yield from self._emit(output_rows, metric, context)
                            output_rows = []
            yield from self._emit(output_rows, metric, context)
        finally:
            if reserved:
                context.memory_account.release(reserved)
            metric.peak_memory_bytes = max(
                metric.peak_memory_bytes,
                context.memory_account.peak_bytes,
            )
            context.spill_metrics.peak_memory_bytes = max(
                context.spill_metrics.peak_memory_bytes,
                context.memory_account.peak_bytes,
            )
            metric.elapsed_seconds += perf_counter() - started

    def _execute_partitioned(
        self,
        build_rows: Iterable[MutableRow],
        build_schema: pa.Schema,
        probe_operator: BatchOperator,
        build_keys: Sequence[Expression],
        probe_keys: Sequence[Expression],
        build_is_left: bool,
        metric: OperatorMetrics,
        context: ExecutionContext,
    ) -> BatchIterator:
        partition_count = 8
        build_files = _partition_rows(
            build_rows,
            build_schema,
            build_keys,
            partition_count,
            "join-build",
            context,
            metric,
        )
        probe_batches = iter(probe_operator.execute(context))
        try:
            first_probe = next(probe_batches)
        except StopIteration:
            first_probe = None
        if first_probe is None:
            probe_schema = pa.schema(
                [
                    self._output_schema.field(name)
                    for name in (self._right_columns if build_is_left else self._left_columns)
                ]
            )
            probe_rows: Iterable[MutableRow] = ()
        else:
            probe_schema = first_probe.schema
            probe_rows = _rows_from_batches(
                chain((first_probe,), probe_batches),
                context,
                metric,
            )
        probe_files = _partition_rows(
            probe_rows,
            probe_schema,
            probe_keys,
            partition_count,
            "join-probe",
            context,
            metric,
        )
        context.spill_metrics.hash_partitions += sum(
            bool(build_files[index] or probe_files[index]) for index in range(partition_count)
        )
        for partition in range(partition_count):
            yield from self._join_partition(
                build_files[partition],
                probe_files[partition],
                build_schema,
                probe_schema,
                build_keys,
                probe_keys,
                build_is_left,
                metric,
                context,
            )

    def _join_partition(
        self,
        build_files: list[Path],
        probe_files: list[Path],
        build_schema: pa.Schema,
        probe_schema: pa.Schema,
        build_keys: Sequence[Expression],
        probe_keys: Sequence[Expression],
        build_is_left: bool,
        metric: OperatorMetrics,
        context: ExecutionContext,
    ) -> BatchIterator:
        build_rows: list[MutableRow] = []
        reserved = 0
        fits = True
        for row in _rows_from_files(build_files, context):
            charge = estimate_row_size(cast(dict[str, object], row))
            if not context.memory_account.try_reserve(charge):
                fits = False
                break
            reserved += charge
            build_rows.append(row)
        if not fits:
            if reserved:
                context.memory_account.release(reserved)
            context.spill_metrics.sort_merge_fallbacks += 1
            yield from self._sort_merge_partition(
                build_files,
                probe_files,
                build_schema,
                probe_schema,
                build_keys,
                probe_keys,
                build_is_left,
                metric,
                context,
            )
            return
        try:
            yield from self._hash_partition_rows(
                build_rows,
                _rows_from_files(probe_files, context),
                build_keys,
                probe_keys,
                build_is_left,
                metric,
                context,
            )
        finally:
            if reserved:
                context.memory_account.release(reserved)

    def _hash_partition_rows(
        self,
        build_rows: list[MutableRow],
        probe_rows: Iterable[MutableRow],
        build_keys: Sequence[Expression],
        probe_keys: Sequence[Expression],
        build_is_left: bool,
        metric: OperatorMetrics,
        context: ExecutionContext,
    ) -> BatchIterator:
        table: dict[tuple[SQLValue, ...], list[int]] = {}
        matched: set[int] = set()
        for index, row in enumerate(build_rows):
            key = _join_key(build_keys, row)
            if key is not None:
                table.setdefault(key, []).append(index)
        output: list[MutableRow] = []
        for probe_row in probe_rows:
            key = _join_key(probe_keys, probe_row)
            matches = [] if key is None else table.get(key, [])
            if matches:
                for index in matches:
                    matched.add(index)
                    output.append(
                        build_rows[index] | probe_row
                        if build_is_left
                        else probe_row | build_rows[index]
                    )
            elif _side_is_preserved(
                self._join_type,
                "right" if build_is_left else "left",
            ):
                null_build = dict.fromkeys(
                    self._left_columns if build_is_left else self._right_columns
                )
                output.append(null_build | probe_row if build_is_left else probe_row | null_build)
            if len(output) >= context.batch_size:
                yield from self._emit(output, metric, context)
                output = []
        if _side_is_preserved(self._join_type, self._build_side):
            null_probe = dict.fromkeys(self._right_columns if build_is_left else self._left_columns)
            for index, row in enumerate(build_rows):
                if index not in matched:
                    output.append(row | null_probe if build_is_left else null_probe | row)
        yield from self._emit(output, metric, context)

    def _sort_merge_partition(
        self,
        build_files: list[Path],
        probe_files: list[Path],
        build_schema: pa.Schema,
        probe_schema: pa.Schema,
        build_keys: Sequence[Expression],
        probe_keys: Sequence[Expression],
        build_is_left: bool,
        metric: OperatorMetrics,
        context: ExecutionContext,
    ) -> BatchIterator:
        sort_metric = OperatorMetrics()
        build_order = tuple(SortExpression(item) for item in build_keys)
        probe_order = tuple(SortExpression(item) for item in probe_keys)
        sorted_build = _external_sorted_rows(
            _batches_from_files(build_files, context),
            build_schema,
            build_order,
            InMemorySorter(),
            context,
            sort_metric,
            spill_kind="external_sort",
        )
        sorted_probe = _external_sorted_rows(
            _batches_from_files(probe_files, context),
            probe_schema,
            probe_order,
            InMemorySorter(),
            context,
            sort_metric,
            spill_kind="external_sort",
        )
        yield from self._merge_join_rows(
            sorted_build,
            sorted_probe,
            build_keys,
            probe_keys,
            build_is_left,
            metric,
            context,
        )

    def _merge_join_rows(
        self,
        build_rows: Iterator[MutableRow],
        probe_rows: Iterator[MutableRow],
        build_keys: Sequence[Expression],
        probe_keys: Sequence[Expression],
        build_is_left: bool,
        metric: OperatorMetrics,
        context: ExecutionContext,
    ) -> BatchIterator:
        build_row = next(build_rows, None)
        probe_row = next(probe_rows, None)
        output: list[MutableRow] = []

        def append_unmatched(row: MutableRow, *, build: bool) -> None:
            side = self._build_side if build else ("right" if build_is_left else "left")
            if not _side_is_preserved(self._join_type, side):
                return
            other_columns = (
                self._right_columns
                if (build and build_is_left) or (not build and not build_is_left)
                else self._left_columns
            )
            nulls = dict.fromkeys(other_columns)
            if build:
                output.append(row | nulls if build_is_left else nulls | row)
            else:
                output.append(nulls | row if build_is_left else row | nulls)

        while build_row is not None and probe_row is not None:
            context.check_cancelled()
            build_key = _join_key(build_keys, build_row)
            probe_key = _join_key(probe_keys, probe_row)
            if build_key is None:
                append_unmatched(build_row, build=True)
                build_row = next(build_rows, None)
            elif probe_key is None:
                append_unmatched(probe_row, build=False)
                probe_row = next(probe_rows, None)
            elif build_key == probe_key:
                build_group, build_row = _take_key_group(
                    build_row,
                    build_rows,
                    build_keys,
                    build_key,
                )
                probe_group, probe_row = _take_key_group(
                    probe_row,
                    probe_rows,
                    probe_keys,
                    probe_key,
                )
                for left in build_group:
                    for right in probe_group:
                        output.append(left | right if build_is_left else right | left)
            elif _compare_join_keys(build_key, probe_key) < 0:
                append_unmatched(build_row, build=True)
                build_row = next(build_rows, None)
            else:
                append_unmatched(probe_row, build=False)
                probe_row = next(probe_rows, None)
            if len(output) >= context.batch_size:
                yield from self._emit(output, metric, context)
                output = []
        while build_row is not None:
            append_unmatched(build_row, build=True)
            build_row = next(build_rows, None)
        while probe_row is not None:
            append_unmatched(probe_row, build=False)
            probe_row = next(probe_rows, None)
        yield from self._emit(output, metric, context)

    def _emit(
        self,
        rows: list[MutableRow],
        metric: OperatorMetrics,
        context: ExecutionContext,
    ) -> BatchIterator:
        for batch in _batches_from_rows(rows, self._output_schema, context.batch_size):
            metric.output_batches += 1
            metric.output_rows += batch.num_rows
            yield batch


def _rows_from_batches(
    batches: Iterable[pa.RecordBatch],
    context: ExecutionContext,
    metric: OperatorMetrics,
) -> Iterator[MutableRow]:
    for batch in batches:
        context.check_cancelled()
        metric.input_batches += 1
        metric.input_rows += batch.num_rows
        for row in batch.to_pylist():
            yield cast(MutableRow, row)


def _partition_rows(
    rows: Iterable[MutableRow],
    schema: pa.Schema,
    keys: Sequence[Expression],
    partition_count: int,
    prefix: str,
    context: ExecutionContext,
    metric: OperatorMetrics,
) -> list[list[Path]]:
    files: list[list[Path]] = [[] for _ in range(partition_count)]
    buffers: list[list[MutableRow]] = [[] for _ in range(partition_count)]
    reserved = 0

    def flush() -> None:
        nonlocal reserved
        for partition, buffered in enumerate(buffers):
            if not buffered:
                continue
            path = context.temp_files.write_table(
                pa.Table.from_pylist(buffered, schema=schema),
                f"{prefix}-{partition:05d}",
            )
            size = path.stat().st_size
            files[partition].append(path)
            context.spill_metrics.spill_bytes += size
            context.spill_metrics.spill_files += 1
            context.spill_metrics.spill_count += 1
            metric.spill_bytes += size
            metric.spill_files += 1
            buffered.clear()
        if reserved:
            context.memory_account.release(reserved)
            reserved = 0

    try:
        for row in rows:
            context.check_cancelled()
            charge = estimate_row_size(cast(dict[str, object], row))
            if not context.memory_account.try_reserve(charge):
                flush()
                if not context.memory_account.try_reserve(charge):
                    partition = _join_partition_number(keys, row, partition_count)
                    buffers[partition].append(row)
                    flush()
                    continue
            reserved += charge
            partition = _join_partition_number(keys, row, partition_count)
            buffers[partition].append(row)
        flush()
        return files
    finally:
        if reserved:
            context.memory_account.release(reserved)


def _join_partition_number(
    keys: Sequence[Expression],
    row: Mapping[str, SQLValue],
    partition_count: int,
) -> int:
    key = _join_key(keys, row)
    return 0 if key is None else hash(key) % partition_count


def _batches_from_files(
    files: Iterable[Path],
    context: ExecutionContext,
) -> Iterator[pa.RecordBatch]:
    for path in files:
        for batch in pq.ParquetFile(path).iter_batches(
            batch_size=max(1, min(context.batch_size, 1_024))
        ):
            context.check_cancelled()
            yield batch


def _rows_from_files(
    files: Iterable[Path],
    context: ExecutionContext,
) -> Iterator[MutableRow]:
    for batch in _batches_from_files(files, context):
        for row in batch.to_pylist():
            context.check_cancelled()
            yield cast(MutableRow, row)


def _take_key_group(
    first: MutableRow,
    rows: Iterator[MutableRow],
    expressions: Sequence[Expression],
    key: tuple[SQLValue, ...],
) -> tuple[list[MutableRow], MutableRow | None]:
    group = [first]
    for row in rows:
        if _join_key(expressions, row) != key:
            return group, row
        group.append(row)
    return group, None


def _compare_join_keys(
    left: tuple[SQLValue, ...],
    right: tuple[SQLValue, ...],
) -> int:
    for left_value, right_value in zip(left, right, strict=True):
        result = (left_value > right_value) - (left_value < right_value)  # type: ignore[operator]
        if result:
            return result
    return 0


def _side_is_preserved(join_type: str, side: str) -> bool:
    return join_type == "full" or join_type == side


def _join_key(
    expressions: Sequence[Expression],
    row: Mapping[str, SQLValue],
) -> tuple[SQLValue, ...] | None:
    values = tuple(expression.evaluate(row) for expression in expressions)
    return None if any(value is None for value in values) else values


def _batches_from_rows(
    rows: Sequence[Mapping[str, object]],
    schema: pa.Schema,
    batch_size: int,
) -> Iterator[pa.RecordBatch]:
    for offset in range(0, len(rows), batch_size):
        yield pa.RecordBatch.from_pylist(rows[offset : offset + batch_size], schema=schema)


def arrow_schema_for_aggregate(
    group_by: Sequence[Expression],
    aggregates: Sequence[AggregateExpression],
) -> pa.Schema:
    fields = [
        SchemaField(
            name=expression.sql(),
            data_type=expression.type_info.data_type,
            nullable=expression.type_info.nullable,
        )
        for expression in group_by
    ]
    fields.extend(
        SchemaField(
            name=item.expression.sql(),
            data_type=item.expression.type_info.data_type,
            nullable=item.expression.type_info.nullable,
        )
        for item in aggregates
    )
    return schema_to_arrow(Schema(fields=fields))


def arrow_schema_for_grouping_sets(
    grouping_sets: Sequence[Sequence[Expression]],
    aggregates: Sequence[AggregateExpression],
) -> pa.Schema:
    fields = [
        SchemaField(
            name=expression.sql(),
            data_type=expression.type_info.data_type,
            nullable=True,
        )
        for expression in _unique_expressions(grouping_sets)
    ]
    fields.extend(
        SchemaField(
            name=item.expression.sql(),
            data_type=item.expression.type_info.data_type,
            nullable=item.expression.type_info.nullable,
        )
        for item in aggregates
    )
    return schema_to_arrow(Schema(fields=fields))


def arrow_schema_for_window(
    input_schema: pa.Schema,
    expressions: Sequence[NamedWindowExpression],
) -> pa.Schema:
    fields = list(input_schema)
    existing = set(input_schema.names)
    for item in expressions:
        name = item.expression.sql()
        if name not in existing:
            data_type = schema_to_arrow(
                Schema(
                    fields=[
                        SchemaField(
                            name=name,
                            data_type=item.expression.type_info.data_type,
                            nullable=item.expression.type_info.nullable,
                        )
                    ]
                )
            ).field(0)
            fields.append(data_type)
            existing.add(name)
    return pa.schema(fields)

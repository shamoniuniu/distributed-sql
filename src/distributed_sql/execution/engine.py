"""Compilation and execution of supported logical plans on one worker."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import fields, is_dataclass

import pyarrow as pa

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.data_source import DataSourceRegistry, ScanRequest, schema_to_arrow
from distributed_sql.planner.expressions import Binary, BinaryOperator, Column, Expression
from distributed_sql.planner.logical import (
    Aggregate,
    Filter,
    GroupingSets,
    Join,
    Limit,
    LogicalPlan,
    Order,
    Project,
    Scan,
    Window,
)

from .operators import (
    BatchOperator,
    ExecutionContext,
    FilterOperator,
    GroupingSetsOperator,
    HashAggregateOperator,
    HashJoinOperator,
    InMemorySorter,
    LimitOperator,
    OrderOperator,
    ProjectOperator,
    ScanOperator,
    SortAggregateOperator,
    Sorter,
    WindowOperator,
    arrow_schema_for_aggregate,
    arrow_schema_for_grouping_sets,
    arrow_schema_for_window,
)
from .runtime_filter import (
    RuntimeFilterBinding,
    RuntimeFilterChannel,
    runtime_filter_is_safe,
)


class LocalExecutor:
    """Build and run the Task 5 subset of a bound logical plan."""

    def __init__(
        self,
        tables: Mapping[str, CatalogTable],
        data_sources: DataSourceRegistry,
        *,
        sorter: Sorter | None = None,
    ) -> None:
        self._tables = {name.casefold(): table for name, table in tables.items()}
        self._data_sources = data_sources
        self._sorter = sorter or InMemorySorter()

    def build(self, plan: LogicalPlan, *, batch_size: int = 65_536) -> BatchOperator:
        return self._build(plan, batch_size, ())

    def _build(
        self,
        plan: LogicalPlan,
        batch_size: int,
        runtime_filters: tuple[RuntimeFilterBinding, ...],
    ) -> BatchOperator:
        if isinstance(plan, Scan):
            table = self._table(plan.table_name)
            return ScanOperator(
                plan.node_id,
                self._data_sources.for_table(table),
                table,
                ScanRequest(
                    projection=tuple(field.name for field in plan.output_schema.fields),
                    batch_size=batch_size,
                ),
                alias=plan.alias,
                runtime_filters=_scan_runtime_filters(plan.alias, runtime_filters),
            )
        if isinstance(plan, Project):
            return ProjectOperator(
                plan.node_id,
                self._build(plan.input, batch_size, runtime_filters),
                plan.expressions,
                schema_to_arrow(plan.output_schema),
            )
        if isinstance(plan, Filter):
            return FilterOperator(
                plan.node_id,
                self._build(plan.input, batch_size, runtime_filters),
                plan.predicate,
            )
        if isinstance(plan, Limit):
            return LimitOperator(
                plan.node_id,
                self._build(plan.input, batch_size, runtime_filters),
                plan.count,
            )
        if isinstance(plan, Order):
            return OrderOperator(
                plan.node_id,
                self._build(plan.input, batch_size, runtime_filters),
                plan.order_by,
                _runtime_schema(plan.input),
                self._sorter,
            )
        if isinstance(plan, Aggregate):
            aggregate_type = SortAggregateOperator if plan.group_by else HashAggregateOperator
            arguments = (
                (arrow_schema_for_aggregate(plan.group_by, plan.aggregates),)
                if aggregate_type is HashAggregateOperator
                else (
                    _runtime_schema(plan.input),
                    arrow_schema_for_aggregate(plan.group_by, plan.aggregates),
                )
            )
            return aggregate_type(
                plan.node_id,
                self._build(plan.input, batch_size, runtime_filters),
                plan.group_by,
                plan.aggregates,
                *arguments,
            )
        if isinstance(plan, GroupingSets):
            return GroupingSetsOperator(
                plan.node_id,
                self._build(plan.input, batch_size, runtime_filters),
                plan.grouping_sets,
                plan.aggregates,
                arrow_schema_for_grouping_sets(plan.grouping_sets, plan.aggregates),
            )
        if isinstance(plan, Window):
            return WindowOperator(
                plan.node_id,
                self._build(plan.input, batch_size, runtime_filters),
                plan.expressions,
                arrow_schema_for_window(_runtime_schema(plan.input), plan.expressions),
                self._sorter,
            )
        if isinstance(plan, Join):
            left_keys, right_keys = _hash_join_keys(plan)
            channel = (
                RuntimeFilterChannel()
                if runtime_filter_is_safe(plan.join_type, plan.build_side)
                else None
            )
            left_filters = runtime_filters
            right_filters = runtime_filters
            if channel is not None:
                if plan.build_side == "left":
                    right_filters += (RuntimeFilterBinding(right_keys, channel),)
                else:
                    left_filters += (RuntimeFilterBinding(left_keys, channel),)
            return HashJoinOperator(
                plan.node_id,
                self._build(plan.left, batch_size, left_filters),
                self._build(plan.right, batch_size, right_filters),
                left_keys,
                right_keys,
                plan.join_type,
                schema_to_arrow(plan.output_schema),
                _runtime_names(plan.left),
                _runtime_names(plan.right),
                build_side=plan.build_side,
                runtime_filter_channel=channel,
            )
        raise ValueError(f"Logical operator {type(plan).__name__} is not supported.")

    def execute(
        self,
        plan: LogicalPlan,
        context: ExecutionContext | None = None,
    ) -> Iterator[pa.RecordBatch]:
        execution_context = context or ExecutionContext()
        try:
            yield from self.build(
                plan,
                batch_size=execution_context.batch_size,
            ).execute(execution_context)
        finally:
            execution_context.close()

    def execute_table(
        self,
        plan: LogicalPlan,
        context: ExecutionContext | None = None,
    ) -> pa.Table:
        batches = list(self.execute(plan, context))
        if batches:
            return pa.Table.from_batches(batches)
        return pa.Table.from_batches([], schema=schema_to_arrow(plan.output_schema))

    def _table(self, name: str) -> CatalogTable:
        try:
            return self._tables[name.casefold()]
        except KeyError as exc:
            raise ValueError(f"No execution table is registered for {name!r}.") from exc


def _hash_join_keys(plan: Join) -> tuple[tuple[Expression, ...], tuple[Expression, ...]]:
    left_sources = _plan_sources(plan.left)
    right_sources = _plan_sources(plan.right)
    left_keys: list[Expression] = []
    right_keys: list[Expression] = []
    for equality in _split_equalities(plan.condition):
        left_expression = equality.left
        right_expression = equality.right
        first_sources = _expression_sources(left_expression)
        second_sources = _expression_sources(right_expression)
        if first_sources and first_sources <= left_sources and second_sources <= right_sources:
            left_keys.append(left_expression)
            right_keys.append(right_expression)
        elif first_sources and first_sources <= right_sources and second_sources <= left_sources:
            left_keys.append(right_expression)
            right_keys.append(left_expression)
        else:
            raise ValueError("Hash join keys must reference one input side each.")
    return tuple(left_keys), tuple(right_keys)


def _split_equalities(expression: Expression) -> tuple[Binary, ...]:
    if isinstance(expression, Binary) and expression.operator is BinaryOperator.AND:
        return _split_equalities(expression.left) + _split_equalities(expression.right)
    if isinstance(expression, Binary) and expression.operator is BinaryOperator.EQUAL:
        return (expression,)
    raise ValueError("Task 5 Hash Join supports conjunctions of equality predicates only.")


def _expression_sources(expression: Expression) -> set[str]:
    if isinstance(expression, Column):
        return {expression.source.casefold()}
    result: set[str] = set()
    if is_dataclass(expression):
        for item in fields(expression):
            value = getattr(expression, item.name)
            if isinstance(value, Expression):
                result.update(_expression_sources(value))
            elif isinstance(value, tuple):
                for child in value:
                    if isinstance(child, Expression):
                        result.update(_expression_sources(child))
    return result


def _plan_sources(plan: LogicalPlan) -> set[str]:
    if isinstance(plan, Scan):
        return {plan.alias.casefold()}
    result: set[str] = set()
    for child in plan.children:
        result.update(_plan_sources(child))
    return result


def _runtime_names(plan: LogicalPlan) -> list[str]:
    if isinstance(plan, Scan):
        return [f"{plan.alias}.{field.name}" for field in plan.output_schema.fields]
    if isinstance(plan, Filter | Limit | Order):
        return _runtime_names(plan.input)
    if isinstance(plan, Project):
        return [item.name for item in plan.expressions]
    if isinstance(plan, Aggregate):
        return [
            *(item.sql() for item in plan.group_by),
            *(item.expression.sql() for item in plan.aggregates),
        ]
    if isinstance(plan, GroupingSets):
        names = {
            expression.sql() for grouping_set in plan.grouping_sets for expression in grouping_set
        }
        return [*names, *(item.expression.sql() for item in plan.aggregates)]
    if isinstance(plan, Window):
        return [
            *_runtime_names(plan.input),
            *(item.expression.sql() for item in plan.expressions),
        ]
    return [field.name for field in plan.output_schema.fields]


def _runtime_schema(plan: LogicalPlan) -> pa.Schema:
    if isinstance(plan, Scan):
        schema = schema_to_arrow(plan.output_schema)
        return pa.schema(
            [
                pa.field(
                    name,
                    field.type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                for name, field in zip(_runtime_names(plan), schema, strict=True)
            ],
            metadata=schema.metadata,
        )
    if isinstance(plan, Filter | Limit | Order):
        return _runtime_schema(plan.input)
    if isinstance(plan, Aggregate):
        return arrow_schema_for_aggregate(plan.group_by, plan.aggregates)
    if isinstance(plan, GroupingSets):
        return arrow_schema_for_grouping_sets(plan.grouping_sets, plan.aggregates)
    if isinstance(plan, Window):
        return arrow_schema_for_window(_runtime_schema(plan.input), plan.expressions)
    return schema_to_arrow(plan.output_schema)


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

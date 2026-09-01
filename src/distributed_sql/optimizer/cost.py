"""Cardinality, data distribution, and resource cost estimation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from enum import StrEnum
from math import log2, prod
from typing import Literal as TypeLiteral

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.common.protocol import DataType, PartitionStrategy, Schema
from distributed_sql.planner.expressions import (
    Between,
    Binary,
    BinaryOperator,
    Cast,
    Column,
    Expression,
    InList,
    IsNull,
    Like,
)
from distributed_sql.planner.expressions import (
    Literal as ExpressionLiteral,
)
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

from .utils import expression_columns, plan_sources

DEFAULT_ROW_COUNT = 1_000_000.0
DEFAULT_NDV = 100_000.0
DEFAULT_NULL_FRACTION = 0.1


class JoinStrategy(StrEnum):
    REUSE = "reuse"
    BROADCAST = "broadcast"
    REPARTITION_LEFT = "repartition_left"
    REPARTITION_RIGHT = "repartition_right"
    REPARTITION_BOTH = "repartition_both"


@dataclass(frozen=True, slots=True)
class Distribution:
    strategy: PartitionStrategy = PartitionStrategy.UNKNOWN
    keys: tuple[str, ...] = ()
    partition_count: int = 1


@dataclass(frozen=True, slots=True)
class ColumnEstimate:
    null_fraction: float
    distinct_count: float
    min_value: object | None
    max_value: object | None
    average_size_bytes: float
    sources: frozenset[str]


@dataclass(frozen=True, slots=True)
class Cost:
    cpu: float = 0.0
    network: float = 0.0
    memory: float = 0.0
    disk: float = 0.0

    def __add__(self, other: Cost) -> Cost:
        return Cost(
            self.cpu + other.cpu,
            self.network + other.network,
            self.memory + other.memory,
            self.disk + other.disk,
        )

    @property
    def total(self) -> float:
        # Network and disk are deliberately more expensive than CPU; memory is a
        # peak-pressure signal and therefore receives a smaller additive weight.
        return self.cpu + self.network * 4.0 + self.memory * 0.05 + self.disk * 8.0


@dataclass(frozen=True, slots=True)
class PlanEstimate:
    row_count: float
    size_bytes: float
    columns: dict[str, ColumnEstimate]
    cost: Cost
    sources: frozenset[str]
    distribution: Distribution = dataclass_field(default_factory=Distribution)


@dataclass(frozen=True, slots=True)
class JoinDecision:
    node_id: str
    build_side: TypeLiteral["left", "right"]
    strategy: JoinStrategy
    left_keys: tuple[str, ...]
    right_keys: tuple[str, ...]
    estimated_rows: float
    estimated_bytes: float
    cost: Cost
    reason: str


class CostModel:
    """Estimate plans from immutable Catalog metadata with explicit fallbacks."""

    def __init__(
        self,
        catalog: Mapping[str, CatalogTable],
        *,
        worker_count: int = 2,
        memory_budget_bytes: int = 64 * 1024 * 1024,
        broadcast_threshold_bytes: int = 10 * 1024 * 1024,
    ) -> None:
        if worker_count < 1:
            raise ValueError("worker_count must be positive")
        if memory_budget_bytes < 1:
            raise ValueError("memory_budget_bytes must be positive")
        self.catalog = {name.casefold(): table for name, table in catalog.items()}
        self.worker_count = worker_count
        self.memory_budget_bytes = float(memory_budget_bytes)
        self.broadcast_threshold_bytes = float(broadcast_threshold_bytes)

    def estimate(self, plan: LogicalPlan) -> PlanEstimate:
        if isinstance(plan, Scan):
            return self._scan(plan)
        if isinstance(plan, Filter):
            return self._filter(plan)
        if isinstance(plan, Project):
            return self._project(plan)
        if isinstance(plan, Aggregate):
            return self._aggregate(plan)
        if isinstance(plan, GroupingSets):
            return self._grouping_sets(plan)
        if isinstance(plan, Join):
            return self.estimate_join(plan)[0]
        if isinstance(plan, Limit):
            child = self.estimate(plan.input)
            rows = min(child.row_count, float(plan.count))
            return self._unary(child, rows, cpu=rows * 0.05)
        if isinstance(plan, Order):
            child = self.estimate(plan.input)
            working = child.size_bytes
            spill = max(working - self.memory_budget_bytes, 0.0)
            return self._unary(
                child,
                child.row_count,
                cpu=child.row_count * max(log2(max(child.row_count, 1.0)), 1.0),
                memory=min(working, self.memory_budget_bytes),
                disk=spill * 2.0,
            )
        if isinstance(plan, Window):
            child = self.estimate(plan.input)
            return self._unary(
                child,
                child.row_count,
                cpu=child.row_count,
                memory=min(child.size_bytes, self.memory_budget_bytes),
            )
        raise TypeError(f"Unsupported logical plan: {type(plan).__name__}")

    def estimate_join(self, plan: Join) -> tuple[PlanEstimate, JoinDecision]:
        left = self.estimate(plan.left)
        right = self.estimate(plan.right)
        left_keys, right_keys = _join_keys(plan)
        rows = self._join_rows(plan, left, right, left_keys, right_keys)
        width = _row_width(plan.output_schema)
        size = rows * width
        build_side: TypeLiteral["left", "right"] = (
            "left" if left.size_bytes <= right.size_bytes else "right"
        )
        strategy, network, reason = self._join_strategy(
            left,
            right,
            left_keys,
            right_keys,
            build_side,
        )
        build_bytes = left.size_bytes if build_side == "left" else right.size_bytes
        spill = max(build_bytes - self.memory_budget_bytes, 0.0)
        operation = Cost(
            cpu=left.row_count + right.row_count + rows,
            network=network,
            memory=min(build_bytes, self.memory_budget_bytes),
            disk=spill * 2.0,
        )
        cost = left.cost + right.cost + operation
        distribution = _join_distribution(
            strategy,
            left,
            right,
            left_keys,
            right_keys,
            self.worker_count,
        )
        estimate = PlanEstimate(
            row_count=rows,
            size_bytes=size,
            columns={**left.columns, **right.columns},
            cost=cost,
            sources=left.sources | right.sources | frozenset({"derived:join"}),
            distribution=distribution,
        )
        decision = JoinDecision(
            node_id=plan.node_id,
            build_side=build_side,
            strategy=strategy,
            left_keys=left_keys,
            right_keys=right_keys,
            estimated_rows=rows,
            estimated_bytes=size,
            cost=operation,
            reason=reason,
        )
        return estimate, decision

    def _scan(self, plan: Scan) -> PlanEstimate:
        table = self.catalog.get(plan.table_name.casefold())
        if table is None:
            raise ValueError(f"No Catalog table is registered for {plan.table_name!r}.")
        statistics = table.statistics
        sources: set[str] = set()
        if statistics is not None:
            sources.add(f"statistics_source:{statistics.source}")
        raw_columns = (
            {name.casefold(): value for name, value in statistics.columns.items()}
            if statistics is not None
            else {}
        )
        if statistics is not None and statistics.row_count is not None:
            rows = float(statistics.row_count)
            sources.add(f"catalog:{plan.table_name}.row_count")
        elif table.partitions and all(item.row_count is not None for item in table.partitions):
            rows = float(sum(item.row_count or 0 for item in table.partitions))
            sources.add(f"partitions:{plan.table_name}.row_count")
        else:
            rows = DEFAULT_ROW_COUNT
            sources.add(f"default:{plan.table_name}.row_count={int(DEFAULT_ROW_COUNT)}")

        columns: dict[str, ColumnEstimate] = {}
        for field in plan.output_schema.fields:
            raw = raw_columns.get(field.name.casefold())
            column_sources: set[str] = set()
            if raw is not None and raw.null_count is not None:
                null_fraction = min(float(raw.null_count) / max(rows, 1.0), 1.0)
                column_sources.add(f"catalog:{plan.table_name}.{field.name}.null_count")
            else:
                null_fraction = DEFAULT_NULL_FRACTION if field.nullable else 0.0
                column_sources.add(f"default:{plan.table_name}.{field.name}.null_fraction")
            if raw is not None and raw.distinct_count is not None:
                ndv = min(float(raw.distinct_count), max(rows - rows * null_fraction, 0.0))
                column_sources.add(f"catalog:{plan.table_name}.{field.name}.ndv")
            else:
                ndv = max(rows * (1.0 - null_fraction), 0.0)
                column_sources.add(f"default:{plan.table_name}.{field.name}.ndv")
            average_size = (
                raw.average_size_bytes
                if raw is not None and raw.average_size_bytes is not None
                else _type_width(field.data_type)
            )
            if raw is None or raw.average_size_bytes is None:
                column_sources.add(f"default:{plan.table_name}.{field.name}.width")
            columns[f"{plan.alias}.{field.name}".casefold()] = ColumnEstimate(
                null_fraction,
                ndv,
                raw.min_value if raw is not None else None,
                raw.max_value if raw is not None else None,
                average_size,
                frozenset(column_sources),
            )
            sources.update(column_sources)

        if statistics is not None and statistics.size_bytes is not None:
            size = float(statistics.size_bytes)
            sources.add(f"catalog:{plan.table_name}.size_bytes")
        elif table.partitions and all(item.size_bytes is not None for item in table.partitions):
            size = float(sum(item.size_bytes or 0 for item in table.partitions))
            sources.add(f"partitions:{plan.table_name}.size_bytes")
        else:
            size = rows * sum(item.average_size_bytes for item in columns.values())
            sources.add(f"default:{plan.table_name}.size_bytes")
        distribution = Distribution(
            table.partition_strategy,
            tuple(f"{plan.alias}.{key}".casefold() for key in table.partition_keys),
            max(len(table.partitions), 1),
        )
        return PlanEstimate(
            rows,
            size,
            columns,
            Cost(cpu=rows * 0.1, disk=size),
            frozenset(sources),
            distribution,
        )

    def _filter(self, plan: Filter) -> PlanEstimate:
        child = self.estimate(plan.input)
        selectivity, evidence = self._selectivity(plan.predicate, child)
        rows = min(max(child.row_count * selectivity, 0.0), child.row_count)
        ratio = rows / child.row_count if child.row_count else 0.0
        columns = {
            name: ColumnEstimate(
                min(column.null_fraction, 1.0),
                min(column.distinct_count, max(rows, 1.0)),
                column.min_value,
                column.max_value,
                column.average_size_bytes,
                column.sources,
            )
            for name, column in child.columns.items()
        }
        return PlanEstimate(
            rows,
            child.size_bytes * ratio,
            columns,
            child.cost + Cost(cpu=child.row_count),
            child.sources | frozenset({evidence}),
            child.distribution,
        )

    def _project(self, plan: Project) -> PlanEstimate:
        child = self.estimate(plan.input)
        columns: dict[str, ColumnEstimate] = {}
        for item in plan.expressions:
            if isinstance(item.expression, Column):
                column = _column_estimate(item.expression, child)
                if column is not None:
                    columns[item.name.casefold()] = column
        width = _row_width(plan.output_schema)
        return PlanEstimate(
            child.row_count,
            child.row_count * width,
            columns,
            child.cost + Cost(cpu=child.row_count * 0.2),
            child.sources | frozenset({"derived:project"}),
            child.distribution,
        )

    def _aggregate(self, plan: Aggregate) -> PlanEstimate:
        child = self.estimate(plan.input)
        rows = self._group_count(plan.group_by, child)
        width = _row_width(plan.output_schema)
        memory = rows * width
        return PlanEstimate(
            rows,
            rows * width,
            {},
            child.cost
            + Cost(
                cpu=child.row_count,
                memory=min(memory, self.memory_budget_bytes),
                disk=max(memory - self.memory_budget_bytes, 0.0) * 2.0,
            ),
            child.sources | frozenset({"derived:aggregate"}),
        )

    def _grouping_sets(self, plan: GroupingSets) -> PlanEstimate:
        child = self.estimate(plan.input)
        rows = min(
            child.row_count * max(len(plan.grouping_sets), 1),
            sum(self._group_count(grouping_set, child) for grouping_set in plan.grouping_sets),
        )
        width = _row_width(plan.output_schema)
        return PlanEstimate(
            rows,
            rows * width,
            {},
            child.cost + Cost(cpu=child.row_count * max(len(plan.grouping_sets), 1)),
            child.sources | frozenset({"derived:grouping_sets"}),
        )

    def _group_count(
        self,
        expressions: tuple[Expression, ...],
        child: PlanEstimate,
    ) -> float:
        if not expressions:
            return 1.0
        if child.row_count == 0:
            return 0.0
        counts = []
        for expression in expressions:
            column = _column_estimate(expression, child)
            counts.append(column.distinct_count if column is not None else DEFAULT_NDV)
        return max(min(prod(counts), child.row_count), 1.0)

    def _selectivity(
        self,
        expression: Expression,
        estimate: PlanEstimate,
    ) -> tuple[float, str]:
        if isinstance(expression, Binary):
            if expression.operator is BinaryOperator.AND:
                left, left_source = self._selectivity(expression.left, estimate)
                right, right_source = self._selectivity(expression.right, estimate)
                return left * right, f"derived:and({left_source},{right_source})"
            if expression.operator is BinaryOperator.OR:
                left, left_source = self._selectivity(expression.left, estimate)
                right, right_source = self._selectivity(expression.right, estimate)
                return left + right - left * right, f"derived:or({left_source},{right_source})"
            pair = _column_literal(expression)
            if pair is not None:
                column_expression, literal, operator = pair
                column = _column_estimate(column_expression, estimate)
                if column is not None:
                    non_null = 1.0 - column.null_fraction
                    if operator is BinaryOperator.EQUAL:
                        if _uses_default_ndv(column):
                            return non_null * 0.1, "default:equality_selectivity=0.1"
                        return non_null / max(column.distinct_count, 1.0), "column_ndv"
                    if operator is BinaryOperator.NOT_EQUAL:
                        if _uses_default_ndv(column):
                            return non_null * 0.9, "default:inequality_selectivity=0.9"
                        selected = non_null * (
                            1.0 - 1.0 / max(column.distinct_count, 1.0)
                        )
                        return selected, "column_ndv"
                    ranged = _range_selectivity(operator, literal.value, column)
                    if ranged is not None:
                        return non_null * ranged, "column_min_max"
                    return non_null * 0.5, "default:range_selectivity=0.5"
            if expression.operator is BinaryOperator.EQUAL:
                columns = tuple(expression_columns(expression))
                if len(columns) == 2:
                    first = _column_estimate(columns[0], estimate)
                    second = _column_estimate(columns[1], estimate)
                    if first is not None and second is not None:
                        return 1.0 / max(first.distinct_count, second.distinct_count), "column_ndv"
        if isinstance(expression, IsNull):
            column = _column_estimate(expression.expression, estimate)
            if column is not None:
                value = column.null_fraction
                return (1.0 - value if expression.negated else value), "column_null_count"
        if isinstance(expression, InList):
            column = _column_estimate(expression.expression, estimate)
            if column is not None:
                selectivity = len(expression.options) / max(column.distinct_count, 1.0)
                return min(selectivity, 1.0) * (1.0 - column.null_fraction), "column_ndv"
        if isinstance(expression, Between):
            return 0.25, "default:between_selectivity=0.25"
        if isinstance(expression, Like):
            return 0.2, "default:like_selectivity=0.2"
        if isinstance(expression, ExpressionLiteral):
            return (1.0 if expression.value is True else 0.0), "literal"
        return 0.5, "default:filter_selectivity=0.5"

    def _join_rows(
        self,
        plan: Join,
        left: PlanEstimate,
        right: PlanEstimate,
        left_keys: tuple[str, ...],
        right_keys: tuple[str, ...],
    ) -> float:
        if left_keys:
            selectivity = 1.0
            for left_key, right_key in zip(left_keys, right_keys, strict=True):
                left_column = left.columns.get(left_key.casefold())
                right_column = right.columns.get(right_key.casefold())
                if left_column is None or right_column is None:
                    selectivity *= 0.1
                    continue
                if _uses_default_ndv(left_column) or _uses_default_ndv(right_column):
                    selectivity *= 0.1
                    continue
                selectivity *= (
                    (1.0 - left_column.null_fraction)
                    * (1.0 - right_column.null_fraction)
                    / max(left_column.distinct_count, right_column.distinct_count, 1.0)
                )
            rows = left.row_count * right.row_count * selectivity
        else:
            rows = left.row_count * right.row_count * 0.1
        if plan.join_type == "left":
            rows = max(rows, left.row_count)
        elif plan.join_type == "right":
            rows = max(rows, right.row_count)
        elif plan.join_type == "full":
            rows = max(rows, left.row_count, right.row_count)
        return max(rows, 0.0)

    def _join_strategy(
        self,
        left: PlanEstimate,
        right: PlanEstimate,
        left_keys: tuple[str, ...],
        right_keys: tuple[str, ...],
        build_side: TypeLiteral["left", "right"],
    ) -> tuple[JoinStrategy, float, str]:
        candidates: list[tuple[JoinStrategy, float, str]] = []
        if _compatible(left.distribution, right.distribution, left_keys, right_keys):
            candidates.append((JoinStrategy.REUSE, 0.0, "both inputs already colocated"))
        if _partitioned_on(left.distribution, left_keys):
            candidates.append(
                (JoinStrategy.REPARTITION_RIGHT, right.size_bytes, "reuse left partitioning")
            )
        if _partitioned_on(right.distribution, right_keys):
            candidates.append(
                (JoinStrategy.REPARTITION_LEFT, left.size_bytes, "reuse right partitioning")
            )
        candidates.append(
            (
                JoinStrategy.REPARTITION_BOTH,
                left.size_bytes + right.size_bytes,
                "hash repartition both inputs",
            )
        )
        build = left if build_side == "left" else right
        if build.size_bytes <= min(self.broadcast_threshold_bytes, self.memory_budget_bytes):
            candidates.append(
                (
                    JoinStrategy.BROADCAST,
                    build.size_bytes * max(self.worker_count - 1, 0),
                    f"broadcast {build_side} build input",
                )
            )
        return min(candidates, key=lambda item: (item[1], item[0].value))

    @staticmethod
    def _unary(
        child: PlanEstimate,
        rows: float,
        *,
        cpu: float = 0.0,
        memory: float = 0.0,
        disk: float = 0.0,
    ) -> PlanEstimate:
        ratio = rows / child.row_count if child.row_count else 0.0
        return PlanEstimate(
            rows,
            child.size_bytes * ratio,
            child.columns,
            child.cost + Cost(cpu=cpu, memory=memory, disk=disk),
            child.sources | frozenset({"derived:unary"}),
            child.distribution,
        )


def _type_width(data_type: DataType) -> float:
    if data_type in {DataType.BOOLEAN}:
        return 1.0
    if data_type in {DataType.INT32, DataType.FLOAT32, DataType.DATE}:
        return 4.0
    if data_type in {
        DataType.INT64,
        DataType.FLOAT64,
        DataType.DECIMAL,
        DataType.TIMESTAMP,
    }:
        return 8.0
    if data_type is DataType.BINARY:
        return 32.0
    if data_type in {DataType.LIST, DataType.STRUCT}:
        return 64.0
    return 24.0


def _row_width(schema: Schema) -> float:
    return max(sum(_type_width(field.data_type) for field in schema.fields), 1.0)


def _column_estimate(
    expression: Expression,
    estimate: PlanEstimate,
) -> ColumnEstimate | None:
    if not isinstance(expression, Column):
        return None
    return estimate.columns.get(expression.sql().casefold()) or estimate.columns.get(
        expression.name.casefold()
    )


def _uses_default_ndv(column: ColumnEstimate) -> bool:
    return any(
        source.endswith(".ndv") and source.startswith("default:")
        for source in column.sources
    )


def _column_literal(
    expression: Binary,
) -> tuple[Column, ExpressionLiteral, BinaryOperator] | None:
    left = expression.left
    right = expression.right
    if isinstance(left, Cast):
        left = left.expression
    if isinstance(right, Cast):
        right = right.expression
    if isinstance(left, Column) and isinstance(right, ExpressionLiteral):
        return left, right, expression.operator
    if isinstance(right, Column) and isinstance(left, ExpressionLiteral):
        reversed_operators = {
            BinaryOperator.LESS_THAN: BinaryOperator.GREATER_THAN,
            BinaryOperator.LESS_THAN_OR_EQUAL: BinaryOperator.GREATER_THAN_OR_EQUAL,
            BinaryOperator.GREATER_THAN: BinaryOperator.LESS_THAN,
            BinaryOperator.GREATER_THAN_OR_EQUAL: BinaryOperator.LESS_THAN_OR_EQUAL,
        }
        return right, left, reversed_operators.get(expression.operator, expression.operator)
    return None


def _range_selectivity(
    operator: BinaryOperator,
    value: object,
    column: ColumnEstimate,
) -> float | None:
    if operator not in {
        BinaryOperator.LESS_THAN,
        BinaryOperator.LESS_THAN_OR_EQUAL,
        BinaryOperator.GREATER_THAN,
        BinaryOperator.GREATER_THAN_OR_EQUAL,
    }:
        return None
    minimum = column.min_value
    maximum = column.max_value
    if not isinstance(minimum, int | float) or not isinstance(maximum, int | float):
        return None
    if not isinstance(value, int | float) or maximum <= minimum:
        return None
    fraction = min(max((float(value) - minimum) / (maximum - minimum), 0.0), 1.0)
    if operator in {BinaryOperator.GREATER_THAN, BinaryOperator.GREATER_THAN_OR_EQUAL}:
        return 1.0 - fraction
    return fraction


def _join_keys(plan: Join) -> tuple[tuple[str, ...], tuple[str, ...]]:
    left_sources = plan_sources(plan.left)
    right_sources = plan_sources(plan.right)
    left_keys: list[str] = []
    right_keys: list[str] = []

    def visit(expression: Expression) -> None:
        if isinstance(expression, Binary) and expression.operator is BinaryOperator.AND:
            visit(expression.left)
            visit(expression.right)
            return
        if not isinstance(expression, Binary) or expression.operator is not BinaryOperator.EQUAL:
            return
        if not isinstance(expression.left, Column) or not isinstance(expression.right, Column):
            return
        left_source = expression.left.source.casefold()
        right_source = expression.right.source.casefold()
        if left_source in left_sources and right_source in right_sources:
            left_keys.append(expression.left.sql().casefold())
            right_keys.append(expression.right.sql().casefold())
        elif right_source in left_sources and left_source in right_sources:
            left_keys.append(expression.right.sql().casefold())
            right_keys.append(expression.left.sql().casefold())

    visit(plan.condition)
    return tuple(left_keys), tuple(right_keys)


def _partitioned_on(distribution: Distribution, keys: tuple[str, ...]) -> bool:
    if distribution.strategy is PartitionStrategy.SINGLE:
        return True
    return (
        distribution.strategy is PartitionStrategy.HASH
        and bool(keys)
        and tuple(key.casefold() for key in distribution.keys)
        == tuple(key.casefold() for key in keys)
    )


def _compatible(
    left: Distribution,
    right: Distribution,
    left_keys: tuple[str, ...],
    right_keys: tuple[str, ...],
) -> bool:
    if left.strategy is PartitionStrategy.SINGLE and right.strategy is PartitionStrategy.SINGLE:
        return True
    return (
        _partitioned_on(left, left_keys)
        and _partitioned_on(right, right_keys)
        and left.partition_count == right.partition_count
    )


def _join_distribution(
    strategy: JoinStrategy,
    left: PlanEstimate,
    right: PlanEstimate,
    left_keys: tuple[str, ...],
    right_keys: tuple[str, ...],
    worker_count: int,
) -> Distribution:
    if strategy is JoinStrategy.REUSE:
        return left.distribution
    if strategy is JoinStrategy.REPARTITION_RIGHT:
        return left.distribution
    if strategy is JoinStrategy.REPARTITION_LEFT:
        return right.distribution
    if strategy is JoinStrategy.BROADCAST:
        return right.distribution if left.size_bytes <= right.size_bytes else left.distribution
    keys = left_keys or right_keys
    return Distribution(PartitionStrategy.HASH, keys, worker_count)

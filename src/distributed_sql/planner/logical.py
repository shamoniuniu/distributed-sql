"""Logical relational operators produced by the SQL binder."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from pydantic import JsonValue

from distributed_sql.common.protocol import PlanNode, PlanNodeType, Schema
from distributed_sql.planner.expressions import Expression, SortExpression, WindowFunction


class LogicalPlan(Protocol):
    @property
    def node_id(self) -> str: ...

    @property
    def output_schema(self) -> Schema: ...

    @property
    def children(self) -> tuple[LogicalPlan, ...]: ...

    def to_protocol(self) -> PlanNode: ...


def _protocol(
    node_id: str,
    node_type: PlanNodeType,
    output_schema: Schema,
    children: tuple[LogicalPlan, ...],
    properties: dict[str, JsonValue],
) -> PlanNode:
    return PlanNode(
        node_id=node_id,
        node_type=node_type,
        output_schema=output_schema,
        children=[child.to_protocol() for child in children],
        properties=properties,
    )


@dataclass(frozen=True, slots=True)
class NamedExpression:
    name: str
    expression: Expression


@dataclass(frozen=True, slots=True)
class Scan:
    node_id: str
    table_name: str
    alias: str
    output_schema: Schema

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return ()

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.SCAN,
            self.output_schema,
            self.children,
            {"table": self.table_name, "alias": self.alias},
        )


@dataclass(frozen=True, slots=True)
class Project:
    node_id: str
    input: LogicalPlan
    expressions: tuple[NamedExpression, ...]
    output_schema: Schema

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.PROJECT,
            self.output_schema,
            self.children,
            {
                "expressions": [
                    f"{item.expression.sql()} AS {item.name}" for item in self.expressions
                ]
            },
        )


@dataclass(frozen=True, slots=True)
class Filter:
    node_id: str
    input: LogicalPlan
    predicate: Expression
    output_schema: Schema
    phase: str = "where"

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.FILTER,
            self.output_schema,
            self.children,
            {"predicate": self.predicate.sql(), "phase": self.phase},
        )


@dataclass(frozen=True, slots=True)
class Aggregate:
    node_id: str
    input: LogicalPlan
    group_by: tuple[Expression, ...]
    aggregates: tuple[AggregateExpression, ...]
    output_schema: Schema

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.AGGREGATE,
            self.output_schema,
            self.children,
            {
                "group_by": [expression.sql() for expression in self.group_by],
                "aggregates": [item.expression.sql() for item in self.aggregates],
            },
        )


@dataclass(frozen=True, slots=True)
class AggregateExpression:
    name: str
    expression: Expression


@dataclass(frozen=True, slots=True)
class Join:
    node_id: str
    left: LogicalPlan
    right: LogicalPlan
    join_type: str
    condition: Expression
    output_schema: Schema
    input_limit: int | None = None
    build_side: str = "right"

    def __post_init__(self) -> None:
        if self.build_side not in {"left", "right"}:
            raise ValueError("join build_side must be left or right")

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.left, self.right)

    def to_protocol(self) -> PlanNode:
        properties: dict[str, JsonValue] = {
            "join_type": self.join_type,
            "condition": self.condition.sql(),
            "build_side": self.build_side,
        }
        if self.input_limit is not None:
            properties["input_limit"] = self.input_limit
        return _protocol(
            self.node_id,
            PlanNodeType.JOIN,
            self.output_schema,
            self.children,
            properties,
        )


@dataclass(frozen=True, slots=True)
class Limit:
    node_id: str
    input: LogicalPlan
    count: int
    output_schema: Schema

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.LIMIT,
            self.output_schema,
            self.children,
            {"count": self.count},
        )


@dataclass(frozen=True, slots=True)
class Order:
    node_id: str
    input: LogicalPlan
    order_by: tuple[SortExpression, ...]
    output_schema: Schema

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.ORDER,
            self.output_schema,
            self.children,
            {"order_by": [expression.sql() for expression in self.order_by]},
        )


@dataclass(frozen=True, slots=True)
class Window:
    node_id: str
    input: LogicalPlan
    expressions: tuple[NamedWindowExpression, ...]
    output_schema: Schema

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.WINDOW,
            self.output_schema,
            self.children,
            {
                "expressions": [
                    f"{item.expression.sql()} AS {item.name}" for item in self.expressions
                ]
            },
        )


@dataclass(frozen=True, slots=True)
class NamedWindowExpression:
    name: str
    expression: WindowFunction


@dataclass(frozen=True, slots=True)
class GroupingSets:
    node_id: str
    input: LogicalPlan
    grouping_sets: tuple[tuple[Expression, ...], ...]
    aggregates: tuple[AggregateExpression, ...]
    output_schema: Schema

    @property
    def children(self) -> tuple[LogicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return _protocol(
            self.node_id,
            PlanNodeType.GROUPING_SETS,
            self.output_schema,
            self.children,
            {
                "grouping_sets": [
                    [expression.sql() for expression in grouping_set]
                    for grouping_set in self.grouping_sets
                ],
                "aggregates": [item.expression.sql() for item in self.aggregates],
            },
        )

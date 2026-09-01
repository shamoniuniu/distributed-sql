"""Shared plan and expression helpers for rule-based optimization."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import fields, is_dataclass, replace
from typing import cast

from distributed_sql.common.protocol import Schema, SchemaField
from distributed_sql.planner.expressions import (
    AggregateFunction,
    Between,
    Binary,
    BinaryOperator,
    Case,
    Cast,
    Column,
    Expression,
    InList,
    IsNull,
    Like,
    Literal,
    ScalarFunction,
    SortExpression,
    SQLValue,
    Unary,
    WindowFunction,
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


def expression_children(expression: Expression) -> tuple[Expression, ...]:
    result: list[Expression] = []
    if not is_dataclass(expression):
        return ()
    for item in fields(expression):
        value = getattr(expression, item.name)
        if isinstance(value, Expression):
            result.append(value)
        elif isinstance(value, tuple):
            result.extend(_expressions_in_tuple(value))
    return tuple(result)


def _expressions_in_tuple(values: tuple[object, ...]) -> list[Expression]:
    result: list[Expression] = []
    for value in values:
        if isinstance(value, Expression):
            result.append(value)
        elif isinstance(value, tuple):
            result.extend(_expressions_in_tuple(value))
    return result


def walk_expression(expression: Expression) -> tuple[Expression, ...]:
    result = [expression]
    for child in expression_children(expression):
        result.extend(walk_expression(child))
    return tuple(result)


def expression_columns(expression: Expression) -> frozenset[Column]:
    return frozenset(item for item in walk_expression(expression) if isinstance(item, Column))


def expression_sources(expression: Expression) -> frozenset[str]:
    return frozenset(column.source.casefold() for column in expression_columns(expression))


def plan_sources(plan: LogicalPlan) -> frozenset[str]:
    if isinstance(plan, Scan):
        return frozenset({plan.alias.casefold()})
    return frozenset().union(*(plan_sources(child) for child in plan.children))


def split_conjuncts(expression: Expression) -> tuple[Expression, ...]:
    if isinstance(expression, Binary) and expression.operator is BinaryOperator.AND:
        return split_conjuncts(expression.left) + split_conjuncts(expression.right)
    return (expression,)


def combine_conjuncts(expressions: tuple[Expression, ...]) -> Expression:
    if not expressions:
        raise ValueError("At least one conjunct is required.")
    result = expressions[0]
    for expression in expressions[1:]:
        result = Binary(
            BinaryOperator.AND,
            result,
            expression,
            result.type_info,
        )
    return result


def normalize_conjuncts(expression: Expression) -> Expression:
    unique = {item.sql(): item for item in split_conjuncts(expression)}
    return combine_conjuncts(tuple(unique[key] for key in sorted(unique)))


def map_expression(
    expression: Expression,
    transform: Callable[[Expression], Expression],
) -> Expression:
    """Rebuild an expression bottom-up, then invoke a typed transform callable."""

    def recurse(item: Expression) -> Expression:
        return map_expression(item, transform)

    rebuilt: Expression
    if isinstance(expression, Cast):
        rebuilt = replace(expression, expression=recurse(expression.expression))
    elif isinstance(expression, Unary):
        rebuilt = replace(expression, expression=recurse(expression.expression))
    elif isinstance(expression, Binary):
        rebuilt = replace(
            expression,
            left=recurse(expression.left),
            right=recurse(expression.right),
        )
    elif isinstance(expression, IsNull):
        rebuilt = replace(expression, expression=recurse(expression.expression))
    elif isinstance(expression, InList):
        rebuilt = replace(
            expression,
            expression=recurse(expression.expression),
            options=tuple(recurse(item) for item in expression.options),
        )
    elif isinstance(expression, Between):
        rebuilt = replace(
            expression,
            expression=recurse(expression.expression),
            low=recurse(expression.low),
            high=recurse(expression.high),
        )
    elif isinstance(expression, Like):
        rebuilt = replace(
            expression,
            expression=recurse(expression.expression),
            pattern=recurse(expression.pattern),
        )
    elif isinstance(expression, Case):
        rebuilt = replace(
            expression,
            branches=tuple(
                (recurse(condition), recurse(result)) for condition, result in expression.branches
            ),
            default=recurse(expression.default),
        )
    elif isinstance(expression, ScalarFunction | AggregateFunction):
        rebuilt = replace(
            expression,
            arguments=tuple(recurse(item) for item in expression.arguments),
        )
    elif isinstance(expression, WindowFunction):
        rebuilt = replace(
            expression,
            function=recurse(expression.function),
            partition_by=tuple(recurse(item) for item in expression.partition_by),
            order_by=tuple(
                SortExpression(
                    recurse(item.expression),
                    item.ascending,
                    item.nulls_first,
                )
                for item in expression.order_by
            ),
        )
    else:
        rebuilt = expression
    return transform(rebuilt)


def contains_aggregate(expression: Expression) -> bool:
    return any(isinstance(item, AggregateFunction) for item in walk_expression(expression))


def contains_window(expression: Expression) -> bool:
    return any(isinstance(item, WindowFunction) for item in walk_expression(expression))


def is_constant(expression: Expression) -> bool:
    return not any(
        isinstance(item, Column | AggregateFunction | WindowFunction)
        for item in walk_expression(expression)
    )


def schema_subset(schema: Schema, names: set[str]) -> Schema:
    return Schema(
        fields=[
            field
            for field in schema.fields
            if field.name.casefold() in names or field.name.rsplit(".", 1)[-1].casefold() in names
        ],
        metadata=schema.metadata,
    )


def schema_for_expressions(
    names_and_expressions: tuple[tuple[str, Expression], ...],
) -> Schema:
    return Schema(
        fields=[
            SchemaField(
                name=name,
                data_type=expression.type_info.data_type,
                nullable=expression.type_info.nullable,
            )
            for name, expression in names_and_expressions
        ]
    )


def replace_children(plan: LogicalPlan, children: tuple[LogicalPlan, ...]) -> LogicalPlan:
    if not children:
        return plan
    if isinstance(plan, Project):
        return replace(plan, input=children[0])
    if isinstance(plan, Filter):
        return replace(plan, input=children[0], output_schema=children[0].output_schema)
    if isinstance(plan, Aggregate):
        return replace(plan, input=children[0])
    if isinstance(plan, Join):
        return replace(plan, left=children[0], right=children[1])
    if isinstance(plan, Limit):
        return replace(plan, input=children[0], output_schema=children[0].output_schema)
    if isinstance(plan, Order):
        return replace(plan, input=children[0], output_schema=children[0].output_schema)
    if isinstance(plan, Window):
        return replace(plan, input=children[0])
    if isinstance(plan, GroupingSets):
        return replace(plan, input=children[0])
    raise TypeError(f"Unsupported logical plan: {type(plan).__name__}")


def plan_text(plan: LogicalPlan, *, indent: int = 0) -> str:
    prefix = "  " * indent
    details = ""
    if isinstance(plan, Scan):
        details = f"[{plan.table_name} AS {plan.alias}]"
    elif isinstance(plan, Project):
        details = "[" + ", ".join(item.name for item in plan.expressions) + "]"
    elif isinstance(plan, Filter):
        details = f"[{plan.predicate.sql()}]"
    elif isinstance(plan, Aggregate):
        details = "[group=" + ", ".join(item.sql() for item in plan.group_by) + "]"
    elif isinstance(plan, Join):
        hint = f", input_limit={plan.input_limit}" if plan.input_limit is not None else ""
        details = f"[{plan.join_type}, {plan.condition.sql()}{hint}]"
    elif isinstance(plan, Limit):
        details = f"[{plan.count}]"
    elif isinstance(plan, Order):
        details = "[" + ", ".join(item.sql() for item in plan.order_by) + "]"
    lines = [f"{prefix}{type(plan).__name__}#{plan.node_id}{details}"]
    for child in plan.children:
        lines.append(plan_text(child, indent=indent + 1))
    return "\n".join(lines)


def literal_for(expression: Expression, value: object) -> Literal:
    return Literal(cast(SQLValue, value), expression.type_info)

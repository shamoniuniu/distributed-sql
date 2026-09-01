"""Semantics-preserving logical optimization rules."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import replace
from itertools import combinations
from typing import Protocol

from distributed_sql.common.protocol import DataType, SchemaField
from distributed_sql.planner.expressions import (
    Binary,
    BinaryOperator,
    Column,
    Expression,
    Literal,
)
from distributed_sql.planner.logical import (
    Aggregate,
    Filter,
    GroupingSets,
    Join,
    Limit,
    LogicalPlan,
    NamedExpression,
    Order,
    Project,
    Scan,
    Window,
)
from distributed_sql.planner.types import TypeInfo

from .utils import (
    combine_conjuncts,
    contains_aggregate,
    contains_window,
    expression_columns,
    expression_sources,
    is_constant,
    literal_for,
    map_expression,
    normalize_conjuncts,
    plan_sources,
    schema_for_expressions,
    schema_subset,
    split_conjuncts,
)


class Rule(Protocol):
    name: str
    whole_plan: bool

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None: ...


class PredicatePushdownProject:
    name = "predicate_pushdown_project"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(plan, Filter) or not isinstance(plan.input, Project):
            return None
        project = plan.input
        aliases = {item.name.casefold(): item.expression for item in project.expressions}
        columns = expression_columns(plan.predicate)
        if any(column.name.casefold() not in aliases for column in columns):
            return None

        def substitute(expression: Expression) -> Expression:
            if isinstance(expression, Column):
                return aliases[expression.name.casefold()]
            return expression

        predicate = map_expression(plan.predicate, substitute)
        if contains_aggregate(predicate) or contains_window(predicate):
            return None
        pushed = Filter(
            plan.node_id,
            project.input,
            predicate,
            project.input.output_schema,
            plan.phase,
        )
        return replace(project, input=pushed)


class PredicatePushdownAggregate:
    name = "predicate_pushdown_aggregate"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(plan, Filter) or not isinstance(plan.input, Aggregate):
            return None
        aggregate = plan.input
        grouping_columns = {
            column.sql()
            for expression in aggregate.group_by
            if isinstance(expression, Column)
            for column in expression_columns(expression)
        }
        pushable: list[Expression] = []
        residual: list[Expression] = []
        for conjunct in split_conjuncts(plan.predicate):
            columns = {column.sql() for column in expression_columns(conjunct)}
            if (
                not contains_aggregate(conjunct)
                and not contains_window(conjunct)
                and bool(columns)
                and columns <= grouping_columns
            ):
                pushable.append(conjunct)
            else:
                residual.append(conjunct)
        if not pushable:
            return None
        pushed = Filter(
            f"{plan.node_id}_pre_aggregate",
            aggregate.input,
            combine_conjuncts(tuple(pushable)),
            aggregate.input.output_schema,
            "where",
        )
        rewritten: LogicalPlan = replace(aggregate, input=pushed)
        if residual:
            rewritten = replace(
                plan,
                input=rewritten,
                predicate=combine_conjuncts(tuple(residual)),
            )
        return rewritten


class PredicatePushdownJoin:
    name = "predicate_pushdown_join"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(plan, Filter) or not isinstance(plan.input, Join):
            return None
        join = plan.input
        left_sources = plan_sources(join.left)
        right_sources = plan_sources(join.right)
        left_predicates: list[Expression] = []
        right_predicates: list[Expression] = []
        residual: list[Expression] = []
        for conjunct in split_conjuncts(plan.predicate):
            sources = expression_sources(conjunct)
            if sources and sources <= left_sources and join.join_type in {"inner", "left"}:
                left_predicates.append(conjunct)
            elif sources and sources <= right_sources and join.join_type in {"inner", "right"}:
                right_predicates.append(conjunct)
            else:
                residual.append(conjunct)
        if not left_predicates and not right_predicates:
            return None
        left = join.left
        right = join.right
        if left_predicates:
            left = Filter(
                f"{plan.node_id}_left",
                left,
                combine_conjuncts(tuple(left_predicates)),
                left.output_schema,
                plan.phase,
            )
        if right_predicates:
            right = Filter(
                f"{plan.node_id}_right",
                right,
                combine_conjuncts(tuple(right_predicates)),
                right.output_schema,
                plan.phase,
            )
        rewritten: LogicalPlan = replace(join, left=left, right=right)
        if residual:
            rewritten = replace(
                plan,
                input=rewritten,
                predicate=combine_conjuncts(tuple(residual)),
            )
        return rewritten


class LimitPushdownProject:
    name = "limit_pushdown_project"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(plan, Limit) or not isinstance(plan.input, Project):
            return None
        project = plan.input
        pushed = Limit(plan.node_id, project.input, plan.count, project.input.output_schema)
        return replace(project, input=pushed)


class LimitJoinInputHint:
    name = "limit_join_input_hint"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(plan, Limit) or not isinstance(plan.input, Join):
            return None
        join = plan.input
        if join.input_limit is not None and join.input_limit <= plan.count:
            return None
        return replace(plan, input=replace(join, input_limit=plan.count))


class ColumnPruning:
    name = "column_pruning"
    whole_plan = True

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        required = {field.name.casefold() for field in plan.output_schema.fields}
        rewritten = self._prune(plan, required)
        return rewritten if rewritten != plan else None

    def _prune(self, plan: LogicalPlan, required: set[str]) -> LogicalPlan:
        if isinstance(plan, Scan):
            scan_required = {
                column.name.casefold()
                for name in required
                for column in _matching_scan_columns(plan, name)
            }
            return replace(plan, output_schema=schema_subset(plan.output_schema, scan_required))
        if isinstance(plan, Project):
            selected = tuple(
                item
                for item in plan.expressions
                if item.name.casefold() in required or f".{item.name.casefold()}" in required
            )
            child_required = _required_names(
                expression for item in selected for expression in (item.expression,)
            )
            child = self._prune(plan.input, child_required)
            return replace(
                plan,
                input=child,
                expressions=selected,
                output_schema=schema_for_expressions(
                    tuple((item.name, item.expression) for item in selected)
                ),
            )
        if isinstance(plan, Filter):
            child_required = required | _required_names((plan.predicate,))
            child = self._prune(plan.input, child_required)
            return replace(plan, input=child, output_schema=child.output_schema)
        if isinstance(plan, Join):
            condition_required = _required_names((plan.condition,))
            all_required = required | condition_required
            left_sources = plan_sources(plan.left)
            right_sources = plan_sources(plan.right)
            left_required = _for_sources(all_required, left_sources)
            right_required = _for_sources(all_required, right_sources)
            left = self._prune(plan.left, left_required)
            right = self._prune(plan.right, right_required)
            return replace(
                plan,
                left=left,
                right=right,
                output_schema=schema_subset(plan.output_schema, required),
            )
        if isinstance(plan, Aggregate):
            child_required = _required_names(
                (*plan.group_by, *(item.expression for item in plan.aggregates))
            )
            return replace(plan, input=self._prune(plan.input, child_required))
        if isinstance(plan, GroupingSets):
            expressions = tuple(
                expression for grouping_set in plan.grouping_sets for expression in grouping_set
            ) + tuple(item.expression for item in plan.aggregates)
            return replace(plan, input=self._prune(plan.input, _required_names(expressions)))
        if isinstance(plan, Order):
            child_required = required | _required_names(
                tuple(item.expression for item in plan.order_by)
            )
            child = self._prune(plan.input, child_required)
            return replace(plan, input=child, output_schema=child.output_schema)
        if isinstance(plan, Window):
            child_required = required | _required_names(
                tuple(item.expression for item in plan.expressions)
            )
            child = self._prune(plan.input, child_required)
            return replace(plan, input=child)
        if isinstance(plan, Limit):
            child = self._prune(plan.input, required)
            return replace(plan, input=child, output_schema=child.output_schema)
        return plan


class ConstantFolding:
    name = "constant_folding"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        rewritten = _map_plan_expressions(plan, _fold_expression)
        return rewritten if rewritten != plan else None


class PredicateMerge:
    name = "predicate_merge"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(plan, Filter):
            return None
        if isinstance(plan.input, Filter):
            merged = normalize_conjuncts(combine_conjuncts((plan.input.predicate, plan.predicate)))
            return Filter(
                plan.node_id,
                plan.input.input,
                merged,
                plan.input.input.output_schema,
                plan.phase,
            )
        normalized = normalize_conjuncts(plan.predicate)
        return replace(plan, predicate=normalized) if normalized != plan.predicate else None


class EqualityInference:
    name = "equality_inference"
    whole_plan = False

    def apply(self, plan: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(plan, Filter):
            return None
        evidence = list(split_conjuncts(plan.predicate))
        if isinstance(plan.input, Join) and plan.input.join_type == "inner":
            evidence.extend(split_conjuncts(plan.input.condition))
        inferred = _equality_closure(tuple(evidence))
        existing = {_equality_key(item) for item in evidence}
        additions = tuple(
            expression for expression in inferred if _equality_key(expression) not in existing
        )
        if not additions:
            return None
        predicate = normalize_conjuncts(
            combine_conjuncts(split_conjuncts(plan.predicate) + additions)
        )
        return replace(plan, predicate=predicate)


DEFAULT_RULES: tuple[Rule, ...] = (
    ConstantFolding(),
    PredicateMerge(),
    EqualityInference(),
    PredicatePushdownProject(),
    PredicatePushdownAggregate(),
    PredicatePushdownJoin(),
    LimitPushdownProject(),
    LimitJoinInputHint(),
    ColumnPruning(),
)


def _matching_scan_columns(plan: Scan, required: str) -> tuple[Column, ...]:
    result = []
    for field in plan.output_schema.fields:
        column = Column(field.name, plan.alias, _type_info(field))
        if required in {field.name.casefold(), column.sql().casefold()}:
            result.append(column)
    return tuple(result)


def _type_info(field: SchemaField) -> TypeInfo:
    return TypeInfo(field.data_type, field.nullable)


def _required_names(expressions: Iterable[Expression]) -> set[str]:
    return {
        column.sql().casefold()
        for expression in expressions
        for column in expression_columns(expression)
    }


def _for_sources(required: set[str], sources: frozenset[str]) -> set[str]:
    return {
        name for name in required if "." not in name or name.split(".", 1)[0].casefold() in sources
    }


def _fold_expression(expression: Expression) -> Expression:
    if isinstance(expression, Binary):
        if expression.operator is BinaryOperator.AND:
            if isinstance(expression.left, Literal):
                if expression.left.value is True:
                    return expression.right
                if expression.left.value is False:
                    return expression.left
            if isinstance(expression.right, Literal):
                if expression.right.value is True:
                    return expression.left
                if expression.right.value is False:
                    return expression.right
        if expression.operator is BinaryOperator.OR:
            if isinstance(expression.left, Literal):
                if expression.left.value is False:
                    return expression.right
                if expression.left.value is True:
                    return expression.left
            if isinstance(expression.right, Literal):
                if expression.right.value is False:
                    return expression.left
                if expression.right.value is True:
                    return expression.right
    if not is_constant(expression) or isinstance(expression, Literal):
        return expression
    try:
        return literal_for(expression, expression.evaluate({}))
    except (ArithmeticError, TypeError, ValueError):
        return expression


def _map_plan_expressions(
    plan: LogicalPlan,
    transform: Callable[[Expression], Expression],
) -> LogicalPlan:
    def fold(expression: Expression) -> Expression:
        return map_expression(expression, transform)

    if isinstance(plan, Project):
        return replace(
            plan,
            expressions=tuple(
                NamedExpression(item.name, fold(item.expression)) for item in plan.expressions
            ),
        )
    if isinstance(plan, Filter):
        return replace(plan, predicate=fold(plan.predicate))
    if isinstance(plan, Aggregate):
        return replace(
            plan,
            group_by=tuple(fold(item) for item in plan.group_by),
            aggregates=tuple(
                replace(item, expression=fold(item.expression)) for item in plan.aggregates
            ),
        )
    if isinstance(plan, Join):
        return replace(plan, condition=fold(plan.condition))
    if isinstance(plan, Order):
        return replace(
            plan,
            order_by=tuple(
                replace(item, expression=fold(item.expression)) for item in plan.order_by
            ),
        )
    return plan


def _equality_key(expression: Expression) -> str:
    if isinstance(expression, Binary) and expression.operator is BinaryOperator.EQUAL:
        return "=".join(sorted((expression.left.sql(), expression.right.sql())))
    return expression.sql()


def _equality_closure(expressions: tuple[Expression, ...]) -> tuple[Expression, ...]:
    terms: dict[str, Expression] = {}
    edges: list[tuple[str, str]] = []
    for expression in expressions:
        if not isinstance(expression, Binary) or expression.operator is not BinaryOperator.EQUAL:
            continue
        if not isinstance(expression.left, Column) and not is_constant(expression.left):
            continue
        if not isinstance(expression.right, Column) and not is_constant(expression.right):
            continue
        if _constant_is_null(expression.left) or _constant_is_null(expression.right):
            continue
        left = expression.left.sql()
        right = expression.right.sql()
        terms[left] = expression.left
        terms[right] = expression.right
        edges.append((left, right))

    adjacency: dict[str, set[str]] = {term: set() for term in terms}
    for left, right in edges:
        adjacency[left].add(right)
        adjacency[right].add(left)
    result: list[Expression] = []
    visited: set[str] = set()
    for start in sorted(adjacency):
        if start in visited:
            continue
        stack = [start]
        component: set[str] = set()
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            visited.add(current)
            stack.extend(adjacency[current] - component)
        for left, right in combinations(sorted(component), 2):
            left_expression = terms[left]
            right_expression = terms[right]
            result.append(
                Binary(
                    BinaryOperator.EQUAL,
                    left_expression,
                    right_expression,
                    TypeInfo(
                        DataType.BOOLEAN,
                        left_expression.type_info.nullable or right_expression.type_info.nullable,
                    ),
                )
            )
    return tuple(result)


def _constant_is_null(expression: Expression) -> bool:
    if not is_constant(expression):
        return False
    try:
        return expression.evaluate({}) is None
    except (ArithmeticError, TypeError, ValueError):
        return False

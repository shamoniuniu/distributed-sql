"""Name binding, expression typing, and logical plan construction."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from itertools import count
from typing import NoReturn, cast

from sqlglot import exp

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import DataType, Schema, SchemaField
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
    Unary,
    UnaryOperator,
    WindowFrame,
    WindowFunction,
    coerce_pair,
    numeric_result_type,
)
from distributed_sql.planner.logical import (
    Aggregate,
    AggregateExpression,
    Filter,
    GroupingSets,
    Join,
    Limit,
    LogicalPlan,
    NamedExpression,
    NamedWindowExpression,
    Order,
    Project,
    Scan,
    Window,
)
from distributed_sql.planner.parser import expression_location, parse_select
from distributed_sql.planner.types import NUMERIC_TYPES, TypeInfo, common_type, literal_type

CatalogEntry = Schema | CatalogTable


@dataclass(frozen=True, slots=True)
class _Source:
    table_name: str
    alias: str
    schema: Schema


class _Scope:
    def __init__(self) -> None:
        self._sources: dict[str, _Source] = {}

    @property
    def sources(self) -> tuple[_Source, ...]:
        return tuple(self._sources.values())

    def add(self, source: _Source, expression: exp.Expression) -> None:
        key = source.alias.casefold()
        if key in self._sources:
            _binding_error(
                f"Duplicate table alias '{source.alias}'.",
                expression,
                alias=source.alias,
            )
        self._sources[key] = source

    def resolve(self, column: exp.Column) -> Column:
        qualifier = column.table
        name = column.name
        if qualifier:
            source = self._sources.get(qualifier.casefold())
            if source is None:
                _binding_error(
                    f"Unknown table or alias '{qualifier}'.",
                    column,
                    qualifier=qualifier,
                )
            field = _find_field(source.schema, name)
            if field is None:
                _binding_error(
                    f"Column '{qualifier}.{name}' does not exist.",
                    column,
                    column=f"{qualifier}.{name}",
                )
            return Column(
                name=field.name,
                source=source.alias,
                result_type=TypeInfo(field.data_type, field.nullable),
            )

        matches: list[tuple[_Source, SchemaField]] = []
        for source in self._sources.values():
            field = _find_field(source.schema, name)
            if field is not None:
                matches.append((source, field))
        if not matches:
            _binding_error(f"Column '{name}' does not exist.", column, column=name)
        if len(matches) > 1:
            _binding_error(
                f"Column reference '{name}' is ambiguous.",
                column,
                column=name,
                candidates=[source.alias for source, _ in matches],
            )
        source, field = matches[0]
        return Column(
            name=field.name,
            source=source.alias,
            result_type=TypeInfo(field.data_type, field.nullable),
        )


def _find_field(schema: Schema, name: str) -> SchemaField | None:
    folded = name.casefold()
    return next((field for field in schema.fields if field.name.casefold() == folded), None)


def _binding_error(
    message: str,
    expression: exp.Expression,
    **details: str | int | list[str],
) -> NoReturn:
    context: dict[str, object] = dict(details)
    context.update(expression_location(expression))
    raise DistributedSQLError(ErrorCode.BINDING_ERROR, message, context=context)


def _schema_of(entry: CatalogEntry) -> Schema:
    return entry if isinstance(entry, Schema) else entry.schema_


def _field_from_expression(name: str, expression: Expression) -> SchemaField:
    return SchemaField(
        name=name,
        data_type=expression.type_info.data_type,
        nullable=expression.type_info.nullable,
    )


def _bound_children(expression: Expression) -> Iterable[Expression]:
    if not is_dataclass(expression):
        return
    for field in fields(expression):
        value = getattr(expression, field.name)
        if isinstance(value, Expression):
            yield value
        elif isinstance(value, tuple):
            for item in value:
                if isinstance(item, Expression):
                    yield item
                elif isinstance(item, tuple):
                    for nested in item:
                        if isinstance(nested, Expression):
                            yield nested


def _walk_bound(expression: Expression) -> Iterable[Expression]:
    yield expression
    for child in _bound_children(expression):
        yield from _walk_bound(child)


def _walk_aggregates(expression: Expression) -> Iterable[AggregateFunction]:
    if isinstance(expression, WindowFunction):
        return
    if isinstance(expression, AggregateFunction):
        yield expression
        return
    for child in _bound_children(expression):
        yield from _walk_aggregates(child)


class Binder:
    """Bind SQL against an immutable catalog view and build a logical plan."""

    def __init__(self, catalog: Mapping[str, CatalogEntry], *, default_namespace: str = "default"):
        self._catalog = {name.casefold(): entry for name, entry in catalog.items()}
        self._default_namespace = default_namespace
        self._ids = count(1)

    def bind(self, sql: str) -> LogicalPlan:
        self._ids = count(1)
        statement = parse_select(sql)
        scope = _Scope()
        plan = self._bind_from(statement, scope)

        where = statement.args.get("where")
        if where is not None:
            predicate = self._bind_expression(where.this, scope)
            self._require_boolean(predicate, where.this)
            self._reject_aggregate_or_window(predicate, where.this)
            plan = Filter(self._id("filter"), plan, predicate, plan.output_schema)

        projections = self._bind_projections(statement, scope)
        aliases = {item.name.casefold(): item.expression for item in projections}
        group_by, grouping_sets = self._bind_groups(statement, scope)
        having = statement.args.get("having")
        having_predicate = (
            self._bind_expression(having.this, scope, aliases=aliases)
            if having is not None
            else None
        )
        if having is not None and having_predicate is not None:
            self._require_boolean(having_predicate, having.this)
            if any(isinstance(item, WindowFunction) for item in _walk_bound(having_predicate)):
                _binding_error("Window functions are not allowed in HAVING.", having.this)
        aggregates = self._collect_aggregates(projections, having_predicate)
        self._validate_aggregation(statement, projections, group_by, grouping_sets, aggregates)
        projection_schema = self._projection_schema(projections)
        if grouping_sets is not None:
            projection_schema = Schema(
                fields=[
                    field.model_copy(update={"nullable": True})
                    if not any(
                        isinstance(item, AggregateFunction | WindowFunction)
                        for item in _walk_bound(projection.expression)
                    )
                    else field
                    for field, projection in zip(
                        projection_schema.fields,
                        projections,
                        strict=True,
                    )
                ]
            )

        if grouping_sets is not None:
            plan = GroupingSets(
                self._id("grouping_sets"),
                plan,
                grouping_sets,
                aggregates,
                projection_schema,
            )
        elif group_by or aggregates:
            plan = Aggregate(
                self._id("aggregate"),
                plan,
                group_by,
                aggregates,
                projection_schema,
            )

        if having is not None and having_predicate is not None:
            plan = Filter(
                self._id("having"),
                plan,
                having_predicate,
                plan.output_schema,
                phase="having",
            )

        windows = self._collect_windows(projections)
        if windows:
            window_fields = list(plan.output_schema.fields)
            existing = {field.name.casefold() for field in window_fields}
            for item in windows:
                if item.name.casefold() not in existing:
                    window_fields.append(_field_from_expression(item.name, item.expression))
                    existing.add(item.name.casefold())
            plan = Window(
                self._id("window"),
                plan,
                windows,
                Schema(fields=window_fields),
            )

        plan = Project(
            self._id("project"),
            plan,
            projections,
            projection_schema,
        )

        order = statement.args.get("order")
        if order is not None:
            order_by = tuple(
                self._bind_ordered(item, scope, aliases, projections) for item in order.expressions
            )
            plan = Order(self._id("order"), plan, order_by, plan.output_schema)

        limit = statement.args.get("limit")
        if limit is not None:
            count_value = self._bind_limit(limit)
            plan = Limit(self._id("limit"), plan, count_value, plan.output_schema)
        return plan

    plan = bind

    def _id(self, prefix: str) -> str:
        return f"{prefix}_{next(self._ids)}"

    def _catalog_entry(self, table: exp.Table) -> tuple[str, CatalogEntry]:
        parts = [part.name for part in table.parts]
        table_name = ".".join(parts)
        candidates = [table_name]
        if len(parts) == 1:
            candidates.append(f"{self._default_namespace}.{table_name}")
        for candidate in candidates:
            entry = self._catalog.get(candidate.casefold())
            if entry is not None:
                return candidate, entry
        _binding_error(f"Table '{table_name}' does not exist.", table, table=table_name)

    def _bind_table(self, table: exp.Table, scope: _Scope) -> Scan:
        table_name, entry = self._catalog_entry(table)
        alias = table.alias_or_name
        source = _Source(table_name, alias, _schema_of(entry))
        scope.add(source, table)
        return Scan(self._id("scan"), table_name, alias, source.schema)

    def _bind_from(self, statement: exp.Select, scope: _Scope) -> LogicalPlan:
        from_clause = cast(exp.From, statement.args["from"])
        plan: LogicalPlan = self._bind_table(cast(exp.Table, from_clause.this), scope)
        for join_expression in statement.args.get("joins") or []:
            right = self._bind_table(cast(exp.Table, join_expression.this), scope)
            condition = self._bind_expression(join_expression.args["on"], scope)
            self._require_boolean(condition, join_expression.args["on"])
            self._reject_aggregate_or_window(condition, join_expression.args["on"])
            join_type = self._join_type(join_expression)
            output_fields = self._join_fields(scope, join_type)
            plan = Join(
                self._id("join"),
                plan,
                right,
                join_type,
                condition,
                Schema(fields=output_fields),
            )
        return plan

    @staticmethod
    def _join_type(join: exp.Join) -> str:
        side = str(join.args.get("side") or "").upper()
        if side == "LEFT":
            return "left"
        if side == "RIGHT":
            return "right"
        if side == "FULL":
            return "full"
        return "inner"

    @staticmethod
    def _join_fields(scope: _Scope, join_type: str) -> list[SchemaField]:
        result: list[SchemaField] = []
        sources = scope.sources
        for index, source in enumerate(sources):
            nullable_side = (
                join_type == "full"
                or (join_type == "left" and index == len(sources) - 1)
                or (join_type == "right" and index < len(sources) - 1)
            )
            for field in source.schema.fields:
                result.append(
                    SchemaField(
                        name=f"{source.alias}.{field.name}",
                        data_type=field.data_type,
                        nullable=field.nullable or nullable_side,
                        metadata=field.metadata,
                        children=field.children,
                    )
                )
        return result

    def _bind_projections(
        self, statement: exp.Select, scope: _Scope
    ) -> tuple[NamedExpression, ...]:
        result: list[NamedExpression] = []
        for index, selected in enumerate(statement.expressions, start=1):
            if isinstance(selected, exp.Star):
                for source in scope.sources:
                    result.extend(
                        NamedExpression(
                            (
                                f"{source.alias}.{field.name}"
                                if len(scope.sources) > 1
                                else field.name
                            ),
                            Column(
                                field.name,
                                source.alias,
                                TypeInfo(field.data_type, field.nullable),
                            ),
                        )
                        for field in source.schema.fields
                    )
                continue
            if isinstance(selected, exp.Column) and selected.is_star:
                selected_source = next(
                    (
                        item
                        for item in scope.sources
                        if item.alias.casefold() == selected.table.casefold()
                    ),
                    None,
                )
                if selected_source is None:
                    _binding_error(
                        f"Unknown table or alias '{selected.table}'.",
                        selected,
                        qualifier=selected.table,
                    )
                result.extend(
                    NamedExpression(
                        (
                            f"{selected_source.alias}.{field.name}"
                            if len(scope.sources) > 1
                            else field.name
                        ),
                        Column(
                            field.name,
                            selected_source.alias,
                            TypeInfo(field.data_type, field.nullable),
                        ),
                    )
                    for field in selected_source.schema.fields
                )
                continue
            expression_ast = selected.this if isinstance(selected, exp.Alias) else selected
            expression = self._bind_expression(expression_ast, scope)
            if isinstance(selected, exp.Alias):
                name = selected.alias
            elif isinstance(expression_ast, exp.Column):
                name = expression_ast.name
            else:
                name = f"_col{index}"
            result.append(NamedExpression(name, expression))

        names = [item.name.casefold() for item in result]
        duplicate = next((name for name in names if names.count(name) > 1), None)
        if duplicate is not None:
            _binding_error(
                f"Duplicate output column alias '{duplicate}'.",
                statement,
                alias=duplicate,
            )
        return tuple(result)

    @staticmethod
    def _projection_schema(projections: tuple[NamedExpression, ...]) -> Schema:
        return Schema(
            fields=[
                _field_from_expression(projection.name, projection.expression)
                for projection in projections
            ]
        )

    def _bind_groups(
        self,
        statement: exp.Select,
        scope: _Scope,
    ) -> tuple[tuple[Expression, ...], tuple[tuple[Expression, ...], ...] | None]:
        group = statement.args.get("group")
        if group is None:
            return (), None
        ordinary = tuple(self._bind_expression(item, scope) for item in group.expressions)
        for item, bound in zip(group.expressions, ordinary, strict=True):
            self._reject_aggregate_or_window(bound, item)
        grouping_nodes = group.args.get("grouping_sets") or []
        if not grouping_nodes:
            return ordinary, None
        if ordinary:
            _binding_error(
                "Ordinary GROUP BY expressions cannot be mixed with GROUPING SETS.",
                group,
            )
        sets: list[tuple[Expression, ...]] = []
        for grouping in grouping_nodes:
            for item in grouping.expressions:
                expressions = item.expressions if isinstance(item, exp.Tuple) else [item]
                sets.append(tuple(self._bind_expression(value, scope) for value in expressions))
        return (), tuple(sets)

    @staticmethod
    def _collect_aggregates(
        projections: tuple[NamedExpression, ...],
        having: Expression | None,
    ) -> tuple[AggregateExpression, ...]:
        result: list[AggregateExpression] = []
        seen: set[str] = set()
        for projection in projections:
            for expression in _walk_aggregates(projection.expression):
                if expression.sql() not in seen:
                    result.append(AggregateExpression(projection.name, expression))
                    seen.add(expression.sql())
        if having is not None:
            for index, expression in enumerate(_walk_aggregates(having), start=1):
                if expression.sql() not in seen:
                    result.append(AggregateExpression(f"_having{index}", expression))
                    seen.add(expression.sql())
        return tuple(result)

    @staticmethod
    def _collect_windows(
        projections: tuple[NamedExpression, ...],
    ) -> tuple[NamedWindowExpression, ...]:
        result: list[NamedWindowExpression] = []
        for projection in projections:
            for expression in _walk_bound(projection.expression):
                if isinstance(expression, WindowFunction):
                    result.append(NamedWindowExpression(projection.name, expression))
        return tuple(result)

    def _validate_aggregation(
        self,
        statement: exp.Select,
        projections: tuple[NamedExpression, ...],
        group_by: tuple[Expression, ...],
        grouping_sets: tuple[tuple[Expression, ...], ...] | None,
        aggregates: tuple[AggregateExpression, ...],
    ) -> None:
        if not aggregates and not group_by and grouping_sets is None:
            if statement.args.get("having") is not None:
                _binding_error("HAVING requires aggregation.", statement.args["having"])
            return
        grouping_columns = {
            expression.sql()
            for expression in group_by
            for expression in _walk_bound(expression)
            if isinstance(expression, Column)
        }
        if grouping_sets is not None:
            grouping_columns.update(
                expression.sql()
                for grouping_set in grouping_sets
                for item in grouping_set
                for expression in _walk_bound(item)
                if isinstance(expression, Column)
            )
        for projection in projections:
            if any(
                isinstance(item, AggregateFunction | WindowFunction)
                for item in _walk_bound(projection.expression)
            ):
                continue
            columns = {
                item.sql()
                for item in _walk_bound(projection.expression)
                if isinstance(item, Column)
            }
            if not columns <= grouping_columns:
                _binding_error(
                    f"Expression '{projection.name}' must appear in GROUP BY or be aggregated.",
                    statement,
                    column=projection.name,
                )

    def _bind_ordered(
        self,
        ordered: exp.Ordered,
        scope: _Scope,
        aliases: Mapping[str, Expression],
        projections: tuple[NamedExpression, ...],
    ) -> SortExpression:
        expression_ast = ordered.this
        expression: Expression
        if isinstance(expression_ast, exp.Literal) and expression_ast.is_int:
            ordinal = int(expression_ast.this)
            if ordinal < 1 or ordinal > len(projections):
                _binding_error("ORDER BY position is outside the select list.", expression_ast)
            projection = projections[ordinal - 1]
            expression = Column(projection.name, "", projection.expression.type_info)
        elif (
            projections
            and isinstance(expression_ast, exp.Column)
            and not expression_ast.table
            and expression_ast.name.casefold() in aliases
        ):
            projection = next(
                item
                for item in projections
                if item.name.casefold() == expression_ast.name.casefold()
            )
            expression = Column(projection.name, "", projection.expression.type_info)
        else:
            expression = self._bind_expression(expression_ast, scope, aliases=aliases)
        return SortExpression(
            expression,
            ascending=not bool(ordered.args.get("desc")),
            nulls_first=bool(ordered.args.get("nulls_first")),
        )

    @staticmethod
    def _bind_limit(limit: exp.Limit) -> int:
        value = limit.expression
        if not isinstance(value, exp.Literal) or not value.is_int:
            _binding_error("LIMIT must be a non-negative integer literal.", limit)
        result = int(value.this)
        if result < 0:
            _binding_error("LIMIT must be a non-negative integer literal.", limit)
        return result

    def _bind_expression(
        self,
        expression: exp.Expression,
        scope: _Scope,
        *,
        aliases: Mapping[str, Expression] | None = None,
    ) -> Expression:
        if isinstance(expression, exp.Paren):
            return self._bind_expression(expression.this, scope, aliases=aliases)
        if isinstance(expression, exp.Column):
            if not expression.table and aliases and expression.name.casefold() in aliases:
                return aliases[expression.name.casefold()]
            return scope.resolve(expression)
        if isinstance(expression, exp.Null):
            return Literal(None, TypeInfo(DataType.NULL))
        if isinstance(expression, exp.Boolean):
            boolean_value = bool(expression.this)
            return Literal(boolean_value, literal_type(boolean_value))
        if isinstance(expression, exp.Literal):
            if expression.is_string:
                literal_value: str | int | float = expression.this
            elif expression.is_int:
                literal_value = int(expression.this)
            else:
                literal_value = float(expression.this)
            return Literal(literal_value, literal_type(literal_value))
        if isinstance(expression, exp.Cast):
            source = self._bind_expression(expression.this, scope, aliases=aliases)
            target = self._parse_data_type(expression.to, expression)
            return Cast(source, TypeInfo(target, source.type_info.nullable), implicit=False)
        if isinstance(expression, exp.Neg):
            operand = self._bind_expression(expression.this, scope, aliases=aliases)
            self._require_numeric(operand, expression)
            return Unary(UnaryOperator.NEGATE, operand, operand.type_info)
        if isinstance(expression, exp.Not):
            operand = self._bind_expression(expression.this, scope, aliases=aliases)
            self._require_boolean(operand, expression)
            return Unary(
                UnaryOperator.NOT,
                operand,
                TypeInfo(DataType.BOOLEAN, operand.type_info.nullable),
            )

        binary_operators: dict[type[exp.Expression], BinaryOperator] = {
            exp.Add: BinaryOperator.ADD,
            exp.Sub: BinaryOperator.SUBTRACT,
            exp.Mul: BinaryOperator.MULTIPLY,
            exp.Div: BinaryOperator.DIVIDE,
            exp.Mod: BinaryOperator.MODULO,
            exp.EQ: BinaryOperator.EQUAL,
            exp.NEQ: BinaryOperator.NOT_EQUAL,
            exp.LT: BinaryOperator.LESS_THAN,
            exp.LTE: BinaryOperator.LESS_THAN_OR_EQUAL,
            exp.GT: BinaryOperator.GREATER_THAN,
            exp.GTE: BinaryOperator.GREATER_THAN_OR_EQUAL,
            exp.And: BinaryOperator.AND,
            exp.Or: BinaryOperator.OR,
        }
        for expression_type, operator in binary_operators.items():
            if isinstance(expression, expression_type):
                return self._bind_binary(cast(exp.Binary, expression), operator, scope, aliases)

        if isinstance(expression, exp.Is):
            if not isinstance(expression.expression, exp.Null):
                _binding_error("Only IS NULL and IS NOT NULL are supported.", expression)
            return IsNull(self._bind_expression(expression.this, scope, aliases=aliases))
        if isinstance(expression, exp.In):
            if expression.args.get("query"):
                _binding_error("IN subqueries are not supported.", expression)
            in_value = self._bind_expression(expression.this, scope, aliases=aliases)
            options = tuple(
                self._bind_expression(item, scope, aliases=aliases)
                for item in expression.expressions
            )
            if not options:
                _binding_error("IN requires at least one value.", expression)
            coerced_options: list[Expression] = []
            target_type = in_value.type_info
            for option in options:
                target_type = common_type(target_type, option.type_info)
            if in_value.type_info.data_type is not target_type.data_type:
                in_value = Cast(in_value, target_type)
            for option in options:
                coerced_options.append(
                    option
                    if option.type_info.data_type is target_type.data_type
                    else Cast(option, target_type)
                )
            return InList(in_value, tuple(coerced_options))
        if isinstance(expression, exp.Between):
            between_value = self._bind_expression(expression.this, scope, aliases=aliases)
            low = self._bind_expression(expression.args["low"], scope, aliases=aliases)
            high = self._bind_expression(expression.args["high"], scope, aliases=aliases)
            between_value, low, comparison_type = coerce_pair(between_value, low)
            if high.type_info.data_type is not comparison_type.data_type:
                high = Cast(high, comparison_type)
            return Between(between_value, low, high)
        if isinstance(expression, exp.Like):
            like_value = self._bind_expression(expression.this, scope, aliases=aliases)
            pattern = self._bind_expression(expression.expression, scope, aliases=aliases)
            self._require_string(like_value, expression.this)
            self._require_string(pattern, expression.expression)
            return Like(like_value, pattern)
        if isinstance(expression, exp.Case):
            return self._bind_case(expression, scope, aliases)
        if isinstance(expression, exp.Window):
            return self._bind_window(expression, scope, aliases)
        if isinstance(expression, exp.Count | exp.Sum | exp.Avg | exp.Min | exp.Max):
            return self._bind_aggregate(expression, scope, aliases)
        if isinstance(expression, exp.Func):
            return self._bind_scalar_function(expression, scope, aliases)
        _binding_error(
            f"Expression '{type(expression).__name__}' is not supported.",
            expression,
            expression_type=type(expression).__name__,
        )

    def _bind_binary(
        self,
        expression: exp.Binary,
        operator: BinaryOperator,
        scope: _Scope,
        aliases: Mapping[str, Expression] | None,
    ) -> Expression:
        left = self._bind_expression(expression.this, scope, aliases=aliases)
        right = self._bind_expression(expression.expression, scope, aliases=aliases)
        if operator in {BinaryOperator.AND, BinaryOperator.OR}:
            self._require_boolean(left, expression.this)
            self._require_boolean(right, expression.expression)
            return Binary(
                operator,
                left,
                right,
                TypeInfo(
                    DataType.BOOLEAN,
                    left.type_info.nullable or right.type_info.nullable,
                ),
            )
        left, right, common = coerce_pair(left, right)
        if operator in {
            BinaryOperator.ADD,
            BinaryOperator.SUBTRACT,
            BinaryOperator.MULTIPLY,
            BinaryOperator.DIVIDE,
            BinaryOperator.MODULO,
        }:
            result = numeric_result_type(
                left.type_info,
                right.type_info,
                division=operator is BinaryOperator.DIVIDE,
            )
            if result.data_type is not common.data_type:
                left = Cast(left, result)
                right = Cast(right, result)
            return Binary(operator, left, right, result)
        return Binary(
            operator,
            left,
            right,
            TypeInfo(DataType.BOOLEAN, nullable=common.nullable),
        )

    def _bind_case(
        self,
        expression: exp.Case,
        scope: _Scope,
        aliases: Mapping[str, Expression] | None,
    ) -> Expression:
        if expression.this is not None:
            _binding_error("Only searched CASE expressions are supported.", expression)
        branches: list[tuple[Expression, Expression]] = []
        result_type = TypeInfo(DataType.NULL)
        for item in expression.args.get("ifs") or []:
            condition = self._bind_expression(item.this, scope, aliases=aliases)
            self._require_boolean(condition, item.this)
            result = self._bind_expression(item.args["true"], scope, aliases=aliases)
            result_type = common_type(result_type, result.type_info)
            branches.append((condition, result))
        default_ast = expression.args.get("default")
        default = (
            self._bind_expression(default_ast, scope, aliases=aliases)
            if default_ast is not None
            else Literal(None, TypeInfo(DataType.NULL))
        )
        result_type = common_type(result_type, default.type_info)
        coerced = tuple(
            (
                condition,
                result
                if result.type_info.data_type is result_type.data_type
                else Cast(result, result_type),
            )
            for condition, result in branches
        )
        if default.type_info.data_type is not result_type.data_type:
            default = Cast(default, result_type)
        return Case(coerced, default, result_type)

    def _bind_aggregate(
        self,
        expression: exp.Expression,
        scope: _Scope,
        aliases: Mapping[str, Expression] | None,
    ) -> AggregateFunction:
        aggregate_names: dict[type[exp.Expression], str] = {
            exp.Count: "count",
            exp.Sum: "sum",
            exp.Avg: "avg",
            exp.Min: "min",
            exp.Max: "max",
        }
        name = aggregate_names[type(expression)]
        argument_ast = expression.this
        distinct = isinstance(argument_ast, exp.Distinct)
        if distinct:
            argument_nodes = list(argument_ast.expressions)
        elif argument_ast is None or isinstance(argument_ast, exp.Star):
            argument_nodes = []
        else:
            argument_nodes = [argument_ast]
        if name != "count" and not argument_nodes:
            _binding_error(f"{name.upper()} requires one argument.", expression)
        if len(argument_nodes) > 1:
            _binding_error("Aggregate functions accept one argument.", expression)
        arguments = tuple(
            self._bind_expression(item, scope, aliases=aliases) for item in argument_nodes
        )
        for argument_node, argument in zip(argument_nodes, arguments, strict=True):
            if any(
                isinstance(item, AggregateFunction | WindowFunction)
                for item in _walk_bound(argument)
            ):
                _binding_error("Aggregate functions cannot be nested.", argument_node)
        if name in {"sum", "avg"}:
            self._require_numeric(arguments[0], expression)
        if name == "count":
            result_type = TypeInfo(DataType.INT64, nullable=False)
        elif name == "avg":
            result_type = TypeInfo(DataType.FLOAT64, nullable=True)
        else:
            result_type = TypeInfo(arguments[0].type_info.data_type, nullable=True)
        return AggregateFunction(name, arguments, result_type, distinct)

    def _bind_scalar_function(
        self,
        expression: exp.Func,
        scope: _Scope,
        aliases: Mapping[str, Expression] | None,
    ) -> ScalarFunction:
        name = expression.key.lower()
        allowed = {
            "lower",
            "upper",
            "length",
            "abs",
            "coalesce",
            "concat",
            "substring",
            "round",
        }
        if name not in allowed:
            _binding_error(f"Function '{name}' is not supported.", expression, function=name)
        argument_nodes = list(expression.iter_expressions())
        arguments = tuple(
            self._bind_expression(item, scope, aliases=aliases) for item in argument_nodes
        )
        if name in {"lower", "upper", "length", "abs"} and len(arguments) != 1:
            _binding_error(f"Function '{name}' expects one argument.", expression)
        if name == "substring" and len(arguments) not in {2, 3}:
            _binding_error("Function 'substring' expects two or three arguments.", expression)
        if name == "round" and len(arguments) not in {1, 2}:
            _binding_error("Function 'round' expects one or two arguments.", expression)
        if name in {"lower", "upper", "length", "concat"}:
            for argument in arguments:
                self._require_string(argument, expression)
        if name == "substring":
            self._require_string(arguments[0], expression)
            for argument in arguments[1:]:
                self._require_numeric(argument, expression)
        if name in {"abs", "round"}:
            self._require_numeric(arguments[0], expression)
            if name == "round" and len(arguments) == 2:
                self._require_numeric(arguments[1], expression)
            result_type = arguments[0].type_info
        elif name == "length":
            result_type = TypeInfo(
                DataType.INT64,
                nullable=any(item.type_info.nullable for item in arguments),
            )
        elif name == "coalesce":
            if not arguments:
                _binding_error("COALESCE requires at least one argument.", expression)
            result_type = arguments[0].type_info
            for argument in arguments[1:]:
                result_type = common_type(result_type, argument.type_info)
            arguments = tuple(
                argument
                if argument.type_info.data_type is result_type.data_type
                else Cast(argument, result_type)
                for argument in arguments
            )
        else:
            result_type = TypeInfo(
                DataType.STRING,
                nullable=any(item.type_info.nullable for item in arguments),
            )
        return ScalarFunction(name, arguments, result_type)

    def _bind_window(
        self,
        expression: exp.Window,
        scope: _Scope,
        aliases: Mapping[str, Expression] | None,
    ) -> WindowFunction:
        function_ast = expression.this
        if isinstance(function_ast, exp.RowNumber | exp.Rank | exp.DenseRank):
            ranking_names = {
                exp.RowNumber: "row_number",
                exp.Rank: "rank",
                exp.DenseRank: "dense_rank",
            }
            function: Expression = ScalarFunction(
                ranking_names[type(function_ast)],
                (),
                TypeInfo(DataType.INT64, nullable=False),
            )
        elif isinstance(function_ast, exp.Count | exp.Sum | exp.Avg | exp.Min | exp.Max):
            function = self._bind_aggregate(function_ast, scope, aliases)
        else:
            _binding_error("Unsupported window function.", function_ast)
        partition_by = tuple(
            self._bind_expression(item, scope, aliases=aliases)
            for item in expression.args.get("partition_by") or []
        )
        order = expression.args.get("order")
        order_by = (
            tuple(self._bind_ordered(item, scope, aliases or {}, ()) for item in order.expressions)
            if order is not None
            else ()
        )
        specification = expression.args.get("spec")
        frame = self._bind_window_frame(specification) if specification is not None else None
        return WindowFunction(function, partition_by, order_by, frame)

    @staticmethod
    def _bind_window_frame(specification: exp.WindowSpec) -> WindowFrame:
        kind = str(specification.args.get("kind") or "").upper()
        if kind != "ROWS":
            _binding_error("Only ROWS window frames are supported.", specification)

        def boundary(value: object, side: object) -> str:
            if isinstance(value, exp.Literal) and value.is_int:
                return f"{value.this} {side}"
            if isinstance(value, str):
                return f"{value} {side or ''}".strip()
            _binding_error("Window frame boundaries must be integer ROWS bounds.", specification)

        start = boundary(specification.args.get("start"), specification.args.get("start_side"))
        end = boundary(specification.args.get("end"), specification.args.get("end_side"))
        return WindowFrame(kind, start, end)

    @staticmethod
    def _parse_data_type(data_type: exp.DataType, expression: exp.Expression) -> DataType:
        mapping = {
            exp.DataType.Type.BOOLEAN: DataType.BOOLEAN,
            exp.DataType.Type.INT: DataType.INT32,
            exp.DataType.Type.BIGINT: DataType.INT64,
            exp.DataType.Type.FLOAT: DataType.FLOAT32,
            exp.DataType.Type.DOUBLE: DataType.FLOAT64,
            exp.DataType.Type.DECIMAL: DataType.DECIMAL,
            exp.DataType.Type.VARCHAR: DataType.STRING,
            exp.DataType.Type.TEXT: DataType.STRING,
            exp.DataType.Type.BINARY: DataType.BINARY,
            exp.DataType.Type.DATE: DataType.DATE,
            exp.DataType.Type.TIMESTAMP: DataType.TIMESTAMP,
        }
        result = mapping.get(data_type.this)
        if result is None:
            _binding_error(f"Type '{data_type.sql()}' is not supported.", expression)
        return result

    @staticmethod
    def _require_boolean(expression: Expression, source: exp.Expression) -> None:
        if expression.type_info.data_type not in {DataType.BOOLEAN, DataType.NULL}:
            _binding_error("A boolean expression is required.", source)

    @staticmethod
    def _require_numeric(expression: Expression, source: exp.Expression) -> None:
        if expression.type_info.data_type not in NUMERIC_TYPES:
            _binding_error("A numeric expression is required.", source)

    @staticmethod
    def _require_string(expression: Expression, source: exp.Expression) -> None:
        if expression.type_info.data_type is not DataType.STRING:
            _binding_error("A string expression is required.", source)

    @staticmethod
    def _reject_aggregate_or_window(
        expression: Expression,
        source: exp.Expression,
    ) -> None:
        if any(
            isinstance(item, AggregateFunction | WindowFunction) for item in _walk_bound(expression)
        ):
            _binding_error("Aggregate and window functions are not allowed here.", source)


def parse_date_literal(value: str, target: DataType) -> date | datetime:
    """Public helper used by execution code when applying explicit date casts."""

    if target is DataType.DATE:
        return date.fromisoformat(value)
    if target is DataType.TIMESTAMP:
        return datetime.fromisoformat(value)
    raise ValueError(f"{target.value} is not a date-like type")

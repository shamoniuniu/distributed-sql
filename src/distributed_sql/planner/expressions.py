"""Bound SQL expressions and scalar evaluation semantics."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import cast

from distributed_sql.common.protocol import DataType
from distributed_sql.planner.types import NUMERIC_TYPES, TypeInfo, common_type

type SQLValue = bool | int | float | Decimal | str | bytes | date | datetime | None
type Row = Mapping[str, SQLValue]


class Expression(ABC):
    @property
    @abstractmethod
    def type_info(self) -> TypeInfo:
        raise NotImplementedError

    @abstractmethod
    def evaluate(self, row: Row) -> SQLValue:
        raise NotImplementedError

    @abstractmethod
    def sql(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class Literal(Expression):
    value: SQLValue
    result_type: TypeInfo

    @property
    def type_info(self) -> TypeInfo:
        return self.result_type

    def evaluate(self, row: Row) -> SQLValue:
        del row
        return self.value

    def sql(self) -> str:
        if self.value is None:
            return "NULL"
        if isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        if isinstance(self.value, str):
            return "'" + self.value.replace("'", "''") + "'"
        return str(self.value)


@dataclass(frozen=True, slots=True)
class Column(Expression):
    name: str
    source: str
    result_type: TypeInfo

    @property
    def qualified_name(self) -> str:
        return f"{self.source}.{self.name}" if self.source else self.name

    @property
    def type_info(self) -> TypeInfo:
        return self.result_type

    def evaluate(self, row: Row) -> SQLValue:
        if self.qualified_name in row:
            return row[self.qualified_name]
        return row[self.name]

    def sql(self) -> str:
        return self.qualified_name


@dataclass(frozen=True, slots=True)
class Cast(Expression):
    expression: Expression
    target: TypeInfo
    implicit: bool = True

    @property
    def type_info(self) -> TypeInfo:
        return TypeInfo(self.target.data_type, self.expression.type_info.nullable)

    def evaluate(self, row: Row) -> SQLValue:
        value = self.expression.evaluate(row)
        if value is None:
            return None
        target = self.target.data_type
        if target in {DataType.INT32, DataType.INT64}:
            return int(cast(int | float | Decimal | str, value))
        if target in {DataType.FLOAT32, DataType.FLOAT64}:
            return float(cast(int | float | Decimal | str, value))
        if target is DataType.DECIMAL:
            return Decimal(str(value))
        if target is DataType.STRING:
            return str(value)
        if target is DataType.BOOLEAN:
            return bool(value)
        if target is DataType.DATE and isinstance(value, str):
            return date.fromisoformat(value)
        if target is DataType.TIMESTAMP and isinstance(value, str):
            return datetime.fromisoformat(value)
        if target is DataType.TIMESTAMP and isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        return value

    def sql(self) -> str:
        return f"CAST({self.expression.sql()} AS {self.target.data_type.value.upper()})"


class UnaryOperator(StrEnum):
    NOT = "NOT"
    NEGATE = "-"


@dataclass(frozen=True, slots=True)
class Unary(Expression):
    operator: UnaryOperator
    expression: Expression
    result_type: TypeInfo

    @property
    def type_info(self) -> TypeInfo:
        return self.result_type

    def evaluate(self, row: Row) -> SQLValue:
        value = self.expression.evaluate(row)
        if value is None:
            return None
        if self.operator is UnaryOperator.NOT:
            return not cast(bool, value)
        return -cast(int | float | Decimal, value)

    def sql(self) -> str:
        return f"{self.operator.value} {self.expression.sql()}"


class BinaryOperator(StrEnum):
    ADD = "+"
    SUBTRACT = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    MODULO = "%"
    EQUAL = "="
    NOT_EQUAL = "<>"
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    AND = "AND"
    OR = "OR"


def sql_and(left: bool | None, right: bool | None) -> bool | None:
    if left is False or right is False:
        return False
    if left is None or right is None:
        return None
    return True


def sql_or(left: bool | None, right: bool | None) -> bool | None:
    if left is True or right is True:
        return True
    if left is None or right is None:
        return None
    return False


@dataclass(frozen=True, slots=True)
class Binary(Expression):
    operator: BinaryOperator
    left: Expression
    right: Expression
    result_type: TypeInfo

    @property
    def type_info(self) -> TypeInfo:
        return self.result_type

    def evaluate(self, row: Row) -> SQLValue:
        left = self.left.evaluate(row)
        right = self.right.evaluate(row)
        if self.operator is BinaryOperator.AND:
            return sql_and(cast(bool | None, left), cast(bool | None, right))
        if self.operator is BinaryOperator.OR:
            return sql_or(cast(bool | None, left), cast(bool | None, right))
        if left is None or right is None:
            return None
        if self.operator is BinaryOperator.ADD:
            return _numeric_operation(left, right, self.operator)
        if self.operator is BinaryOperator.SUBTRACT:
            return _numeric_operation(left, right, self.operator)
        if self.operator is BinaryOperator.MULTIPLY:
            return _numeric_operation(left, right, self.operator)
        if self.operator is BinaryOperator.DIVIDE:
            return _numeric_operation(left, right, self.operator)
        if self.operator is BinaryOperator.MODULO:
            return _numeric_operation(left, right, self.operator)
        if self.operator is BinaryOperator.EQUAL:
            return left == right
        if self.operator is BinaryOperator.NOT_EQUAL:
            return left != right
        if self.operator is BinaryOperator.LESS_THAN:
            return left < right  # type: ignore[operator]
        if self.operator is BinaryOperator.LESS_THAN_OR_EQUAL:
            return left <= right  # type: ignore[operator]
        if self.operator is BinaryOperator.GREATER_THAN:
            return left > right  # type: ignore[operator]
        return left >= right  # type: ignore[operator]

    def sql(self) -> str:
        return f"({self.left.sql()} {self.operator.value} {self.right.sql()})"


def _numeric_operation(
    left: SQLValue,
    right: SQLValue,
    operator: BinaryOperator,
) -> int | float | Decimal:
    if isinstance(left, Decimal) or isinstance(right, Decimal):
        left_number: int | float | Decimal = Decimal(str(left))
        right_number: int | float | Decimal = Decimal(str(right))
    elif isinstance(left, float) or isinstance(right, float):
        left_number = float(cast(int | float, left))
        right_number = float(cast(int | float, right))
    else:
        left_number = cast(int, left)
        right_number = cast(int, right)
    if operator is BinaryOperator.ADD:
        return left_number + right_number  # type: ignore[operator]
    if operator is BinaryOperator.SUBTRACT:
        return left_number - right_number  # type: ignore[operator]
    if operator is BinaryOperator.MULTIPLY:
        return left_number * right_number  # type: ignore[operator]
    if operator is BinaryOperator.DIVIDE:
        return left_number / right_number  # type: ignore[operator]
    return left_number % right_number  # type: ignore[operator]


@dataclass(frozen=True, slots=True)
class IsNull(Expression):
    expression: Expression
    negated: bool = False

    @property
    def type_info(self) -> TypeInfo:
        return TypeInfo(DataType.BOOLEAN, nullable=False)

    def evaluate(self, row: Row) -> SQLValue:
        result = self.expression.evaluate(row) is None
        return not result if self.negated else result

    def sql(self) -> str:
        suffix = "IS NOT NULL" if self.negated else "IS NULL"
        return f"({self.expression.sql()} {suffix})"


@dataclass(frozen=True, slots=True)
class InList(Expression):
    expression: Expression
    options: tuple[Expression, ...]
    negated: bool = False

    @property
    def type_info(self) -> TypeInfo:
        nullable = self.expression.type_info.nullable or any(
            option.type_info.nullable for option in self.options
        )
        return TypeInfo(DataType.BOOLEAN, nullable=nullable)

    def evaluate(self, row: Row) -> SQLValue:
        value = self.expression.evaluate(row)
        if value is None:
            return None
        saw_null = False
        for option in self.options:
            candidate = option.evaluate(row)
            if candidate is None:
                saw_null = True
            elif candidate == value:
                return not self.negated
        result: bool | None = None if saw_null else False
        if self.negated and result is not None:
            return not result
        return result

    def sql(self) -> str:
        operator = "NOT IN" if self.negated else "IN"
        values = ", ".join(option.sql() for option in self.options)
        return f"({self.expression.sql()} {operator} ({values}))"


@dataclass(frozen=True, slots=True)
class Between(Expression):
    expression: Expression
    low: Expression
    high: Expression
    negated: bool = False

    @property
    def type_info(self) -> TypeInfo:
        nullable = any(item.type_info.nullable for item in (self.expression, self.low, self.high))
        return TypeInfo(DataType.BOOLEAN, nullable=nullable)

    def evaluate(self, row: Row) -> SQLValue:
        value = self.expression.evaluate(row)
        low = self.low.evaluate(row)
        high = self.high.evaluate(row)
        if value is None or low is None or high is None:
            return None
        result = low <= value <= high  # type: ignore[operator]
        return not result if self.negated else result

    def sql(self) -> str:
        operator = "NOT BETWEEN" if self.negated else "BETWEEN"
        return f"({self.expression.sql()} {operator} {self.low.sql()} AND {self.high.sql()})"


@dataclass(frozen=True, slots=True)
class Like(Expression):
    expression: Expression
    pattern: Expression
    negated: bool = False

    @property
    def type_info(self) -> TypeInfo:
        return TypeInfo(
            DataType.BOOLEAN,
            nullable=self.expression.type_info.nullable or self.pattern.type_info.nullable,
        )

    def evaluate(self, row: Row) -> SQLValue:
        value = self.expression.evaluate(row)
        pattern = self.pattern.evaluate(row)
        if value is None or pattern is None:
            return None
        regex = re.escape(str(pattern)).replace("%", ".*").replace("_", ".")
        result = re.fullmatch(regex, str(value), flags=re.DOTALL) is not None
        return not result if self.negated else result

    def sql(self) -> str:
        operator = "NOT LIKE" if self.negated else "LIKE"
        return f"({self.expression.sql()} {operator} {self.pattern.sql()})"


@dataclass(frozen=True, slots=True)
class Case(Expression):
    branches: tuple[tuple[Expression, Expression], ...]
    default: Expression
    result_type: TypeInfo

    @property
    def type_info(self) -> TypeInfo:
        return self.result_type

    def evaluate(self, row: Row) -> SQLValue:
        for condition, result in self.branches:
            if condition.evaluate(row) is True:
                return result.evaluate(row)
        return self.default.evaluate(row)

    def sql(self) -> str:
        branches = " ".join(
            f"WHEN {condition.sql()} THEN {result.sql()}" for condition, result in self.branches
        )
        return f"CASE {branches} ELSE {self.default.sql()} END"


@dataclass(frozen=True, slots=True)
class ScalarFunction(Expression):
    name: str
    arguments: tuple[Expression, ...]
    result_type: TypeInfo

    @property
    def type_info(self) -> TypeInfo:
        return self.result_type

    def evaluate(self, row: Row) -> SQLValue:
        values = [argument.evaluate(row) for argument in self.arguments]
        name = self.name.lower()
        if name == "coalesce":
            return next((value for value in values if value is not None), None)
        if any(value is None for value in values):
            return None
        if name == "lower":
            return str(values[0]).lower()
        if name == "upper":
            return str(values[0]).upper()
        if name == "length":
            return len(cast(str | bytes, values[0]))
        if name == "abs":
            return cast(SQLValue, abs(cast(int | float | Decimal, values[0])))
        if name == "concat":
            return "".join(str(value) for value in values)
        if name == "substring":
            text = str(values[0])
            start = cast(int, values[1])
            begin = max(start - 1, 0)
            if len(values) == 2:
                return text[begin:]
            return text[begin : begin + cast(int, values[2])]
        if name == "round":
            digits = cast(int, values[1]) if len(values) == 2 else 0
            return cast(SQLValue, round(cast(int | float | Decimal, values[0]), digits))
        raise ValueError(f"Function {self.name} cannot be evaluated as a scalar expression.")

    def sql(self) -> str:
        return f"{self.name.upper()}({', '.join(arg.sql() for arg in self.arguments)})"


@dataclass(frozen=True, slots=True)
class AggregateFunction(Expression):
    name: str
    arguments: tuple[Expression, ...]
    result_type: TypeInfo
    distinct: bool = False

    @property
    def type_info(self) -> TypeInfo:
        return self.result_type

    def evaluate(self, row: Row) -> SQLValue:
        try:
            return row[self.sql()]
        except KeyError as exc:
            raise ValueError("Aggregate expressions require an aggregate operator.") from exc

    def sql(self) -> str:
        distinct = "DISTINCT " if self.distinct else ""
        arguments = "*" if not self.arguments else ", ".join(arg.sql() for arg in self.arguments)
        return f"{self.name.upper()}({distinct}{arguments})"


@dataclass(frozen=True, slots=True)
class SortExpression:
    expression: Expression
    ascending: bool = True
    nulls_first: bool = False

    def sql(self) -> str:
        direction = "ASC" if self.ascending else "DESC"
        nulls = "NULLS FIRST" if self.nulls_first else "NULLS LAST"
        return f"{self.expression.sql()} {direction} {nulls}"


@dataclass(frozen=True, slots=True)
class WindowFrame:
    kind: str
    start: str
    end: str


@dataclass(frozen=True, slots=True)
class WindowFunction(Expression):
    function: Expression
    partition_by: tuple[Expression, ...]
    order_by: tuple[SortExpression, ...]
    frame: WindowFrame | None

    @property
    def type_info(self) -> TypeInfo:
        return self.function.type_info

    def evaluate(self, row: Row) -> SQLValue:
        try:
            return row[self.sql()]
        except KeyError as exc:
            raise ValueError("Window expressions require a window operator.") from exc

    def sql(self) -> str:
        clauses: list[str] = []
        if self.partition_by:
            clauses.append("PARTITION BY " + ", ".join(expr.sql() for expr in self.partition_by))
        if self.order_by:
            clauses.append("ORDER BY " + ", ".join(expr.sql() for expr in self.order_by))
        if self.frame:
            clauses.append(f"{self.frame.kind} BETWEEN {self.frame.start} AND {self.frame.end}")
        return f"{self.function.sql()} OVER ({' '.join(clauses)})"


def coerce_pair(left: Expression, right: Expression) -> tuple[Expression, Expression, TypeInfo]:
    result = common_type(left.type_info, right.type_info)
    if left.type_info.data_type is not result.data_type:
        left = Cast(left, result)
    if right.type_info.data_type is not result.data_type:
        right = Cast(right, result)
    return left, right, result


def numeric_result_type(left: TypeInfo, right: TypeInfo, *, division: bool = False) -> TypeInfo:
    result = common_type(left, right)
    if result.data_type not in NUMERIC_TYPES:
        raise TypeError("Arithmetic operators require numeric operands.")
    if division and result.data_type in {DataType.INT32, DataType.INT64}:
        return TypeInfo(DataType.FLOAT64, nullable=result.nullable)
    return result

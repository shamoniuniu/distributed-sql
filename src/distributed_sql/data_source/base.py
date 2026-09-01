"""Common contracts for file and table scans."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.expressions import (
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNull,
    Or,
)

from distributed_sql.catalog.models import CatalogTable, TableFormat

type PredicateValue = bool | int | float | str | bytes


class PredicateOperator(StrEnum):
    EQUAL = "eq"
    NOT_EQUAL = "ne"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL = "le"
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL = "ge"
    IN = "in"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class ScanPredicate(ABC):
    @property
    @abstractmethod
    def columns(self) -> frozenset[str]:
        """Columns required to evaluate this predicate."""

    @abstractmethod
    def to_arrow(self) -> pc.Expression:
        """Convert to a PyArrow dataset expression."""

    @abstractmethod
    def to_iceberg(self) -> BooleanExpression:
        """Convert to a PyIceberg row-filter expression."""


@dataclass(frozen=True, slots=True)
class Predicate(ScanPredicate):
    column: str
    operator: PredicateOperator
    value: PredicateValue | tuple[PredicateValue, ...] | None = None

    def __post_init__(self) -> None:
        set_operator = self.operator in {PredicateOperator.IN, PredicateOperator.NOT_IN}
        null_operator = self.operator in {
            PredicateOperator.IS_NULL,
            PredicateOperator.IS_NOT_NULL,
        }
        if set_operator and not isinstance(self.value, tuple):
            raise ValueError(f"{self.operator.value} requires a tuple value")
        if null_operator and self.value is not None:
            raise ValueError(f"{self.operator.value} does not accept a value")
        if not set_operator and not null_operator and self.value is None:
            raise ValueError(f"{self.operator.value} requires a value")

    @property
    def columns(self) -> frozenset[str]:
        return frozenset({self.column})

    def to_arrow(self) -> pc.Expression:
        column = pc.field(self.column)
        if self.operator is PredicateOperator.EQUAL:
            return column == self.value
        if self.operator is PredicateOperator.NOT_EQUAL:
            return column != self.value
        if self.operator is PredicateOperator.LESS_THAN:
            return column < self.value
        if self.operator is PredicateOperator.LESS_THAN_OR_EQUAL:
            return column <= self.value
        if self.operator is PredicateOperator.GREATER_THAN:
            return column > self.value
        if self.operator is PredicateOperator.GREATER_THAN_OR_EQUAL:
            return column >= self.value
        if self.operator is PredicateOperator.IN:
            return column.isin(self.value)
        if self.operator is PredicateOperator.NOT_IN:
            return ~column.isin(self.value)
        if self.operator is PredicateOperator.IS_NULL:
            return column.is_null(nan_is_null=False)
        return column.is_valid()

    def to_iceberg(self) -> BooleanExpression:
        if self.operator is PredicateOperator.EQUAL:
            return EqualTo(self.column, self._literal_value())
        if self.operator is PredicateOperator.NOT_EQUAL:
            return NotEqualTo(self.column, self._literal_value())
        if self.operator is PredicateOperator.LESS_THAN:
            return LessThan(self.column, self._literal_value())
        if self.operator is PredicateOperator.LESS_THAN_OR_EQUAL:
            return LessThanOrEqual(self.column, self._literal_value())
        if self.operator is PredicateOperator.GREATER_THAN:
            return GreaterThan(self.column, self._literal_value())
        if self.operator is PredicateOperator.GREATER_THAN_OR_EQUAL:
            return GreaterThanOrEqual(self.column, self._literal_value())
        if self.operator is PredicateOperator.IN:
            return In(self.column, self._set_values())
        if self.operator is PredicateOperator.NOT_IN:
            return NotIn(self.column, self._set_values())
        if self.operator is PredicateOperator.IS_NULL:
            return IsNull(self.column)
        return NotNull(self.column)

    def _set_values(self) -> Any:
        if not isinstance(self.value, tuple):
            raise AssertionError("set predicates require tuple values")
        return self.value

    def _literal_value(self) -> Any:
        if self.value is None or isinstance(self.value, tuple):
            raise AssertionError("comparison predicates require scalar values")
        return self.value


@dataclass(frozen=True, slots=True)
class CompoundPredicate(ScanPredicate):
    operator: str
    predicates: tuple[ScanPredicate, ...]

    def __post_init__(self) -> None:
        if self.operator not in {"and", "or", "not"}:
            raise ValueError(f"Unsupported compound predicate: {self.operator}")
        expected = 1 if self.operator == "not" else 2
        if len(self.predicates) != expected:
            raise ValueError(f"{self.operator} requires {expected} predicate(s)")

    @property
    def columns(self) -> frozenset[str]:
        return frozenset().union(*(predicate.columns for predicate in self.predicates))

    def to_arrow(self) -> pc.Expression:
        first = self.predicates[0].to_arrow()
        if self.operator == "not":
            return ~first
        second = self.predicates[1].to_arrow()
        return first & second if self.operator == "and" else first | second

    def to_iceberg(self) -> BooleanExpression:
        first = self.predicates[0].to_iceberg()
        if self.operator == "not":
            return Not(first)
        second = self.predicates[1].to_iceberg()
        return And(first, second) if self.operator == "and" else Or(first, second)


@dataclass(frozen=True, slots=True)
class FileScanTask:
    location: str
    format: TableFormat
    start: int = 0
    length: int | None = None
    record_count: int | None = None
    partition_values: dict[str, Any] = field(default_factory=dict)
    delete_files: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ScanRequest:
    projection: tuple[str, ...] | None = None
    predicate: ScanPredicate | None = None
    batch_size: int = 65_536
    file_tasks: tuple[FileScanTask, ...] | None = None

    def __post_init__(self) -> None:
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.projection is not None and len(self.projection) != len(set(self.projection)):
            raise ValueError("projection columns must be unique")


@dataclass(frozen=True, slots=True)
class ScanPlan:
    schema: pa.Schema
    file_tasks: tuple[FileScanTask, ...]
    snapshot_id: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class DataSource(ABC):
    @abstractmethod
    def plan_scan(self, table: CatalogTable, request: ScanRequest) -> ScanPlan:
        """Resolve metadata into immutable file tasks."""

    @abstractmethod
    def scan(self, table: CatalogTable, request: ScanRequest) -> Iterator[pa.RecordBatch]:
        """Read a table as uniformly sized Arrow record batches."""

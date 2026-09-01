"""SQL type inference and implicit coercion rules."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import DataType

type ScalarValue = bool | int | float | Decimal | str | bytes | None


@dataclass(frozen=True, slots=True)
class TypeInfo:
    data_type: DataType
    nullable: bool = True


_NUMERIC_PRECEDENCE = {
    DataType.INT32: 0,
    DataType.INT64: 1,
    DataType.FLOAT32: 2,
    DataType.FLOAT64: 3,
    DataType.DECIMAL: 4,
}
NUMERIC_TYPES = frozenset(_NUMERIC_PRECEDENCE)


def literal_type(value: ScalarValue) -> TypeInfo:
    if value is None:
        return TypeInfo(DataType.NULL)
    if isinstance(value, bool):
        return TypeInfo(DataType.BOOLEAN, nullable=False)
    if isinstance(value, int):
        data_type = DataType.INT32 if -(2**31) <= value < 2**31 else DataType.INT64
        return TypeInfo(data_type, nullable=False)
    if isinstance(value, float):
        return TypeInfo(DataType.FLOAT64, nullable=False)
    if isinstance(value, Decimal):
        return TypeInfo(DataType.DECIMAL, nullable=False)
    if isinstance(value, bytes):
        return TypeInfo(DataType.BINARY, nullable=False)
    return TypeInfo(DataType.STRING, nullable=False)


def common_type(left: TypeInfo, right: TypeInfo) -> TypeInfo:
    """Return the common type used by binary expressions and CASE."""

    nullable = left.nullable or right.nullable
    if left.data_type is DataType.NULL:
        return TypeInfo(right.data_type, nullable=True)
    if right.data_type is DataType.NULL:
        return TypeInfo(left.data_type, nullable=True)
    if left.data_type is right.data_type:
        return TypeInfo(left.data_type, nullable=nullable)
    if left.data_type in NUMERIC_TYPES and right.data_type in NUMERIC_TYPES:
        result = max((left.data_type, right.data_type), key=_NUMERIC_PRECEDENCE.__getitem__)
        return TypeInfo(result, nullable=nullable)
    if {left.data_type, right.data_type} == {DataType.DATE, DataType.TIMESTAMP}:
        return TypeInfo(DataType.TIMESTAMP, nullable=nullable)
    raise DistributedSQLError(
        ErrorCode.BINDING_ERROR,
        f"Types {left.data_type.value} and {right.data_type.value} are not compatible.",
        context={"left_type": left.data_type.value, "right_type": right.data_type.value},
    )


def can_implicitly_cast(source: DataType, target: DataType) -> bool:
    if source is target or source is DataType.NULL:
        return True
    if source in NUMERIC_TYPES and target in NUMERIC_TYPES:
        return _NUMERIC_PRECEDENCE[source] <= _NUMERIC_PRECEDENCE[target]
    return source is DataType.DATE and target is DataType.TIMESTAMP

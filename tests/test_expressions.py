from decimal import Decimal

import pytest

from distributed_sql.common.protocol import DataType, Schema, SchemaField
from distributed_sql.planner import (
    Between,
    Binary,
    BinaryOperator,
    Binder,
    Case,
    Cast,
    Column,
    InList,
    IsNull,
    Like,
    Literal,
    Project,
    TypeInfo,
    sql_and,
    sql_or,
)
from distributed_sql.planner.types import literal_type


def literal(value: bool | int | float | Decimal | str | None) -> Literal:
    return Literal(value, literal_type(value))


@pytest.mark.parametrize(
    "left, right, expected",
    [
        pytest.param(True, None, None, id="真且未知"),
        pytest.param(False, None, False, id="假且未知"),
        pytest.param(None, False, False, id="未知且假"),
        pytest.param(None, None, None, id="未知且未知"),
    ],
)
def test_sql_and_uses_three_valued_logic(
    left: bool | None,
    right: bool | None,
    expected: bool | None,
) -> None:
    assert sql_and(left, right) is expected


@pytest.mark.parametrize(
    "left, right, expected",
    [
        pytest.param(False, None, None, id="假或未知"),
        pytest.param(True, None, True, id="真或未知"),
        pytest.param(None, True, True, id="未知或真"),
        pytest.param(None, None, None, id="未知或未知"),
    ],
)
def test_sql_or_uses_three_valued_logic(
    left: bool | None,
    right: bool | None,
    expected: bool | None,
) -> None:
    assert sql_or(left, right) is expected


def test_binary_arithmetic_and_comparison_propagate_null() -> None:
    null_add = Binary(
        BinaryOperator.ADD,
        literal(None),
        literal(1),
        TypeInfo(DataType.INT32),
    )
    null_equal = Binary(
        BinaryOperator.EQUAL,
        literal(None),
        literal(None),
        TypeInfo(DataType.BOOLEAN),
    )

    assert null_add.evaluate({}) is None
    assert null_equal.evaluate({}) is None


def test_in_between_like_case_and_is_null_evaluate_sql_semantics() -> None:
    value = Column("value", "t", TypeInfo(DataType.INT64))
    in_list = InList(value, (literal(1), literal(None)))
    between = Between(value, literal(1), literal(3))
    like = Like(
        Column("name", "t", TypeInfo(DataType.STRING)),
        literal("A_%"),
    )
    case = Case(
        ((IsNull(value), literal("missing")),),
        literal("present"),
        TypeInfo(DataType.STRING, nullable=False),
    )

    assert in_list.evaluate({"t.value": 9}) is None
    assert in_list.evaluate({"t.value": 1}) is True
    assert between.evaluate({"t.value": 2}) is True
    assert like.evaluate({"t.name": "Abc"}) is True
    assert case.evaluate({"t.value": None}) == "missing"
    assert case.evaluate({"t.value": 1}) == "present"


def test_cast_evaluates_numeric_and_date_conversions() -> None:
    decimal_cast = Cast(literal(2), TypeInfo(DataType.DECIMAL))
    date_cast = Cast(literal("2026-08-31"), TypeInfo(DataType.DATE), implicit=False)

    assert decimal_cast.evaluate({}) == Decimal("2")
    assert str(date_cast.evaluate({})) == "2026-08-31"


def test_bound_scalar_functions_evaluate() -> None:
    schema = {"default.items": Schema(fields=[SchemaField(name="name", data_type=DataType.STRING)])}
    plan = Binder(schema).bind(
        "SELECT SUBSTRING(name, 2, 2) AS part, ROUND(2.55, 1) AS rounded FROM items"
    )

    assert isinstance(plan, Project)
    assert plan.expressions[0].expression.evaluate({"items.name": "abcd"}) == "bc"
    assert plan.expressions[1].expression.evaluate({}) == pytest.approx(2.5)

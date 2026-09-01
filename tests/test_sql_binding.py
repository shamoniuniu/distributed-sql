import pytest

from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import DataType, PlanNodeType, Schema, SchemaField
from distributed_sql.planner import (
    Aggregate,
    Binder,
    Cast,
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
from distributed_sql.planner.expressions import Binary


@pytest.fixture
def catalog() -> dict[str, Schema]:
    return {
        "default.orders": Schema(
            fields=[
                SchemaField(name="id", data_type=DataType.INT64, nullable=False),
                SchemaField(name="customer_id", data_type=DataType.INT64),
                SchemaField(name="amount", data_type=DataType.INT32),
            ]
        ),
        "default.customers": Schema(
            fields=[
                SchemaField(name="id", data_type=DataType.INT64, nullable=False),
                SchemaField(name="name", data_type=DataType.STRING),
            ]
        ),
    }


def plan_types(plan: LogicalPlan) -> list[type[object]]:
    result: list[type[object]] = [type(plan)]
    for child in plan.children:
        result.extend(plan_types(child))
    return result


def test_bind_builds_scan_join_filter_aggregate_project_order_and_limit(
    catalog: dict[str, Schema],
) -> None:
    plan = Binder(catalog).bind(
        """
        SELECT o.customer_id, SUM(o.amount) AS total
        FROM orders AS o
        INNER JOIN customers AS c ON o.customer_id = c.id
        WHERE o.amount > 10
        GROUP BY o.customer_id
        ORDER BY total DESC
        LIMIT 5
        """
    )

    assert isinstance(plan, Limit)
    assert isinstance(plan.input, Order)
    assert isinstance(plan.input.input, Project)
    assert isinstance(plan.input.input.input, Aggregate)
    assert isinstance(plan.input.input.input.input, Filter)
    assert isinstance(plan.input.input.input.input.input, Join)
    assert plan_types(plan).count(Scan) == 2
    assert plan.output_schema.fields[1].name == "total"

    protocol = plan.to_protocol()
    assert protocol.node_type is PlanNodeType.LIMIT
    assert protocol.children[0].node_type is PlanNodeType.ORDER
    assert protocol.properties["count"] == 5


def test_bind_inserts_implicit_numeric_cast(catalog: dict[str, Schema]) -> None:
    plan = Binder(catalog).bind("SELECT amount + 0.5 AS adjusted FROM orders")

    assert isinstance(plan, Project)
    expression = plan.expressions[0].expression
    assert isinstance(expression, Binary)
    assert isinstance(expression.left, Cast)
    assert expression.type_info.data_type is DataType.FLOAT64


@pytest.mark.parametrize(
    "sql, message, context_key",
    [
        pytest.param(
            "SELECT id FROM orders o JOIN customers c ON o.customer_id = c.id",
            "ambiguous",
            "candidates",
            id="非限定列歧义",
        ),
        pytest.param(
            "SELECT missing FROM orders",
            "does not exist",
            "column",
            id="列不存在",
        ),
        pytest.param(
            "SELECT amount AS value, id AS value FROM orders",
            "Duplicate output",
            "alias",
            id="输出别名重复",
        ),
        pytest.param(
            "SELECT o.id FROM orders o JOIN customers o ON o.id = o.id",
            "Duplicate table alias",
            "alias",
            id="表别名重复",
        ),
    ],
)
def test_bind_rejects_name_errors_with_location(
    catalog: dict[str, Schema],
    sql: str,
    message: str,
    context_key: str,
) -> None:
    with pytest.raises(DistributedSQLError, match=message) as error:
        Binder(catalog).bind(sql)

    assert error.value.code is ErrorCode.BINDING_ERROR
    assert context_key in error.value.context
    assert error.value.context["line"] == 1
    assert isinstance(error.value.context["column"], int)


def test_bind_builds_window_node_and_rows_frame(catalog: dict[str, Schema]) -> None:
    plan = Binder(catalog).bind(
        """
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY amount
                   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
               ) AS row_num
        FROM orders
        """
    )

    assert isinstance(plan, Project)
    assert isinstance(plan.input, Window)
    window = plan.input.expressions[0].expression
    assert window.frame is not None
    assert window.frame.kind == "ROWS"
    assert window.frame.start == "1 PRECEDING"
    assert window.frame.end == "CURRENT ROW"


def test_window_aggregate_does_not_create_group_aggregate(catalog: dict[str, Schema]) -> None:
    plan = Binder(catalog).bind(
        "SELECT SUM(amount) OVER (PARTITION BY customer_id) AS running_total FROM orders"
    )

    assert isinstance(plan, Project)
    assert isinstance(plan.input, Window)
    assert isinstance(plan.input.input, Scan)


def test_bind_builds_grouping_sets_node(catalog: dict[str, Schema]) -> None:
    plan = Binder(catalog).bind(
        """
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        GROUP BY GROUPING SETS ((customer_id), ())
        """
    )

    assert isinstance(plan, Project)
    assert isinstance(plan.input, GroupingSets)
    assert len(plan.input.grouping_sets) == 2
    assert plan.input.grouping_sets[1] == ()


def test_having_only_aggregate_is_recorded(catalog: dict[str, Schema]) -> None:
    plan = Binder(catalog).bind(
        "SELECT customer_id FROM orders GROUP BY customer_id HAVING SUM(amount) > 10"
    )

    assert isinstance(plan, Project)
    assert isinstance(plan.input, Filter)
    assert isinstance(plan.input.input, Aggregate)
    assert [item.expression.sql() for item in plan.input.input.aggregates] == ["SUM(orders.amount)"]


def test_join_star_uses_unique_qualified_output_names(catalog: dict[str, Schema]) -> None:
    plan = Binder(catalog).bind(
        "SELECT * FROM orders o LEFT JOIN customers c ON o.customer_id = c.id"
    )

    assert isinstance(plan, Project)
    assert [field.name for field in plan.output_schema.fields] == [
        "o.id",
        "o.customer_id",
        "o.amount",
        "c.id",
        "c.name",
    ]


def test_bind_rejects_aggregate_in_where(catalog: dict[str, Schema]) -> None:
    with pytest.raises(DistributedSQLError, match="not allowed here") as error:
        Binder(catalog).bind("SELECT id FROM orders WHERE SUM(amount) > 0")

    assert error.value.code is ErrorCode.BINDING_ERROR

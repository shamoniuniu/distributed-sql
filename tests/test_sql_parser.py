import pytest
from sqlglot import exp

from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.planner import parse_select


def test_parse_select_accepts_supported_select_dialect() -> None:
    statement = parse_select(
        """
        SELECT o.customer_id, SUM(o.amount) AS total
        FROM orders AS o
        LEFT JOIN customers AS c ON o.customer_id = c.id
        WHERE o.amount BETWEEN 10 AND 100
        GROUP BY o.customer_id
        HAVING SUM(o.amount) > 20
        ORDER BY total DESC
        LIMIT 5
        """
    )

    assert isinstance(statement, exp.Select)
    assert len(statement.args["joins"]) == 1
    assert statement.args["limit"].expression.this == "5"


@pytest.mark.parametrize(
    "sql, message",
    [
        pytest.param("DELETE FROM orders", "Only SELECT", id="非SELECT语句"),
        pytest.param(
            "SELECT id FROM orders UNION SELECT id FROM archived_orders",
            "Only SELECT",
            id="集合运算",
        ),
        pytest.param(
            "WITH recent AS (SELECT id FROM orders) SELECT id FROM recent",
            "not supported",
            id="CTE",
        ),
        pytest.param(
            "SELECT * FROM orders CROSS JOIN customers",
            "Only INNER, LEFT, RIGHT, and FULL OUTER JOIN",
            id="交叉连接",
        ),
    ],
)
def test_parse_select_rejects_out_of_scope_syntax(sql: str, message: str) -> None:
    with pytest.raises(DistributedSQLError, match=message) as error:
        parse_select(sql)

    assert error.value.code is ErrorCode.SYNTAX_ERROR


def test_parse_select_reports_sqlglot_error_location() -> None:
    with pytest.raises(DistributedSQLError) as error:
        parse_select("SELECT id\nFROM orders\nWHERE )")

    assert error.value.code is ErrorCode.SYNTAX_ERROR
    assert error.value.context["line"] == 3
    assert isinstance(error.value.context["column"], int)

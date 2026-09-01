"""SQLGlot parsing with an explicit supported-SQL boundary."""

from __future__ import annotations

from typing import NoReturn

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode


def expression_location(expression: exp.Expression) -> dict[str, int]:
    candidates = [expression, *expression.walk()]
    for candidate in candidates:
        meta = candidate.meta
        if "line" in meta and "col" in meta:
            location = {"line": int(meta["line"]), "column": int(meta["col"])}
            if "start" in meta:
                location["start"] = int(meta["start"])
            if "end" in meta:
                location["end"] = int(meta["end"])
            return location
    return {}


def _unsupported(message: str, expression: exp.Expression) -> NoReturn:
    context: dict[str, object] = {"sql": expression.sql(dialect="duckdb")}
    context.update(expression_location(expression))
    raise DistributedSQLError(
        ErrorCode.SYNTAX_ERROR,
        message,
        context=context,
    )


def parse_select(sql: str) -> exp.Select:
    """Parse one query and reject syntax outside the documented SELECT subset."""

    if not sql.strip():
        raise DistributedSQLError(
            ErrorCode.SYNTAX_ERROR,
            "SQL text must not be empty.",
            context={"line": 1, "column": 1},
        )
    try:
        statements = sqlglot.parse(sql, read="duckdb")
    except ParseError as exc:
        detail = exc.errors[0] if exc.errors else {}
        context = {
            key: detail[key]
            for key in ("line", "col", "start_context", "highlight", "into_expression")
            if key in detail and detail[key] is not None
        }
        if "col" in context:
            context["column"] = context.pop("col")
        raise DistributedSQLError(
            ErrorCode.SYNTAX_ERROR,
            str(detail.get("description", "Invalid SQL syntax.")),
            context=context,
        ) from exc

    non_empty = [statement for statement in statements if statement is not None]
    if len(non_empty) != 1:
        raise DistributedSQLError(
            ErrorCode.SYNTAX_ERROR,
            "Exactly one SELECT statement is required.",
            context={"statement_count": len(non_empty)},
        )
    statement = non_empty[0]
    if not isinstance(statement, exp.Select):
        _unsupported("Only SELECT statements are supported.", statement)

    forbidden = (
        exp.Subquery,
        exp.SetOperation,
        exp.CTE,
        exp.Exists,
        exp.Unnest,
        exp.Lateral,
        exp.Pivot,
        exp.Placeholder,
    )
    for expression_type in forbidden:
        found = statement.find(expression_type)
        if found is not None:
            _unsupported(f"{expression_type.__name__} syntax is not supported.", found)

    for argument in ("with_", "qualify", "offset", "locks", "connect", "match"):
        value = statement.args.get(argument)
        if value:
            _unsupported(f"{argument.rstrip('_').upper()} is not supported.", value)
    if statement.args.get("distinct"):
        _unsupported("SELECT DISTINCT is not supported; use COUNT(DISTINCT ...) only.", statement)

    from_clause = statement.args.get("from")
    if from_clause is None or not isinstance(from_clause.this, exp.Table):
        _unsupported("SELECT requires a base table in FROM.", statement)

    for join in statement.args.get("joins") or []:
        if not isinstance(join.this, exp.Table):
            _unsupported("JOIN sources must be catalog tables.", join)
        side = str(join.args.get("side") or "").upper()
        kind = str(join.args.get("kind") or "").upper()
        method = join.args.get("method")
        if side not in {"", "LEFT", "RIGHT", "FULL"} or kind not in {"", "INNER", "OUTER"}:
            _unsupported("Only INNER, LEFT, RIGHT, and FULL OUTER JOIN are supported.", join)
        if method or join.args.get("using") or join.args.get("on") is None:
            _unsupported(
                "JOIN requires an ON condition; NATURAL, USING, and CROSS are unsupported.",
                join,
            )

    group = statement.args.get("group")
    if group is not None and (group.args.get("rollup") or group.args.get("cube")):
        _unsupported("ROLLUP and CUBE are not supported; use GROUPING SETS.", group)
    return statement

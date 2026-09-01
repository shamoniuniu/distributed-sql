from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import DataType, PartitionStrategy, Schema, SchemaField
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import LocalExecutor
from distributed_sql.optimizer import (
    ColumnPruning,
    ConstantFolding,
    EqualityInference,
    LimitJoinInputHint,
    LimitPushdownProject,
    PredicateMerge,
    PredicatePushdownAggregate,
    PredicatePushdownJoin,
    PredicatePushdownProject,
    Rule,
    RuleOptimizer,
)
from distributed_sql.planner import (
    Aggregate,
    Binary,
    BinaryOperator,
    Binder,
    Column,
    Filter,
    IsNull,
    Join,
    Limit,
    Literal,
    LogicalPlan,
    Project,
    Scan,
    TypeInfo,
)


def _table(
    tmp_path: Path,
    name: str,
    rows: list[dict[str, Any]],
    schema: Schema,
) -> CatalogTable:
    location = tmp_path / f"{name}.parquet"
    arrow_types = {DataType.INT64: pa.int64(), DataType.STRING: pa.string()}
    arrow_schema = pa.schema(
        [
            pa.field(field.name, arrow_types[field.data_type], nullable=field.nullable)
            for field in schema.fields
        ]
    )
    pq.write_table(pa.Table.from_pylist(rows, schema=arrow_schema), location)
    now = datetime.now(UTC)
    return CatalogTable(
        namespace="default",
        name=name,
        schema=schema,
        format=TableFormat.PARQUET,
        location=str(location),
        partition_strategy=PartitionStrategy.SINGLE,
        created_at=now,
        updated_at=now,
    )


@pytest.fixture
def optimizer_catalog(tmp_path: Path) -> dict[str, CatalogTable]:
    left_schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="region", data_type=DataType.STRING),
            SchemaField(name="amount", data_type=DataType.INT64),
            SchemaField(name="unused", data_type=DataType.STRING),
        ]
    )
    right_schema = Schema(
        fields=[
            SchemaField(name="id", data_type=DataType.INT64),
            SchemaField(name="label", data_type=DataType.STRING),
        ]
    )
    return {
        "default.items": _table(
            tmp_path,
            "items",
            [
                {"id": 1, "region": "east", "amount": 5, "unused": "x"},
                {"id": 2, "region": "east", "amount": 20, "unused": "y"},
                {"id": 3, "region": None, "amount": 30, "unused": "z"},
                {"id": None, "region": "west", "amount": 40, "unused": "n"},
            ],
            left_schema,
        ),
        "default.labels": _table(
            tmp_path,
            "labels",
            [
                {"id": 2, "label": "two"},
                {"id": 3, "label": None},
                {"id": 4, "label": "four"},
                {"id": None, "label": "null"},
            ],
            right_schema,
        ),
    }


def _bind(sql: str, tables: dict[str, CatalogTable]) -> LogicalPlan:
    return Binder({name: table.schema_ for name, table in tables.items()}).bind(sql)


def _rows(plan: LogicalPlan, tables: dict[str, CatalogTable]) -> list[dict[str, object]]:
    executor = LocalExecutor(
        tables,
        create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
    )
    return cast(list[dict[str, object]], executor.execute_table(plan).to_pylist())


def _assert_equivalent(
    original: LogicalPlan,
    optimized: LogicalPlan,
    tables: dict[str, CatalogTable],
) -> None:
    def normalize(rows: list[dict[str, object]]) -> list[tuple[str, ...]]:
        return sorted(
            tuple("<NULL>" if value is None else repr(value) for value in row.values())
            for row in rows
        )

    assert normalize(_rows(original, tables)) == normalize(_rows(optimized, tables))


def _optimize_one(plan: LogicalPlan, rule: Rule) -> LogicalPlan:
    result = RuleOptimizer((rule,)).optimize(plan)
    assert result.converged
    assert result.trace
    return result.optimized_plan


def test_predicate_pushdown_project_shape_and_equivalence(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    project = _bind("SELECT id AS kept, amount AS value FROM items", optimizer_catalog)
    assert isinstance(project, Project)
    predicate = Binary(
        BinaryOperator.GREATER_THAN,
        Column("value", "", TypeInfo(DataType.INT64)),
        Literal(10, TypeInfo(DataType.INT64, nullable=False)),
        TypeInfo(DataType.BOOLEAN),
    )
    original = Filter("outer_filter", project, predicate, project.output_schema)

    optimized = _optimize_one(original, PredicatePushdownProject())

    assert isinstance(optimized, Project)
    assert isinstance(optimized.input, Filter)
    assert optimized.input.predicate.sql() == "(items.amount > 10)"
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_predicate_pushdown_aggregate_only_moves_group_keys(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    original = _bind(
        """
        SELECT region, SUM(amount) AS total
        FROM items
        GROUP BY region
        HAVING region IS NOT NULL AND SUM(amount) > 10
        """,
        optimizer_catalog,
    )

    optimized = _optimize_one(original, PredicatePushdownAggregate())

    assert isinstance(optimized, Project)
    assert isinstance(optimized.input, Filter)
    assert isinstance(optimized.input.input, Aggregate)
    aggregate = optimized.input.input
    assert isinstance(aggregate.input, Filter)
    assert "items.region IS NULL" in aggregate.input.predicate.sql()
    assert "SUM" in optimized.input.predicate.sql()
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_predicate_pushdown_aggregate_keeps_global_having_constant(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    original = _bind(
        "SELECT COUNT(*) AS total FROM items HAVING FALSE",
        optimizer_catalog,
    )

    result = RuleOptimizer((PredicatePushdownAggregate(),)).optimize(original)

    assert result.trace == ()
    _assert_equivalent(original, result.optimized_plan, optimizer_catalog)


@pytest.mark.parametrize(
    ("join_type", "left_pushed", "right_pushed"),
    [
        ("INNER", True, True),
        ("LEFT", True, False),
        ("RIGHT", False, True),
        ("FULL OUTER", False, False),
    ],
)
def test_predicate_pushdown_join_respects_outer_join_preserved_rows(
    optimizer_catalog: dict[str, CatalogTable],
    join_type: str,
    left_pushed: bool,
    right_pushed: bool,
) -> None:
    original = _bind(
        f"""
        SELECT l.id AS left_id, r.id AS right_id
        FROM items l {join_type} JOIN labels r ON l.id = r.id
        WHERE l.amount > 10 AND r.label IS NOT NULL
        """,
        optimizer_catalog,
    )

    result = RuleOptimizer((PredicatePushdownJoin(),)).optimize(original)
    optimized = result.optimized_plan
    assert result.converged
    assert bool(result.trace) is (left_pushed or right_pushed)

    assert isinstance(optimized, Project)
    node = optimized.input
    if isinstance(node, Filter):
        node = node.input
    assert isinstance(node, Join)
    assert isinstance(node.left, Filter) is left_pushed
    assert isinstance(node.right, Filter) is right_pushed
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_limit_pushdown_project_is_exact_and_equivalent(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    original = _bind("SELECT id, amount + 1 AS value FROM items LIMIT 2", optimizer_catalog)

    optimized = _optimize_one(original, LimitPushdownProject())

    assert isinstance(optimized, Project)
    assert isinstance(optimized.input, Limit)
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_limit_join_hint_keeps_final_limit_and_is_not_executed(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    bound = _bind(
        "SELECT l.id, r.label FROM items l JOIN labels r ON l.id = r.id",
        optimizer_catalog,
    )
    assert isinstance(bound, Project)
    original = Limit("top_limit", bound.input, 1, bound.input.output_schema)

    optimized = _optimize_one(original, LimitJoinInputHint())

    assert isinstance(optimized, Limit)
    assert isinstance(optimized.input, Join)
    assert optimized.input.input_limit == 1
    assert not isinstance(optimized.input.left, Limit)
    assert not isinstance(optimized.input.right, Limit)
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_column_pruning_keeps_hidden_filter_and_join_columns(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    original = _bind(
        """
        SELECT l.region
        FROM items l JOIN labels r ON l.id = r.id
        WHERE r.label IS NOT NULL
        """,
        optimizer_catalog,
    )

    optimized = _optimize_one(original, ColumnPruning())

    assert isinstance(optimized, Project)
    assert isinstance(optimized.input, Filter)
    join = optimized.input.input
    assert isinstance(join, Join)
    assert isinstance(join.left, Scan)
    assert isinstance(join.right, Scan)
    assert [field.name for field in join.left.output_schema.fields] == ["id", "region"]
    assert [field.name for field in join.right.output_schema.fields] == ["id", "label"]
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_column_pruning_preserves_count_star_row_count(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    original = _bind("SELECT COUNT(*) AS total FROM items", optimizer_catalog)

    optimized = _optimize_one(original, ColumnPruning())

    assert isinstance(optimized, Project)
    assert isinstance(optimized.input, Aggregate)
    assert isinstance(optimized.input.input, Scan)
    assert optimized.input.input.output_schema.fields == []
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_constant_folding_preserves_null_three_valued_logic(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    original = _bind(
        "SELECT id FROM items WHERE (1 + 1 = 2) AND (NULL = 1 OR id > 1)",
        optimizer_catalog,
    )

    optimized = _optimize_one(original, ConstantFolding())

    assert isinstance(optimized, Project)
    assert isinstance(optimized.input, Filter)
    assert "NULL" in optimized.input.predicate.sql()
    assert "1 + 1" not in optimized.input.predicate.sql()
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_predicate_merge_normalizes_and_deduplicates(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    base = _bind("SELECT id FROM items", optimizer_catalog)
    assert isinstance(base, Project)
    column = Column("id", "", TypeInfo(DataType.INT64))
    first = IsNull(column, negated=True)
    second = Binary(
        BinaryOperator.GREATER_THAN,
        column,
        Literal(1, TypeInfo(DataType.INT32, nullable=False)),
        TypeInfo(DataType.BOOLEAN),
    )
    inner = Filter("inner", base, first, base.output_schema)
    original = Filter(
        "outer",
        inner,
        Binary(
            BinaryOperator.AND,
            second,
            first,
            TypeInfo(DataType.BOOLEAN),
        ),
        inner.output_schema,
    )

    optimized = _optimize_one(original, PredicateMerge())

    assert isinstance(optimized, Filter)
    assert not isinstance(optimized.input, Filter)
    assert optimized.predicate.sql().count("IS NOT NULL") == 1
    _assert_equivalent(original, optimized, optimizer_catalog)


def test_equality_inference_crosses_inner_but_not_outer_join(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    inner = _bind(
        """
        SELECT l.id, r.label
        FROM items l JOIN labels r ON l.id = r.id
        WHERE l.id = 2
        """,
        optimizer_catalog,
    )
    outer = _bind(
        """
        SELECT l.id, r.label
        FROM items l LEFT JOIN labels r ON l.id = r.id
        WHERE l.id = 2
        """,
        optimizer_catalog,
    )

    optimized_inner = _optimize_one(inner, EqualityInference())
    outer_result = RuleOptimizer((EqualityInference(),)).optimize(outer)

    assert isinstance(optimized_inner, Project)
    assert isinstance(optimized_inner.input, Filter)
    predicate_sql = optimized_inner.input.predicate.sql()
    assert "r.id" in predicate_sql
    assert "CAST(2 AS INT64)" in predicate_sql
    assert outer_result.trace == ()
    _assert_equivalent(inner, optimized_inner, optimizer_catalog)
    _assert_equivalent(outer, outer_result.optimized_plan, optimizer_catalog)


def test_fixed_point_trace_cycle_guard_and_explain(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    plan = _bind("SELECT id FROM items WHERE TRUE AND id > 1", optimizer_catalog)
    result = RuleOptimizer((ConstantFolding(),)).optimize(plan)

    assert result.converged
    assert result.iterations == 2
    explain = result.explain()
    assert "Original Logical Plan" in explain
    assert "Optimized Logical Plan" in explain
    assert "constant_folding" in explain
    assert "Termination: fixed_point" in explain

    class ToggleLimit:
        name = "toggle_limit"
        whole_plan = False

        def apply(self, candidate: LogicalPlan) -> LogicalPlan | None:
            if not isinstance(candidate, Limit):
                return None
            count = 2 if candidate.count == 1 else 1
            return Limit(candidate.node_id, candidate.input, count, candidate.output_schema)

    limited = Limit("limit", plan, 1, plan.output_schema)
    cycle = RuleOptimizer((ToggleLimit(),), max_iterations=5).optimize(limited)
    assert cycle.termination == "cycle_detected"


def test_default_rule_set_composes_to_fixed_point(
    optimizer_catalog: dict[str, CatalogTable],
) -> None:
    original = _bind(
        """
        SELECT l.region, r.label
        FROM items l JOIN labels r ON l.id = r.id
        WHERE TRUE AND l.id = 2
        LIMIT 3
        """,
        optimizer_catalog,
    )

    result = RuleOptimizer().optimize(original)

    assert result.converged
    matched = {entry.rule for entry in result.trace}
    assert {
        "constant_folding",
        "equality_inference",
        "predicate_pushdown_join",
        "limit_pushdown_project",
        "limit_join_input_hint",
        "column_pruning",
    } <= matched
    _assert_equivalent(original, result.optimized_plan, optimizer_catalog)

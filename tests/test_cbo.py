from __future__ import annotations

from datetime import UTC, datetime

import pyarrow as pa
import pytest

from distributed_sql.catalog import collect_table_statistics
from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.common.protocol import (
    ColumnStatistics,
    DataType,
    Partition,
    PartitionStrategy,
    Schema,
    SchemaField,
    Statistics,
)
from distributed_sql.optimizer import CostBasedOptimizer, CostModel, JoinStrategy
from distributed_sql.optimizer.utils import plan_sources
from distributed_sql.planner import (
    Aggregate,
    Binary,
    BinaryOperator,
    Binder,
    Filter,
    Join,
    LogicalPlan,
    Project,
    ScalarFunction,
    TypeInfo,
)

SCHEMA = Schema(
    fields=[
        SchemaField(name="id", data_type=DataType.INT64, nullable=False),
        SchemaField(name="value", data_type=DataType.INT64),
    ]
)


def _table(
    name: str,
    rows: int | None,
    *,
    size: int | None = None,
    ndv: int | None = None,
    strategy: PartitionStrategy = PartitionStrategy.UNKNOWN,
    partition_key: str | None = None,
    partitions: int = 0,
) -> CatalogTable:
    now = datetime.now(UTC)
    partition_items = [
        Partition(
            partition_id=f"{name}-{index}",
            ordinal=index,
            location=f"/{name}/{index}",
            strategy=strategy,
            keys=[partition_key] if partition_key else [],
            row_count=(rows // partitions if rows is not None else None),
            size_bytes=(size // partitions if size is not None else None),
        )
        for index in range(partitions)
    ]
    statistics = (
        Statistics(
            row_count=rows,
            size_bytes=size,
            columns={
                "id": ColumnStatistics(
                    column_name="id",
                    null_count=0,
                    distinct_count=ndv,
                    min_value=1,
                    max_value=ndv,
                    average_size_bytes=8,
                ),
                "value": ColumnStatistics(
                    column_name="value",
                    null_count=rows // 10,
                    distinct_count=max((ndv or rows) // 2, 1),
                    min_value=0,
                    max_value=100,
                    average_size_bytes=8,
                ),
            },
            source="test",
        )
        if rows is not None
        else None
    )
    return CatalogTable(
        namespace="default",
        name=name,
        schema=SCHEMA,
        format=TableFormat.PARQUET,
        location=f"/{name}",
        partition_strategy=strategy,
        partition_keys=[partition_key] if partition_key else [],
        partitions=partition_items,
        statistics=statistics,
        created_at=now,
        updated_at=now,
    )


def _bind(sql: str, catalog: dict[str, CatalogTable]) -> LogicalPlan:
    return Binder(catalog).bind(sql)


def _join(plan: LogicalPlan) -> Join:
    while isinstance(plan, Project | Filter | Aggregate):
        plan = plan.input
    assert isinstance(plan, Join)
    return plan


def test_collect_statistics_records_all_required_values() -> None:
    table = pa.table({"id": [1, 2, 2, None], "name": ["a", "bb", None, "a"]})

    statistics = collect_table_statistics(table, source="analyze")

    assert statistics.row_count == 4
    assert statistics.size_bytes == table.nbytes
    assert statistics.source == "analyze"
    assert statistics.columns["id"].null_count == 1
    assert statistics.columns["id"].distinct_count == 2
    assert statistics.columns["id"].min_value == 1
    assert statistics.columns["id"].max_value == 2


def test_filter_aggregate_and_join_cardinality_use_column_statistics() -> None:
    catalog = {
        "default.a": _table("a", 1_000, size=16_000, ndv=100),
        "default.b": _table("b", 200, size=3_200, ndv=50),
    }
    model = CostModel(catalog)
    filtered = _bind("SELECT id FROM a WHERE value > 50", catalog)
    grouped = _bind("SELECT value, COUNT(*) FROM a GROUP BY value", catalog)
    joined = _bind("SELECT a.id FROM a JOIN b ON a.id = b.id", catalog)

    assert isinstance(filtered, Project)
    filter_node = filtered.input
    assert isinstance(filter_node, Filter)
    assert model.estimate(filter_node).row_count == pytest.approx(450)
    assert isinstance(grouped, Project)
    aggregate_node = grouped.input
    assert isinstance(aggregate_node, Aggregate)
    assert model.estimate(aggregate_node).row_count == 50
    join_estimate, decision = model.estimate_join(_join(joined))
    assert join_estimate.row_count == pytest.approx(2_000)
    assert decision.build_side == "right"
    assert all(
        value > 0
        for value in (
            join_estimate.cost.cpu,
            join_estimate.cost.memory,
            join_estimate.cost.disk,
        )
    )


@pytest.mark.parametrize(
    ("left_strategy", "right_strategy", "left_key", "right_key", "expected"),
    [
        (PartitionStrategy.HASH, PartitionStrategy.HASH, "id", "id", JoinStrategy.REUSE),
        (
            PartitionStrategy.HASH,
            PartitionStrategy.UNKNOWN,
            "id",
            None,
            JoinStrategy.REPARTITION_RIGHT,
        ),
        (
            PartitionStrategy.UNKNOWN,
            PartitionStrategy.HASH,
            None,
            "id",
            JoinStrategy.REPARTITION_LEFT,
        ),
        (
            PartitionStrategy.UNKNOWN,
            PartitionStrategy.UNKNOWN,
            None,
            None,
            JoinStrategy.REPARTITION_BOTH,
        ),
    ],
)
def test_partition_strategy_selection(
    left_strategy: PartitionStrategy,
    right_strategy: PartitionStrategy,
    left_key: str | None,
    right_key: str | None,
    expected: JoinStrategy,
) -> None:
    catalog = {
        "default.a": _table(
            "a",
            10_000,
            size=160_000,
            ndv=1_000,
            strategy=left_strategy,
            partition_key=left_key,
            partitions=4 if left_key else 0,
        ),
        "default.b": _table(
            "b",
            20_000,
            size=320_000,
            ndv=1_000,
            strategy=right_strategy,
            partition_key=right_key,
            partitions=4 if right_key else 0,
        ),
    }
    join = _join(_bind("SELECT a.id FROM a JOIN b ON a.id = b.id", catalog))

    _, decision = CostModel(catalog, broadcast_threshold_bytes=0).estimate_join(join)

    assert decision.strategy is expected


def test_broadcast_strategy_and_missing_statistics_evidence() -> None:
    catalog = {
        "default.large": _table("large", 1_000_000, size=16_000_000, ndv=100_000),
        "default.small": _table("small", 10, size=160, ndv=10),
        "default.unknown": _table("unknown", None),
    }
    plan = _bind(
        "SELECT large.id FROM large JOIN small ON large.id = small.id",
        catalog,
    )
    result = CostBasedOptimizer(catalog).optimize(plan)
    unknown = CostModel(catalog).estimate(_bind("SELECT id FROM unknown", catalog))

    assert result.join_decisions[0].strategy is JoinStrategy.BROADCAST
    assert result.join_decisions[0].build_side == "right"
    assert any(source.startswith("default:default.unknown.row_count") for source in unknown.sources)
    explain = CostBasedOptimizer(catalog).optimize(
        _bind("SELECT id FROM unknown", catalog)
    ).explain()
    assert "default:default.unknown.row_count=1000000" in explain
    assert "cpu=" in result.explain()
    assert "network=" in result.explain()
    assert "memory=" in result.explain()
    assert "disk=" in result.explain()


def test_dynamic_programming_reorders_inner_joins_but_not_outer_boundary() -> None:
    catalog = {
        "default.fact": _table("fact", 1_000_000, size=16_000_000, ndv=100_000),
        "default.dim": _table("dim", 1_000, size=16_000, ndv=1_000),
        "default.tiny": _table("tiny", 10, size=160, ndv=10),
    }
    inner = _bind(
        """
        SELECT fact.id
        FROM fact
        JOIN dim ON fact.id = dim.id
        JOIN tiny ON dim.id = tiny.id
        """,
        catalog,
    )
    outer = _bind(
        """
        SELECT fact.id
        FROM fact
        LEFT JOIN dim ON fact.id = dim.id
        JOIN tiny ON dim.id = tiny.id
        """,
        catalog,
    )

    inner_result = CostBasedOptimizer(catalog).optimize(inner)
    inner_join = _join(inner_result.optimized_plan)
    outer_result = CostBasedOptimizer(catalog).optimize(outer)

    assert inner_result.reordered_regions == 1
    child_source_sets = [plan_sources(child) for child in inner_join.children]
    assert frozenset({"dim", "tiny"}) in child_source_sets
    assert outer_result.reordered_regions == 0
    assert plan_sources(_join(outer_result.optimized_plan).left) == frozenset({"fact", "dim"})


def test_nondeterministic_join_condition_is_a_reorder_boundary() -> None:
    catalog = {
        "default.a": _table("a", 1_000, size=16_000, ndv=100),
        "default.b": _table("b", 100, size=1_600, ndv=100),
        "default.c": _table("c", 10, size=160, ndv=10),
    }
    plan = _bind(
        "SELECT a.id FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
        catalog,
    )
    root = _join(plan)
    nondeterministic = ScalarFunction(
        "random",
        (),
        TypeInfo(DataType.BOOLEAN, nullable=False),
    )
    guarded = root.__class__(
        root.node_id,
        root.left,
        root.right,
        root.join_type,
        Binary(
            BinaryOperator.AND,
            root.condition,
            nondeterministic,
            TypeInfo(DataType.BOOLEAN, nullable=False),
        ),
        root.output_schema,
    )
    assert isinstance(plan, Project)
    result = CostBasedOptimizer(catalog).optimize(
        Project(plan.node_id, guarded, plan.expressions, plan.output_schema)
    )

    assert result.reordered_regions == 0

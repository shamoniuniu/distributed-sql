from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from pydantic import ValidationError

from distributed_sql.advisor import (
    AdviceStatus,
    AdvisorThresholds,
    QueryAdvisor,
    Severity,
)
from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.common.protocol import (
    Attempt,
    AttemptState,
    ColumnStatistics,
    DataType,
    Partition,
    PartitionStrategy,
    Schema,
    SchemaField,
    Stage,
    StageState,
    Statistics,
    Task,
    TaskState,
)
from distributed_sql.execution import (
    DistributedResult,
    PhysicalPlan,
    RuntimeFilterMetrics,
    ScheduleResult,
    ShuffleFile,
    ShuffleManifest,
    ShuffleMetrics,
    SpillMetrics,
    StageGraph,
    TaskOutcome,
)
from distributed_sql.observability import (
    EventLevel,
    StructuredEventLogger,
    StructuredLogEvent,
    build_query_diagnostics,
)
from distributed_sql.optimizer import CostBasedOptimizationResult, CostBasedOptimizer
from distributed_sql.planner import Binder, LogicalPlan

SCHEMA = Schema(
    fields=[
        SchemaField(name="id", data_type=DataType.INT64, nullable=False),
        SchemaField(name="value", data_type=DataType.INT64),
    ]
)


def _table(name: str, *, statistics: bool = True, rows: int = 100) -> CatalogTable:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    stats = (
        Statistics(
            row_count=rows,
            size_bytes=rows * 16,
            columns={
                "id": ColumnStatistics(
                    column_name="id",
                    null_count=0,
                    distinct_count=rows,
                    min_value=1,
                    max_value=rows,
                    average_size_bytes=8,
                ),
                "value": ColumnStatistics(
                    column_name="value",
                    null_count=0,
                    distinct_count=rows,
                    min_value=1,
                    max_value=rows,
                    average_size_bytes=8,
                ),
            },
            collected_at=now,
            source="analyze",
        )
        if statistics
        else None
    )
    return CatalogTable(
        namespace="default",
        name=name,
        schema=SCHEMA,
        format=TableFormat.PARQUET,
        location=f"/{name}",
        partition_strategy=PartitionStrategy.UNKNOWN,
        statistics=stats,
        created_at=now,
        updated_at=now,
    )


def _optimization(
    *,
    statistics: bool = True,
) -> tuple[CostBasedOptimizationResult, LogicalPlan]:
    tables = {
        "default.left_table": _table("left_table", statistics=statistics, rows=100),
        "default.right_table": _table("right_table", statistics=statistics, rows=10),
    }
    logical = Binder(tables).bind(
        """
        SELECT l.id
        FROM left_table l JOIN right_table r ON l.id = r.id
        """
    )
    optimized = CostBasedOptimizer(tables, broadcast_threshold_bytes=0).optimize(logical)
    return optimized, optimized.optimized_plan


def _execution_result(
    query_id: str,
    physical: PhysicalPlan,
    *,
    shuffle_bytes: int = 1_000,
    spill_bytes: int = 0,
    spill_count: int = 0,
) -> DistributedResult:
    started = datetime(2026, 1, 1, tzinfo=UTC)
    stage = Stage(
        stage_id="stage-1",
        query_id=query_id,
        state=StageState.SUCCEEDED,
        plan=physical.to_protocol(),
    )
    task = Task(
        task_id="task-1",
        query_id=query_id,
        stage_id=stage.stage_id,
        partition=Partition(partition_id="partition-1", ordinal=0, location=""),
        state=TaskState.SUCCEEDED,
        attempt_ids=["attempt-0", "attempt-1"],
    )
    first = Attempt(
        attempt_id="attempt-0",
        task_id=task.task_id,
        attempt_number=0,
        worker_id="worker-1",
        state=AttemptState.LOST,
        started_at=started,
        finished_at=started + timedelta(seconds=1),
        error={"type": "WorkerLostError", "message": "lease expired"},
    )
    second = Attempt(
        attempt_id="attempt-1",
        task_id=task.task_id,
        attempt_number=1,
        worker_id="worker-2",
        state=AttemptState.SUCCEEDED,
        started_at=started + timedelta(seconds=2),
        finished_at=started + timedelta(seconds=3),
    )
    manifest = ShuffleManifest(
        query_id=query_id,
        stage_id=stage.stage_id,
        task_id=task.task_id,
        attempt_id=second.attempt_id,
        files=[
            ShuffleFile(
                partition=0,
                location="/shuffle/part-0",
                row_count=10,
                size_bytes=100,
                checksum="a",
            ),
            ShuffleFile(
                partition=1,
                location="/shuffle/part-1",
                row_count=90,
                size_bytes=900,
                checksum="b",
            ),
        ],
        metrics=ShuffleMetrics(
            records_written=100,
            bytes_written=shuffle_bytes,
            partition_count=2,
        ),
    )
    schedule = ScheduleResult(
        stages={stage.stage_id: stage},
        tasks={task.task_id: task},
        attempts={first.attempt_id: first, second.attempt_id: second},
        outcomes={
            task.task_id: TaskOutcome(
                task.task_id,
                second.attempt_id,
                second.worker_id or "",
                manifest,
            )
        },
        max_running_by_worker={"worker-1": 1, "worker-2": 1},
    )
    spill = SpillMetrics(
        spill_bytes=spill_bytes,
        spill_files=spill_count,
        spill_count=spill_count,
        peak_memory_bytes=512,
    )
    return DistributedResult(
        table=pa.table({"id": [1, 2]}),
        partitions=(),
        stage_graph=StageGraph(stage.stage_id, (stage,), (task,)),
        schedules=(schedule,),
        shuffle_metrics=ShuffleMetrics(
            records_written=100,
            bytes_written=shuffle_bytes,
            records_read=100,
            bytes_read=shuffle_bytes,
            partition_count=2,
        ),
        runtime_filter_metrics=RuntimeFilterMetrics(input_rows=100, output_rows=10),
        spill_metrics=spill,
        task_spill_metrics={task.task_id: spill},
    )


def test_diagnostics_aggregate_real_metrics_retries_logs_and_explain() -> None:
    query_id = "query-observe"
    optimization, physical = _optimization()
    result = _execution_result(query_id, physical)
    logger = StructuredEventLogger()
    logger.emit(
        StructuredLogEvent(
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            level=EventLevel.INFO,
            event="query_planned",
            message="Query plan completed.",
            query_id=query_id,
        )
    )

    diagnostics = build_query_diagnostics(
        query_id,
        optimization,
        physical,
        result,
        logs=logger.events,
    )

    assert diagnostics.runtime.shuffle_bytes_written == 1_000
    assert diagnostics.runtime.runtime_filter_filtered_rows == 90
    assert diagnostics.stages[0].retry_count == 1
    assert diagnostics.tasks[0].spill_bytes == 0
    assert diagnostics.retries[0].previous_state == AttemptState.LOST.value
    assert diagnostics.shuffle_partition_bytes == [100, 900]
    assert {
        (
            event.query_id,
            event.stage_id,
            event.task_id,
            event.attempt_id,
            event.worker_id,
        )
        for event in diagnostics.timeline
        if event.attempt_id is not None
    } == {
        (query_id, "stage-1", "task-1", "attempt-0", "worker-1"),
        (query_id, "stage-1", "task-1", "attempt-1", "worker-2"),
    }
    explain = diagnostics.explain_analyze()
    assert "== Physical Plan ==" in explain
    assert "== Runtime Metrics ==" in explain
    assert "== Task Metrics ==" in explain
    assert "== Retry Events ==" in explain
    assert "shuffle_write_bytes=1000" in explain
    assert "task_sum_seconds=" in explain


def test_advisor_emits_quantified_shuffle_skew_spill_and_join_advice() -> None:
    query_id = "query-advice"
    optimization, physical = _optimization()
    diagnostics = build_query_diagnostics(
        query_id,
        optimization,
        physical,
        _execution_result(
            query_id,
            physical,
            spill_bytes=600,
            spill_count=3,
        ),
    )
    advisor = QueryAdvisor(
        AdvisorThresholds(
            high_shuffle_bytes=500,
            skew_ratio=3,
            skew_min_partition_bytes=100,
            frequent_spill_count=3,
            critical_spill_bytes=500,
            broadcast_candidate_bytes=1_000,
        )
    )

    report = advisor.analyze(diagnostics)

    codes = {item.code.split(":", 1)[0] for item in report.recommendations}
    assert report.status is AdviceStatus.RECOMMENDATIONS
    assert {"HIGH_SHUFFLE", "SHUFFLE_SKEW", "OPERATOR_SPILL"} <= codes
    assert any(code.startswith("JOIN_BROADCAST_CANDIDATE") for code in codes)
    assert next(
        item for item in report.recommendations if item.code == "OPERATOR_SPILL"
    ).severity is Severity.CRITICAL
    assert all(
        item.evidence and item.action and item.expected_impact
        for item in report.recommendations
    )


def test_missing_statistics_is_specific_and_suppresses_join_strategy_advice() -> None:
    query_id = "query-missing-stats"
    optimization, physical = _optimization(statistics=False)
    diagnostics = build_query_diagnostics(
        query_id,
        optimization,
        physical,
        _execution_result(query_id, physical, shuffle_bytes=0),
    )

    report = QueryAdvisor().analyze(diagnostics)

    codes = [item.code for item in report.recommendations]
    assert any(code.startswith("MISSING_STATISTICS:") for code in codes)
    assert not any(code.startswith("JOIN_") for code in codes)
    evidence = report.recommendations[0].evidence
    assert any(item.metric == "statistics_fallbacks" for item in evidence)


def test_no_evidence_returns_explicit_no_recommendations() -> None:
    optimization, physical = _optimization()
    diagnostics = build_query_diagnostics(
        "query-clean",
        optimization,
        physical,
        _execution_result("query-clean", physical, shuffle_bytes=0),
    )

    report = QueryAdvisor(
        AdvisorThresholds(broadcast_candidate_bytes=1)
    ).analyze(diagnostics)

    assert report.status is AdviceStatus.NO_RECOMMENDATIONS
    assert report.message == "暂无高置信度建议"
    assert report.recommendations == []


def test_structured_log_requires_complete_correlation_chain() -> None:
    with pytest.raises(ValidationError, match="attempt_id requires task_id"):
        StructuredLogEvent(
            event="attempt_failed",
            message="failed",
            query_id="query-1",
            attempt_id="attempt-1",
        )

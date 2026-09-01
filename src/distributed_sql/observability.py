"""Structured query diagnostics assembled from optimizer and execution evidence."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from enum import StrEnum
from itertools import pairwise
from typing import Any, cast

import pyarrow as pa
from pydantic import Field, JsonValue, model_validator

from distributed_sql.common.protocol import (
    Attempt,
    PlanNode,
    ProtocolModel,
    RemoteTaskMetrics,
    RemoteTaskResult,
    TaskState,
)
from distributed_sql.execution.distributed import DistributedResult
from distributed_sql.execution.memory import SpillMetrics
from distributed_sql.execution.physical import PhysicalPlan
from distributed_sql.execution.scheduler import ScheduleResult
from distributed_sql.execution.shuffle import ShuffleManifest, ShuffleMetrics
from distributed_sql.optimizer.cbo import CostBasedOptimizationResult


class EventLevel(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class StructuredLogEvent(ProtocolModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    level: EventLevel = EventLevel.INFO
    event: str
    message: str
    query_id: str
    stage_id: str | None = None
    task_id: str | None = None
    attempt_id: str | None = None
    worker_id: str | None = None
    attributes: dict[str, JsonValue] = Field(default_factory=dict)

    @model_validator(mode="after")
    def correlation_is_hierarchical(self) -> StructuredLogEvent:
        if self.attempt_id is not None and self.task_id is None:
            raise ValueError("attempt_id requires task_id")
        if self.task_id is not None and self.stage_id is None:
            raise ValueError("task_id requires stage_id")
        return self


class StructuredEventLogger:
    """Emit JSON logs while retaining events for a query diagnostic timeline."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger or logging.getLogger("distributed_sql.query")
        self._events: list[StructuredLogEvent] = []

    @property
    def events(self) -> tuple[StructuredLogEvent, ...]:
        return tuple(self._events)

    def emit(self, event: StructuredLogEvent) -> StructuredLogEvent:
        self._events.append(event)
        self.logger.log(
            {
                EventLevel.DEBUG: logging.DEBUG,
                EventLevel.INFO: logging.INFO,
                EventLevel.WARNING: logging.WARNING,
                EventLevel.ERROR: logging.ERROR,
            }[event.level],
            json.dumps(
                event.model_dump(mode="json"),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
        return event


class AttemptMetrics(ProtocolModel):
    query_id: str
    stage_id: str
    task_id: str
    attempt_id: str
    worker_id: str | None = None
    state: str
    attempt_number: int = Field(ge=0)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
    error: dict[str, JsonValue] | None = None


class TaskMetrics(ProtocolModel):
    query_id: str
    stage_id: str
    task_id: str
    state: str
    partition_ordinal: int = Field(ge=0)
    attempt_count: int = Field(ge=0)
    retry_count: int = Field(ge=0)
    input_rows: int | None = Field(default=None, ge=0)
    input_bytes: int | None = Field(default=None, ge=0)
    output_rows: int | None = Field(default=None, ge=0)
    output_bytes: int | None = Field(default=None, ge=0)
    duration_seconds: float | None = Field(default=None, ge=0)
    shuffle_records_written: int = Field(default=0, ge=0)
    shuffle_bytes_written: int = Field(default=0, ge=0)
    shuffle_write_seconds: float = Field(default=0.0, ge=0)
    shuffle_records_read: int = Field(default=0, ge=0)
    shuffle_bytes_read: int = Field(default=0, ge=0)
    shuffle_read_seconds: float = Field(default=0.0, ge=0)
    spill_bytes: int = Field(default=0, ge=0)
    spill_files: int = Field(default=0, ge=0)
    spill_count: int = Field(default=0, ge=0)
    peak_memory_bytes: int = Field(default=0, ge=0)
    external_sort_runs: int = Field(default=0, ge=0)
    hash_partitions: int = Field(default=0, ge=0)
    sort_merge_fallbacks: int = Field(default=0, ge=0)
    sort_aggregate_runs: int = Field(default=0, ge=0)


class StageMetrics(ProtocolModel):
    query_id: str
    stage_id: str
    state: str
    task_count: int = Field(ge=0)
    succeeded_tasks: int = Field(ge=0)
    failed_tasks: int = Field(ge=0)
    attempt_count: int = Field(ge=0)
    retry_count: int = Field(ge=0)
    input_rows: int | None = Field(default=None, ge=0)
    input_bytes: int | None = Field(default=None, ge=0)
    output_rows: int | None = Field(default=None, ge=0)
    output_bytes: int | None = Field(default=None, ge=0)
    wall_duration_seconds: float | None = Field(default=None, ge=0)
    max_task_duration_seconds: float | None = Field(default=None, ge=0)
    task_duration_sum_seconds: float | None = Field(default=None, ge=0)
    shuffle_records_written: int = Field(default=0, ge=0)
    shuffle_bytes_written: int = Field(default=0, ge=0)
    shuffle_write_seconds: float = Field(default=0.0, ge=0)
    shuffle_records_read: int = Field(default=0, ge=0)
    shuffle_bytes_read: int = Field(default=0, ge=0)
    shuffle_read_seconds: float = Field(default=0.0, ge=0)
    spill_bytes: int = Field(default=0, ge=0)
    spill_files: int = Field(default=0, ge=0)
    spill_count: int = Field(default=0, ge=0)
    peak_memory_bytes: int = Field(default=0, ge=0)
    external_sort_runs: int = Field(default=0, ge=0)
    hash_partitions: int = Field(default=0, ge=0)
    sort_merge_fallbacks: int = Field(default=0, ge=0)
    sort_aggregate_runs: int = Field(default=0, ge=0)


class RetryEvent(ProtocolModel):
    query_id: str
    stage_id: str
    task_id: str
    attempt_id: str
    worker_id: str | None = None
    attempt_number: int = Field(ge=1)
    previous_attempt_id: str
    previous_state: str
    previous_error: dict[str, JsonValue] | None = None
    timestamp: datetime | None = None


class TimelineEvent(ProtocolModel):
    timestamp: datetime
    event: str
    state: str
    query_id: str
    stage_id: str | None = None
    task_id: str | None = None
    attempt_id: str | None = None
    worker_id: str | None = None
    message: str


class JoinPlanEvidence(ProtocolModel):
    node_id: str
    strategy: str
    build_side: str
    estimated_rows: float = Field(ge=0)
    estimated_bytes: float = Field(ge=0)
    left_bytes: float = Field(ge=0)
    right_bytes: float = Field(ge=0)
    statistics_fallbacks: list[str] = Field(default_factory=list)
    reason: str

    @property
    def build_bytes(self) -> float:
        return self.left_bytes if self.build_side == "left" else self.right_bytes


class OptimizationDiagnostics(ProtocolModel):
    rule_trace_count: int = Field(ge=0)
    rule_trace: list[dict[str, JsonValue]] = Field(default_factory=list)
    termination: str
    iterations: int = Field(ge=0)
    reordered_join_regions: int = Field(ge=0)
    statistics_fallbacks: list[str] = Field(default_factory=list)
    joins: list[JoinPlanEvidence] = Field(default_factory=list)
    explain: str


class RuntimeMetrics(ProtocolModel):
    result_rows: int = Field(ge=0)
    result_bytes: int = Field(ge=0)
    shuffle_records_written: int = Field(ge=0)
    shuffle_bytes_written: int = Field(ge=0)
    shuffle_records_read: int = Field(ge=0)
    shuffle_bytes_read: int = Field(ge=0)
    shuffle_partition_count: int = Field(ge=0)
    shuffle_write_seconds: float = Field(ge=0)
    shuffle_read_seconds: float = Field(ge=0)
    spill_bytes: int = Field(ge=0)
    spill_files: int = Field(ge=0)
    spill_count: int = Field(ge=0)
    peak_memory_bytes: int = Field(ge=0)
    external_sort_runs: int = Field(ge=0)
    hash_partitions: int = Field(ge=0)
    sort_merge_fallbacks: int = Field(ge=0)
    sort_aggregate_runs: int = Field(ge=0)
    runtime_filter_input_rows: int = Field(ge=0)
    runtime_filter_output_rows: int = Field(ge=0)
    runtime_filter_filtered_rows: int = Field(ge=0)


class QueryDiagnostics(ProtocolModel):
    query_id: str
    physical_plan: PlanNode
    optimization: OptimizationDiagnostics
    runtime: RuntimeMetrics
    stages: list[StageMetrics]
    tasks: list[TaskMetrics]
    attempts: list[AttemptMetrics]
    retries: list[RetryEvent]
    shuffle_partition_rows: list[int]
    shuffle_partition_bytes: list[int]
    timeline: list[TimelineEvent]
    logs: list[StructuredLogEvent] = Field(default_factory=list)

    def explain_analyze(self) -> str:
        lines = [
            self.optimization.explain,
            "",
            "== Physical Plan ==",
            _plan_text(self.physical_plan),
            "",
            "== Runtime Metrics ==",
            (
                f"result_rows={self.runtime.result_rows}, "
                f"result_bytes={self.runtime.result_bytes}, "
                f"shuffle_write_bytes={self.runtime.shuffle_bytes_written}, "
                f"shuffle_read_bytes={self.runtime.shuffle_bytes_read}, "
                f"spill_bytes={self.runtime.spill_bytes}, "
                f"peak_memory_bytes={self.runtime.peak_memory_bytes}"
            ),
            "",
            "== Stage Metrics ==",
        ]
        lines.extend(
            (
                f"{stage.stage_id}: state={stage.state}, tasks={stage.task_count}, "
                f"attempts={stage.attempt_count}, retries={stage.retry_count}, "
                f"input_rows={_display(stage.input_rows)}, "
                f"input_bytes={_display(stage.input_bytes)}, "
                f"output_rows={_display(stage.output_rows)}, "
                f"output_bytes={_display(stage.output_bytes)}, "
                f"wall_seconds={_display_float(stage.wall_duration_seconds)}, "
                f"max_task_seconds={_display_float(stage.max_task_duration_seconds)}, "
                f"task_sum_seconds={_display_float(stage.task_duration_sum_seconds)}, "
                f"shuffle_write_rows={stage.shuffle_records_written}, "
                f"shuffle_write_bytes={stage.shuffle_bytes_written}, "
                f"shuffle_read_rows={stage.shuffle_records_read}, "
                f"shuffle_read_bytes={stage.shuffle_bytes_read}, "
                f"spill_bytes={stage.spill_bytes}, spill_count={stage.spill_count}"
            )
            for stage in self.stages
        )
        lines.extend(["", "== Task Metrics =="])
        lines.extend(
            (
                f"{task.task_id}: stage={task.stage_id}, state={task.state}, "
                f"input_rows={_display(task.input_rows)}, "
                f"input_bytes={_display(task.input_bytes)}, "
                f"output_rows={_display(task.output_rows)}, "
                f"output_bytes={_display(task.output_bytes)}, "
                f"duration_seconds={_display_float(task.duration_seconds)}, "
                f"shuffle_write_rows={task.shuffle_records_written}, "
                f"shuffle_write_bytes={task.shuffle_bytes_written}, "
                f"shuffle_read_rows={task.shuffle_records_read}, "
                f"shuffle_read_bytes={task.shuffle_bytes_read}, "
                f"spill_bytes={task.spill_bytes}, spill_count={task.spill_count}"
            )
            for task in self.tasks
        )
        lines.extend(["", "== Retry Events =="])
        if not self.retries:
            lines.append("(none)")
        else:
            lines.extend(
                (
                    f"{event.attempt_id}: stage={event.stage_id}, task={event.task_id}, "
                    f"worker={event.worker_id or 'unassigned'}, "
                    f"previous={event.previous_attempt_id}:{event.previous_state}"
                )
                for event in self.retries
            )
        return "\n".join(lines)


def build_query_diagnostics(
    query_id: str,
    optimization: CostBasedOptimizationResult,
    physical_plan: PhysicalPlan,
    result: DistributedResult,
    *,
    logs: Iterable[StructuredLogEvent] = (),
) -> QueryDiagnostics:
    """Join Task 6-12 outputs without synthesizing unavailable measurements."""

    optimization_diagnostics = _optimization_diagnostics(optimization)
    task_metrics, attempt_metrics, retries, stages, partition_rows, partition_bytes = (
        _schedule_metrics(query_id, result.schedules, result.task_spill_metrics)
    )
    runtime_filter = result.runtime_filter_metrics
    spill = result.spill_metrics
    shuffle = result.shuffle_metrics
    log_items = sorted(
        (event for event in logs if event.query_id == query_id),
        key=lambda item: item.timestamp,
    )
    timeline = _timeline(attempt_metrics, log_items)
    return QueryDiagnostics(
        query_id=query_id,
        physical_plan=physical_plan.to_protocol(),
        optimization=optimization_diagnostics,
        runtime=RuntimeMetrics(
            result_rows=result.table.num_rows,
            result_bytes=result.table.nbytes,
            shuffle_records_written=shuffle.records_written,
            shuffle_bytes_written=shuffle.bytes_written,
            shuffle_records_read=shuffle.records_read,
            shuffle_bytes_read=shuffle.bytes_read,
            shuffle_partition_count=shuffle.partition_count,
            shuffle_write_seconds=shuffle.write_seconds,
            shuffle_read_seconds=shuffle.read_seconds,
            spill_bytes=spill.spill_bytes,
            spill_files=spill.spill_files,
            spill_count=spill.spill_count,
            peak_memory_bytes=spill.peak_memory_bytes,
            external_sort_runs=spill.external_sort_runs,
            hash_partitions=spill.hash_partitions,
            sort_merge_fallbacks=spill.sort_merge_fallbacks,
            sort_aggregate_runs=spill.sort_aggregate_runs,
            runtime_filter_input_rows=runtime_filter.input_rows,
            runtime_filter_output_rows=runtime_filter.output_rows,
            runtime_filter_filtered_rows=runtime_filter.filtered_rows,
        ),
        stages=stages,
        tasks=task_metrics,
        attempts=attempt_metrics,
        retries=retries,
        shuffle_partition_rows=partition_rows,
        shuffle_partition_bytes=partition_bytes,
        timeline=timeline,
        logs=log_items,
    )


def explain_analyze(
    query_id: str,
    optimization: CostBasedOptimizationResult,
    physical_plan: PhysicalPlan,
    result: DistributedResult,
    *,
    logs: Iterable[StructuredLogEvent] = (),
) -> str:
    return build_query_diagnostics(
        query_id,
        optimization,
        physical_plan,
        result,
        logs=logs,
    ).explain_analyze()


def _optimization_diagnostics(
    optimization: CostBasedOptimizationResult,
) -> OptimizationDiagnostics:
    fallbacks = sorted(
        {
            source
            for estimate in optimization.node_estimates.values()
            for source in estimate.sources
            if source.startswith("default:")
        }
    )
    joins: list[JoinPlanEvidence] = []
    plan_by_id = _logical_plan_by_id(optimization.optimized_plan)
    for decision in optimization.join_decisions:
        node = plan_by_id[decision.node_id]
        left, right = node.children
        child_fallbacks = sorted(
            {
                source
                for child in (left, right)
                for source in optimization.node_estimates[child.node_id].sources
                if source.startswith("default:")
            }
        )
        joins.append(
            JoinPlanEvidence(
                node_id=decision.node_id,
                strategy=decision.strategy.value,
                build_side=decision.build_side,
                estimated_rows=decision.estimated_rows,
                estimated_bytes=decision.estimated_bytes,
                left_bytes=optimization.node_estimates[left.node_id].size_bytes,
                right_bytes=optimization.node_estimates[right.node_id].size_bytes,
                statistics_fallbacks=child_fallbacks,
                reason=decision.reason,
            )
        )
    trace: list[dict[str, JsonValue]] = [
        {
            "iteration": cast(JsonValue, item.iteration),
            "rule": cast(JsonValue, item.rule),
            "before": cast(JsonValue, item.before),
            "after": cast(JsonValue, item.after),
        }
        for item in optimization.rbo_result.trace
    ]
    return OptimizationDiagnostics(
        rule_trace_count=len(trace),
        rule_trace=trace,
        termination=optimization.rbo_result.termination,
        iterations=optimization.rbo_result.iterations,
        reordered_join_regions=optimization.reordered_regions,
        statistics_fallbacks=fallbacks,
        joins=joins,
        explain=optimization.explain(),
    )


def _logical_plan_by_id(plan: Any) -> dict[str, Any]:
    result = {plan.node_id: plan}
    for child in plan.children:
        result.update(_logical_plan_by_id(child))
    return result


def _schedule_metrics(
    query_id: str,
    schedules: tuple[ScheduleResult, ...],
    task_spills: Mapping[str, SpillMetrics],
) -> tuple[
    list[TaskMetrics],
    list[AttemptMetrics],
    list[RetryEvent],
    list[StageMetrics],
    list[int],
    list[int],
]:
    tasks: list[TaskMetrics] = []
    attempts: list[AttemptMetrics] = []
    retries: list[RetryEvent] = []
    stages: list[StageMetrics] = []
    partition_rows: dict[int, int] = {}
    partition_bytes: dict[int, int] = {}
    for schedule in schedules:
        stage_tasks: dict[str, list[TaskMetrics]] = {}
        stage_attempts: dict[str, list[Attempt]] = {}
        for task in sorted(schedule.tasks.values(), key=lambda item: item.task_id):
            outcome = schedule.outcomes.get(task.task_id)
            value = outcome.value if outcome is not None else None
            remote = _remote_task_metrics(value)
            rows, size = _outcome_size(value)
            spill = (
                _remote_spill_metrics(remote)
                if remote is not None
                else task_spills.get(task.task_id, SpillMetrics())
            )
            task_attempts = [schedule.attempts[item_id] for item_id in task.attempt_ids]
            stage_attempts.setdefault(task.stage_id, []).extend(task_attempts)
            duration = (
                remote.duration_seconds
                if remote is not None
                else _successful_attempt_duration(task_attempts)
            )
            shuffle = _task_shuffle_metrics(value, remote)
            item = TaskMetrics(
                query_id=query_id,
                stage_id=task.stage_id,
                task_id=task.task_id,
                state=task.state.value,
                partition_ordinal=task.partition.ordinal,
                attempt_count=len(task.attempt_ids),
                retry_count=max(len(task.attempt_ids) - 1, 0),
                input_rows=remote.input_rows if remote is not None else None,
                input_bytes=remote.input_bytes if remote is not None else None,
                output_rows=rows,
                output_bytes=size,
                duration_seconds=duration,
                shuffle_records_written=shuffle.records_written,
                shuffle_bytes_written=shuffle.bytes_written,
                shuffle_write_seconds=shuffle.write_seconds,
                shuffle_records_read=shuffle.records_read,
                shuffle_bytes_read=shuffle.bytes_read,
                shuffle_read_seconds=shuffle.read_seconds,
                spill_bytes=spill.spill_bytes,
                spill_files=spill.spill_files,
                spill_count=spill.spill_count,
                peak_memory_bytes=spill.peak_memory_bytes,
                external_sort_runs=spill.external_sort_runs,
                hash_partitions=spill.hash_partitions,
                sort_merge_fallbacks=spill.sort_merge_fallbacks,
                sort_aggregate_runs=spill.sort_aggregate_runs,
            )
            tasks.append(item)
            stage_tasks.setdefault(task.stage_id, []).append(item)
            attempts.extend(
                _attempt_metric(query_id, task.stage_id, attempt)
                for attempt in task_attempts
            )
            retries.extend(_retry_events(query_id, task.stage_id, task_attempts))
            manifests: list[ShuffleManifest] = []
            if isinstance(value, ShuffleManifest):
                manifests.append(value)
            elif isinstance(value, RemoteTaskResult):
                manifests.extend(
                    ShuffleManifest.model_validate(raw)
                    for raw in value.shuffle_manifests
                )
            for manifest in manifests:
                for file in manifest.files:
                    partition_rows[file.partition] = (
                        partition_rows.get(file.partition, 0) + file.row_count
                    )
                    partition_bytes[file.partition] = (
                        partition_bytes.get(file.partition, 0) + file.size_bytes
                    )
        for stage in sorted(schedule.stages.values(), key=lambda item: item.stage_id):
            items = stage_tasks.get(stage.stage_id, [])
            input_rows = [item.input_rows for item in items]
            input_bytes = [item.input_bytes for item in items]
            known_rows = [item.output_rows for item in items if item.output_rows is not None]
            known_bytes = [item.output_bytes for item in items if item.output_bytes is not None]
            durations = [item.duration_seconds for item in items]
            stages.append(
                StageMetrics(
                    query_id=query_id,
                    stage_id=stage.stage_id,
                    state=stage.state.value,
                    task_count=len(items),
                    succeeded_tasks=sum(
                        item.state == TaskState.SUCCEEDED.value for item in items
                    ),
                    failed_tasks=sum(item.state == TaskState.FAILED.value for item in items),
                    attempt_count=sum(item.attempt_count for item in items),
                    retry_count=sum(item.retry_count for item in items),
                    input_rows=_sum_optional(input_rows),
                    input_bytes=_sum_optional(input_bytes),
                    output_rows=sum(known_rows) if len(known_rows) == len(items) else None,
                    output_bytes=sum(known_bytes) if len(known_bytes) == len(items) else None,
                    wall_duration_seconds=_stage_wall_duration(
                        stage_attempts.get(stage.stage_id, [])
                    ),
                    max_task_duration_seconds=_max_optional(durations),
                    task_duration_sum_seconds=_sum_optional_float(durations),
                    shuffle_records_written=sum(
                        item.shuffle_records_written for item in items
                    ),
                    shuffle_bytes_written=sum(item.shuffle_bytes_written for item in items),
                    shuffle_write_seconds=sum(
                        item.shuffle_write_seconds for item in items
                    ),
                    shuffle_records_read=sum(item.shuffle_records_read for item in items),
                    shuffle_bytes_read=sum(item.shuffle_bytes_read for item in items),
                    shuffle_read_seconds=sum(item.shuffle_read_seconds for item in items),
                    spill_bytes=sum(item.spill_bytes for item in items),
                    spill_files=sum(item.spill_files for item in items),
                    spill_count=sum(item.spill_count for item in items),
                    peak_memory_bytes=max(
                        (item.peak_memory_bytes for item in items),
                        default=0,
                    ),
                    external_sort_runs=sum(item.external_sort_runs for item in items),
                    hash_partitions=sum(item.hash_partitions for item in items),
                    sort_merge_fallbacks=sum(
                        item.sort_merge_fallbacks for item in items
                    ),
                    sort_aggregate_runs=sum(
                        item.sort_aggregate_runs for item in items
                    ),
                )
            )
    return (
        tasks,
        sorted(attempts, key=lambda item: item.attempt_id),
        sorted(retries, key=lambda item: item.attempt_id),
        stages,
        [partition_rows[key] for key in sorted(partition_rows)],
        [partition_bytes[key] for key in sorted(partition_bytes)],
    )


def _remote_task_metrics(value: object | None) -> RemoteTaskMetrics | None:
    if not isinstance(value, RemoteTaskResult):
        return None
    return RemoteTaskMetrics.model_validate(value.metrics)


def _remote_spill_metrics(metrics: RemoteTaskMetrics) -> SpillMetrics:
    return SpillMetrics(
        spill_bytes=metrics.spill_bytes,
        spill_files=metrics.spill_files,
        spill_count=metrics.spill_count,
        peak_memory_bytes=metrics.peak_memory_bytes,
        external_sort_runs=metrics.external_sort_runs,
        hash_partitions=metrics.hash_partitions,
        sort_merge_fallbacks=metrics.sort_merge_fallbacks,
        sort_aggregate_runs=metrics.sort_aggregate_runs,
    )


def _task_shuffle_metrics(
    value: object | None,
    remote: RemoteTaskMetrics | None,
) -> ShuffleMetrics:
    if remote is not None:
        return ShuffleMetrics(
            records_written=remote.shuffle_records_written,
            bytes_written=remote.shuffle_bytes_written,
            records_read=remote.shuffle_records_read,
            bytes_read=remote.shuffle_bytes_read,
            write_seconds=remote.shuffle_write_seconds,
            read_seconds=remote.shuffle_read_seconds,
        )
    if isinstance(value, ShuffleManifest):
        return value.metrics
    return ShuffleMetrics()


def _successful_attempt_duration(attempts: list[Attempt]) -> float | None:
    successful = next(
        (attempt for attempt in reversed(attempts) if attempt.state.value == "succeeded"),
        None,
    )
    if (
        successful is None
        or successful.started_at is None
        or successful.finished_at is None
    ):
        return None
    return max((successful.finished_at - successful.started_at).total_seconds(), 0.0)


def _stage_wall_duration(attempts: list[Attempt]) -> float | None:
    starts = [attempt.started_at for attempt in attempts if attempt.started_at is not None]
    finishes = [
        attempt.finished_at for attempt in attempts if attempt.finished_at is not None
    ]
    if not starts or not finishes:
        return None
    return max((max(finishes) - min(starts)).total_seconds(), 0.0)


def _sum_optional(values: list[int | None]) -> int | None:
    if any(value is None for value in values):
        return None
    return sum(cast(int, value) for value in values)


def _sum_optional_float(values: list[float | None]) -> float | None:
    if any(value is None for value in values):
        return None
    return sum(cast(float, value) for value in values)


def _max_optional(values: list[float | None]) -> float | None:
    if not values or any(value is None for value in values):
        return None
    return max(cast(float, value) for value in values)


def _outcome_size(value: object | None) -> tuple[int | None, int | None]:
    if isinstance(value, RemoteTaskResult):
        metrics = RemoteTaskMetrics.model_validate(value.metrics)
        if value.artifact is not None:
            rows = (
                value.artifact.row_count
                if value.artifact.row_count is not None
                else metrics.output_rows
            )
            return rows, value.artifact.size_bytes
        return metrics.output_rows, metrics.output_bytes
    if isinstance(value, pa.Table):
        return value.num_rows, value.nbytes
    if isinstance(value, ShuffleManifest):
        return value.metrics.records_written, value.metrics.bytes_written
    table = getattr(value, "table", None)
    if isinstance(table, pa.Table):
        return table.num_rows, table.nbytes
    if isinstance(value, tuple) and value and isinstance(value[0], pa.Table):
        return value[0].num_rows, value[0].nbytes
    return None, None


def _attempt_metric(
    query_id: str,
    stage_id: str,
    attempt: Attempt,
) -> AttemptMetrics:
    duration = None
    if attempt.started_at is not None and attempt.finished_at is not None:
        duration = max((attempt.finished_at - attempt.started_at).total_seconds(), 0.0)
    return AttemptMetrics(
        query_id=query_id,
        stage_id=stage_id,
        task_id=attempt.task_id,
        attempt_id=attempt.attempt_id,
        worker_id=attempt.worker_id,
        state=attempt.state.value,
        attempt_number=attempt.attempt_number,
        started_at=attempt.started_at,
        finished_at=attempt.finished_at,
        duration_seconds=duration,
        error=attempt.error,
    )


def _retry_events(
    query_id: str,
    stage_id: str,
    attempts: list[Attempt],
) -> list[RetryEvent]:
    result: list[RetryEvent] = []
    for previous, current in pairwise(attempts):
        result.append(
            RetryEvent(
                query_id=query_id,
                stage_id=stage_id,
                task_id=current.task_id,
                attempt_id=current.attempt_id,
                worker_id=current.worker_id,
                attempt_number=current.attempt_number,
                previous_attempt_id=previous.attempt_id,
                previous_state=previous.state.value,
                previous_error=previous.error,
                timestamp=current.started_at,
            )
        )
    return result


def _timeline(
    attempts: list[AttemptMetrics],
    logs: list[StructuredLogEvent],
) -> list[TimelineEvent]:
    events: list[TimelineEvent] = []
    for attempt in attempts:
        if attempt.started_at is not None:
            events.append(
                TimelineEvent(
                    timestamp=attempt.started_at,
                    event="attempt_started",
                    state="running",
                    query_id=attempt.query_id,
                    stage_id=attempt.stage_id,
                    task_id=attempt.task_id,
                    attempt_id=attempt.attempt_id,
                    worker_id=attempt.worker_id,
                    message=f"Attempt {attempt.attempt_id} started.",
                )
            )
        if attempt.finished_at is not None:
            events.append(
                TimelineEvent(
                    timestamp=attempt.finished_at,
                    event="attempt_finished",
                    state=attempt.state,
                    query_id=attempt.query_id,
                    stage_id=attempt.stage_id,
                    task_id=attempt.task_id,
                    attempt_id=attempt.attempt_id,
                    worker_id=attempt.worker_id,
                    message=f"Attempt {attempt.attempt_id} finished as {attempt.state}.",
                )
            )
    events.extend(
        TimelineEvent(
            timestamp=event.timestamp,
            event=event.event,
            state=event.level.value,
            query_id=event.query_id,
            stage_id=event.stage_id,
            task_id=event.task_id,
            attempt_id=event.attempt_id,
            worker_id=event.worker_id,
            message=event.message,
        )
        for event in logs
    )
    return sorted(
        events,
        key=lambda item: (
            item.timestamp,
            item.event,
            item.attempt_id or "",
        ),
    )


def _plan_text(plan: PlanNode, indent: int = 0) -> str:
    properties = ", ".join(
        f"{key}={value}" for key, value in sorted(plan.properties.items())
    )
    line = f"{'  ' * indent}{plan.node_type.value}[{plan.node_id}]"
    if properties:
        line += f" ({properties})"
    return "\n".join(
        [line, *(_plan_text(child, indent + 1) for child in plan.children)]
    )


def _display(value: int | None) -> str:
    return "unknown" if value is None else str(value)


def _display_float(value: float | None) -> str:
    return "unknown" if value is None else f"{value:.6f}"

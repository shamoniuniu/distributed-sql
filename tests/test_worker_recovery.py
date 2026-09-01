from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import pyarrow as pa
import pytest

from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import (
    AttemptState,
    Partition,
    PlanNode,
    PlanNodeType,
    Stage,
    Task,
    Worker,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
)
from distributed_sql.coordinator.registry import WorkerRegistry
from distributed_sql.execution import (
    CancellationToken,
    LogicalWorker,
    RetryPolicy,
    ShuffleManifest,
    ShuffleStore,
    StageGraph,
    TaskScheduler,
)
from distributed_sql.execution.scheduler import (
    AttemptStarted,
    CancellationConfirmationError,
    ScheduledWork,
)


class _DelayedStartWorker(LogicalWorker):
    async def execute(
        self,
        runner: ScheduledWork,
        attempt_id: str,
        cancellation: CancellationToken,
        on_started: AttemptStarted,
    ) -> object:
        await asyncio.sleep(0.03)
        return await super().execute(runner, attempt_id, cancellation, on_started)


class _CancellationFailingWorker:
    worker_id = "worker-1"
    slots = 1
    max_running = 1
    active = True

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def execute(
        self,
        _runner: ScheduledWork,
        _attempt_id: str,
        cancellation: CancellationToken,
        on_started: AttemptStarted,
    ) -> object:
        on_started(datetime.now(UTC))
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            raise CancellationConfirmationError("lost Worker cannot confirm cancellation") from None
        if cancellation.cancelled:
            raise CancellationConfirmationError("user cancellation was not confirmed")
        return "unexpected"

    async def discard(self, _attempt_id: str) -> None:
        return


class _ControlledRegistry:
    def __init__(self) -> None:
        self.lost_workers: set[str] = set()

    async def expire_leases(self) -> list[str]:
        return sorted(self.lost_workers)

    async def list_workers(self) -> list[Worker]:
        return [
            Worker(
                worker_id=worker_id,
                endpoint=f"http://{worker_id}",
                state=(
                    WorkerState.LOST
                    if worker_id in self.lost_workers
                    else WorkerState.ACTIVE
                ),
                slots=1,
                available_slots=1,
                memory_limit_bytes=1024,
            )
            for worker_id in ("worker-1", "worker-2")
        ]


def _graph(query_id: str = "query-recovery") -> StageGraph:
    stage = Stage(
        stage_id="stage-shuffle",
        query_id=query_id,
        plan=PlanNode(node_id="output", node_type=PlanNodeType.OUTPUT),
    )
    task = Task(
        task_id="task-partition-0",
        query_id=query_id,
        stage_id=stage.stage_id,
        partition=Partition(
            partition_id="partition-0",
            ordinal=0,
            location="",
        ),
    )
    return StageGraph(stage.stage_id, (stage,), (task,))


async def _heartbeat_until_stopped(
    registry: WorkerRegistry,
    worker_id: str,
    lease_id: str,
    stop: asyncio.Event,
) -> None:
    while not stop.is_set():
        await registry.heartbeat(
            worker_id,
            WorkerHeartbeat(lease_id=lease_id, available_slots=1),
        )
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=0.02)


@pytest.mark.asyncio
async def test_worker_lease_loss_retries_without_duplicate_or_missing_shuffle_rows(
    tmp_path: Path,
) -> None:
    registry = WorkerRegistry(lease_ttl_seconds=1.0)
    registered = [
        await registry.register(
            WorkerRegistration(
                worker_id=worker_id,
                endpoint=f"http://{worker_id}",
                slots=1,
                memory_limit_bytes=1024,
            )
        )
        for worker_id in ("worker-1", "worker-2")
    ]
    stop_first = asyncio.Event()
    stop_second = asyncio.Event()
    heartbeats = [
        asyncio.create_task(
            _heartbeat_until_stopped(
                registry,
                worker.worker_id,
                worker.lease_id or "",
                stop,
            )
        )
        for worker, stop in zip(registered, (stop_first, stop_second), strict=True)
    ]
    workers = [LogicalWorker("worker-1", 1), LogicalWorker("worker-2", 1)]
    scheduler = TaskScheduler(
        workers,
        registry=registry,
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=0,
            attempt_timeout_seconds=2,
            lease_poll_interval_seconds=0.01,
        ),
    )
    shuffle = ShuffleStore(
        str(tmp_path / "shuffle"),
        ObjectStoreRouter(LocalObjectStore()),
    )
    graph = _graph()
    first_published = asyncio.Event()
    source = pa.table({"id": [1, 2, 3, 4]})
    await asyncio.sleep(0.03)

    async def write_shuffle(attempt_id: str, _cancellation: object) -> ShuffleManifest:
        manifest = shuffle.write(
            query_id="query-recovery",
            stage_id="stage-shuffle",
            task_id="task-partition-0",
            attempt_id=attempt_id,
            table=source,
            partition_count=1,
        )
        if attempt_id.endswith("000"):
            first_published.set()
            await asyncio.sleep(5)
        return manifest

    query = asyncio.create_task(
        scheduler.run(graph, {"task-partition-0": write_shuffle})
    )
    try:
        await asyncio.wait_for(first_published.wait(), timeout=1)
        stop_first.set()
        await heartbeats[0]
        result = await asyncio.wait_for(query, timeout=3)
    finally:
        stop_first.set()
        stop_second.set()
        await asyncio.gather(*heartbeats)
        if not query.done():
            query.cancel()
            with suppress(asyncio.CancelledError):
                await query

    task = result.tasks["task-partition-0"]
    first, retry = (result.attempts[attempt_id] for attempt_id in task.attempt_ids)
    assert first.state is AttemptState.LOST
    assert first.worker_id == "worker-1"
    assert retry.state is AttemptState.SUCCEEDED
    assert retry.worker_id == "worker-2"
    assert shuffle.load_manifest(
        "query-recovery",
        "stage-shuffle",
        "task-partition-0",
        first.attempt_id,
    ).attempt_id == first.attempt_id

    accepted = result.outcomes["task-partition-0"].value
    assert isinstance(accepted, ShuffleManifest)
    assert accepted.attempt_id == retry.attempt_id
    recovered, _ = shuffle.read_partition([accepted], 0)
    assert recovered.to_pylist() == source.to_pylist()


@pytest.mark.asyncio
async def test_lease_loss_retry_ignores_failed_cancellation_confirmation() -> None:
    failed_worker = _CancellationFailingWorker()
    registry = _ControlledRegistry()
    scheduler = TaskScheduler(
        [failed_worker, LogicalWorker("worker-2", 1)],
        registry=registry,
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=0,
            attempt_timeout_seconds=2,
            lease_poll_interval_seconds=0.01,
        ),
    )
    graph = _graph("query-lease-cancel")

    async def succeed(_attempt_id: str, _cancellation: object) -> str:
        return "recovered"

    pending = asyncio.create_task(
        scheduler.run(graph, {"task-partition-0": succeed})
    )
    await asyncio.wait_for(failed_worker.started.wait(), timeout=1)
    registry.lost_workers.add("worker-1")
    result = await asyncio.wait_for(pending, timeout=1)

    task = result.tasks["task-partition-0"]
    first, retry = [result.attempts[item] for item in task.attempt_ids]
    assert first.state is AttemptState.LOST
    assert first.worker_id == "worker-1"
    assert retry.state is AttemptState.SUCCEEDED
    assert retry.worker_id == "worker-2"
    assert result.outcomes[task.task_id].value == "recovered"


@pytest.mark.asyncio
async def test_user_cancellation_still_requires_remote_confirmation() -> None:
    worker = _CancellationFailingWorker()
    scheduler = TaskScheduler([worker])
    graph = _graph("query-user-cancel")
    pending = asyncio.create_task(
        scheduler.run(graph, {"task-partition-0": object()})
    )
    await asyncio.wait_for(worker.started.wait(), timeout=1)

    scheduler.cancel("query-user-cancel")
    worker.release.set()

    with pytest.raises(CancellationConfirmationError):
        await asyncio.wait_for(pending, timeout=1)


@pytest.mark.asyncio
async def test_retry_timeout_backoff_and_final_error_include_safe_context() -> None:
    starts: list[tuple[str, float]] = []
    finishes: list[tuple[str, float]] = []
    scheduler = TaskScheduler(
        [_DelayedStartWorker("worker-1", 1), _DelayedStartWorker("worker-2", 1)],
        retry_policy=RetryPolicy(
            max_attempts=2,
            backoff_seconds=0.03,
            attempt_timeout_seconds=0.02,
            lease_poll_interval_seconds=0.005,
        ),
    )
    graph = _graph("query-timeout")

    async def hang(attempt_id: str, _cancellation: object) -> None:
        starts.append((attempt_id, perf_counter()))
        try:
            await asyncio.sleep(5)
        finally:
            finishes.append((attempt_id, perf_counter()))

    with pytest.raises(DistributedSQLError) as raised:
        await scheduler.run(graph, {"task-partition-0": hang})

    error = raised.value
    assert error.code is ErrorCode.TASK_FAILED
    assert error.context == {
        "query_id": "query-timeout",
        "stage_id": "stage-shuffle",
        "task_id": "task-partition-0",
        "attempt_id": "task-partition-0-attempt-001",
        "worker_id": "worker-2",
        "attempt_count": 2,
        "failure_kind": "timeout",
    }
    assert [attempt_id for attempt_id, _ in starts] == [
        "task-partition-0-attempt-000",
        "task-partition-0-attempt-001",
    ]
    assert [attempt_id for attempt_id, _ in finishes] == [
        "task-partition-0-attempt-000",
        "task-partition-0-attempt-001",
    ]
    tolerance = 0.005
    for (_, started_at), (_, finished_at) in zip(starts, finishes, strict=True):
        assert finished_at - started_at >= 0.02 - tolerance
    assert starts[1][1] - finishes[0][1] >= 0.03 - tolerance
    assert error.message == "Worker Task execution failed."
    assert "exceeded" not in error.message

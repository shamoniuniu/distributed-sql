"""Dependency-aware partition scheduler for explicit logical Workers."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from typing import Protocol

from distributed_sql.common.exceptions import DistributedSQLError, public_task_error
from distributed_sql.common.protocol import (
    Attempt,
    AttemptState,
    Stage,
    StageState,
    Task,
    TaskState,
    Worker,
    WorkerState,
)

from .operators import CancellationToken, ExecutionCancelled
from .physical import StageGraph

type TaskRunner = Callable[[str, CancellationToken], object | Awaitable[object]]
type ScheduledWork = TaskRunner | object
type AttemptStarted = Callable[[datetime], None]


class WorkerLeaseRegistry(Protocol):
    async def expire_leases(self) -> list[str]: ...

    async def list_workers(self) -> list[Worker]: ...


class WorkerBackend(Protocol):
    worker_id: str
    slots: int
    max_running: int

    @property
    def active(self) -> bool: ...

    async def execute(
        self,
        work: ScheduledWork,
        attempt_id: str,
        cancellation: CancellationToken,
        on_started: AttemptStarted,
    ) -> object: ...

    async def discard(self, attempt_id: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 3
    backoff_seconds: float = 0.05
    attempt_timeout_seconds: float = 30.0
    lease_poll_interval_seconds: float = 0.05

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if self.backoff_seconds < 0:
            raise ValueError("backoff_seconds cannot be negative")
        if self.attempt_timeout_seconds <= 0:
            raise ValueError("attempt_timeout_seconds must be positive")
        if self.lease_poll_interval_seconds <= 0:
            raise ValueError("lease_poll_interval_seconds must be positive")


class WorkerLostError(RuntimeError):
    pass


class CancellationConfirmationError(RuntimeError):
    """Raised when a dispatched attempt cannot confirm cancellation in time."""


def _failure_kind(exc: Exception) -> str:
    if isinstance(exc, DistributedSQLError):
        return "domain"
    if isinstance(exc, WorkerLostError):
        return "network"
    if isinstance(exc, TimeoutError):
        return "timeout"
    return "worker_execution"


def _invoke_runner(
    runner: TaskRunner,
    attempt_id: str,
    cancellation: CancellationToken,
    on_started: AttemptStarted,
) -> object | Awaitable[object]:
    on_started(datetime.now(UTC))
    return runner(attempt_id, cancellation)


def _mark_attempt_started(
    started: asyncio.Event,
    attempt: Attempt,
    started_at: datetime,
) -> None:
    if started.is_set():
        return
    attempt.state = AttemptState.RUNNING
    attempt.started_at = started_at
    started.set()


def _notify_started(
    loop: asyncio.AbstractEventLoop,
    on_started: AttemptStarted,
    started_at: datetime,
) -> None:
    loop.call_soon_threadsafe(on_started, started_at)


@dataclass(slots=True)
class TaskOutcome:
    task_id: str
    attempt_id: str
    worker_id: str
    value: object


@dataclass(slots=True)
class ScheduleResult:
    stages: dict[str, Stage]
    tasks: dict[str, Task]
    attempts: dict[str, Attempt]
    outcomes: dict[str, TaskOutcome]
    max_running_by_worker: dict[str, int]


@dataclass(slots=True)
class LogicalWorker:
    """In-process execution backend with the same slot semantics as a Worker."""

    worker_id: str
    slots: int
    _semaphore: asyncio.Semaphore = field(init=False, repr=False)
    _running: int = field(default=0, init=False, repr=False)
    _active: bool = field(default=True, init=False, repr=False)
    max_running: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if self.slots < 1:
            raise ValueError("worker slots must be positive")
        self._semaphore = asyncio.Semaphore(self.slots)

    @property
    def available_slots(self) -> int:
        return self.slots - self._running

    @property
    def active(self) -> bool:
        return self._active

    def terminate(self) -> None:
        self._active = False

    async def execute(
        self,
        runner: ScheduledWork,
        attempt_id: str,
        cancellation: CancellationToken,
        on_started: AttemptStarted,
    ) -> object:
        if not callable(runner):
            raise TypeError("LogicalWorker only accepts callable Task runners")
        async with self._semaphore:
            if not self._active:
                raise WorkerLostError(f"Worker {self.worker_id!r} was terminated.")
            cancellation.check()
            self._running += 1
            self.max_running = max(self.max_running, self._running)
            try:
                value: object | Awaitable[object]
                if inspect.iscoroutinefunction(runner):
                    on_started(datetime.now(UTC))
                    value = runner(attempt_id, cancellation)
                else:
                    loop = asyncio.get_running_loop()
                    value = await asyncio.to_thread(
                        _invoke_runner,
                        runner,
                        attempt_id,
                        cancellation,
                        partial(_notify_started, loop, on_started),
                    )
                if inspect.isawaitable(value):
                    return await value
                return value
            finally:
                self._running -= 1

    async def discard(self, attempt_id: str) -> None:
        del attempt_id


class TaskScheduler:
    """Run Stage dependencies in order and partition Tasks within Worker slots."""

    def __init__(
        self,
        workers: Sequence[WorkerBackend],
        *,
        registry: WorkerLeaseRegistry | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        if len({worker.worker_id for worker in workers}) != len(workers):
            raise ValueError("worker IDs must be unique")
        if not workers:
            raise ValueError("at least one Worker is required")
        self.workers = sorted(workers, key=lambda item: item.worker_id)
        self._registry = registry
        self._retry_policy = retry_policy or RetryPolicy()
        self._cancellations: dict[str, CancellationToken] = {}
        self._next_worker = 0

    def cancel(self, query_id: str) -> None:
        token = self._cancellations.get(query_id)
        if token is not None:
            token.cancel()

    async def run(
        self,
        graph: StageGraph,
        runners: Mapping[str, ScheduledWork],
    ) -> ScheduleResult:
        stages = {item.stage_id: item.model_copy(deep=True) for item in graph.stages}
        tasks = {item.task_id: item.model_copy(deep=True) for item in graph.tasks}
        attempts: dict[str, Attempt] = {}
        outcomes: dict[str, TaskOutcome] = {}
        token = CancellationToken()
        self._cancellations[stages[graph.root_stage_id].query_id] = token
        remaining = set(stages)
        try:
            while remaining:
                ready = [
                    stage
                    for stage_id, stage in stages.items()
                    if stage_id in remaining
                    and all(
                        stages[dependency].state is StageState.SUCCEEDED
                        for dependency in stage.dependency_stage_ids
                    )
                ]
                if not ready:
                    raise RuntimeError("Stage DAG has a cycle or failed dependency")
                for stage in sorted(ready, key=lambda item: item.stage_id):
                    token.check()
                    stage.state = StageState.RUNNING
                    stage_tasks = sorted(
                        (task for task in tasks.values() if task.stage_id == stage.stage_id),
                        key=lambda item: item.partition.ordinal,
                    )
                    executions = [
                        asyncio.create_task(
                            self._run_task(task, runners[task.task_id], token, attempts),
                            name=f"schedule-{task.task_id}",
                        )
                        for task in stage_tasks
                    ]
                    try:
                        results = await asyncio.gather(*executions)
                    except (asyncio.CancelledError, ExecutionCancelled):
                        settled = await asyncio.gather(*executions, return_exceptions=True)
                        stage.state = StageState.CANCELED
                        for task in stage_tasks:
                            if task.state in {TaskState.PENDING, TaskState.RUNNING}:
                                task.state = TaskState.CANCELED
                        confirmation_failure = next(
                            (
                                item
                                for item in settled
                                if isinstance(item, CancellationConfirmationError)
                            ),
                            None,
                        )
                        if confirmation_failure is not None:
                            raise confirmation_failure from None
                        raise
                    except Exception:
                        stage.state = StageState.FAILED
                        raise
                    outcomes.update((result.task_id, result) for result in results)
                    stage.state = StageState.SUCCEEDED
                    remaining.remove(stage.stage_id)
        except ExecutionCancelled:
            for stage in stages.values():
                if stage.state is StageState.PENDING:
                    stage.state = StageState.CANCELED
            for task in tasks.values():
                if task.state is TaskState.PENDING:
                    task.state = TaskState.CANCELED
        finally:
            self._cancellations.pop(stages[graph.root_stage_id].query_id, None)
        return ScheduleResult(
            stages,
            tasks,
            attempts,
            outcomes,
            {worker.worker_id: worker.max_running for worker in self.workers},
        )

    async def _run_task(
        self,
        task: Task,
        runner: ScheduledWork,
        cancellation: CancellationToken,
        attempts: dict[str, Attempt],
    ) -> TaskOutcome:
        task.state = TaskState.RUNNING
        previous_workers: set[str] = set()
        last_attempt: Attempt | None = None
        last_error: Exception | None = None
        for attempt_number in range(self._retry_policy.max_attempts):
            cancellation.check()
            try:
                worker = await self._choose_worker(previous_workers)
            except WorkerLostError as exc:
                last_error = exc
                if last_attempt is None:
                    attempt_id = f"{task.task_id}-attempt-{len(task.attempt_ids):03d}"
                    now = datetime.now(UTC)
                    last_attempt = Attempt(
                        attempt_id=attempt_id,
                        task_id=task.task_id,
                        attempt_number=len(task.attempt_ids),
                        state=AttemptState.FAILED,
                        started_at=now,
                        finished_at=now,
                        error={"type": type(exc).__name__, "message": str(exc)},
                    )
                    attempts[attempt_id] = last_attempt
                    task.attempt_ids.append(attempt_id)
                break
            previous_workers.add(worker.worker_id)
            attempt_id = f"{task.task_id}-attempt-{len(task.attempt_ids):03d}"
            attempt = Attempt(
                attempt_id=attempt_id,
                task_id=task.task_id,
                attempt_number=len(task.attempt_ids),
                worker_id=worker.worker_id,
            )
            last_attempt = attempt
            attempts[attempt_id] = attempt
            task.attempt_ids.append(attempt_id)
            started = asyncio.Event()
            on_started = partial(_mark_attempt_started, started, attempt)
            try:
                value = await self._execute_attempt(
                    worker,
                    runner,
                    attempt_id,
                    cancellation,
                    started,
                    on_started,
                )
                cancellation.check()
            except ExecutionCancelled:
                attempt.state = AttemptState.CANCELED
                task.state = TaskState.CANCELED
                raise
            except asyncio.CancelledError:
                attempt.state = AttemptState.CANCELED
                task.state = TaskState.CANCELED
                raise
            except CancellationConfirmationError:
                attempt.state = AttemptState.FAILED
                task.state = TaskState.FAILED
                raise
            except Exception as exc:
                last_error = exc
                with suppress(Exception):
                    await worker.discard(attempt_id)
                attempt.state = (
                    AttemptState.LOST if isinstance(exc, WorkerLostError) else AttemptState.FAILED
                )
                attempt.error = public_task_error(
                    exc,
                    context={
                        "query_id": task.query_id,
                        "stage_id": task.stage_id,
                        "task_id": task.task_id,
                        "attempt_id": attempt_id,
                        "worker_id": worker.worker_id,
                        "failure_kind": _failure_kind(exc),
                    },
                ).model_dump(mode="json")
                if attempt_number + 1 < self._retry_policy.max_attempts:
                    await self._retry_backoff(attempt_number, cancellation)
                    continue
                break
            finally:
                attempt.finished_at = datetime.now(UTC)
            attempt.state = AttemptState.SUCCEEDED
            task.state = TaskState.SUCCEEDED
            return TaskOutcome(task.task_id, attempt_id, worker.worker_id, value)

        assert last_attempt is not None
        assert last_error is not None
        task.state = TaskState.FAILED
        detail = public_task_error(
            last_error,
            context={
                "query_id": task.query_id,
                "stage_id": task.stage_id,
                "task_id": task.task_id,
                "attempt_id": last_attempt.attempt_id,
                "worker_id": last_attempt.worker_id,
                "attempt_count": len(task.attempt_ids),
                "failure_kind": _failure_kind(last_error),
            },
        )
        raise DistributedSQLError(
            detail.code,
            detail.message,
            status_code=(
                last_error.status_code if isinstance(last_error, DistributedSQLError) else 500
            ),
            context=detail.context,
        ) from last_error

    async def _execute_attempt(
        self,
        worker: WorkerBackend,
        runner: ScheduledWork,
        attempt_id: str,
        cancellation: CancellationToken,
        started: asyncio.Event,
        on_started: AttemptStarted,
    ) -> object:
        execution = asyncio.create_task(
            worker.execute(runner, attempt_id, cancellation, on_started),
            name=f"execute-{attempt_id}",
        )
        health = asyncio.create_task(
            self._wait_until_worker_lost(worker),
            name=f"lease-{attempt_id}",
        )
        start_wait = asyncio.create_task(started.wait(), name=f"start-{attempt_id}")
        worker_lost = False
        try:
            ready, _ = await asyncio.wait(
                {execution, health, start_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if health in ready:
                await health
                worker_lost = True
                raise WorkerLostError(f"Worker {worker.worker_id!r} lost its heartbeat lease.")
            if execution in ready:
                return await execution
            loop = asyncio.get_running_loop()
            deadline = loop.time() + self._retry_policy.attempt_timeout_seconds
            while True:
                done, _ = await asyncio.wait(
                    {execution, health},
                    timeout=max(0.0, deadline - loop.time()),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if done:
                    break
                if loop.time() >= deadline:
                    raise TimeoutError(
                        f"Attempt {attempt_id!r} exceeded "
                        f"{self._retry_policy.attempt_timeout_seconds:g} seconds."
                    )
            if health in done:
                await health
                worker_lost = True
                raise WorkerLostError(f"Worker {worker.worker_id!r} lost its heartbeat lease.")
            value = await execution
            if not await self._worker_is_healthy(worker):
                raise WorkerLostError(f"Worker {worker.worker_id!r} lost its heartbeat lease.")
            return value
        finally:
            for pending in (execution, health, start_wait):
                if not pending.done():
                    pending.cancel()
            for pending in (execution, health, start_wait):
                try:
                    await pending
                except asyncio.CancelledError:
                    pass
                except CancellationConfirmationError:
                    if not worker_lost:
                        raise

    async def _wait_until_worker_lost(self, worker: WorkerBackend) -> None:
        poll = asyncio.Event()
        while await self._worker_is_healthy(worker):
            with suppress(TimeoutError):
                await asyncio.wait_for(
                    poll.wait(),
                    timeout=self._retry_policy.lease_poll_interval_seconds,
                )

    async def _worker_is_healthy(self, worker: WorkerBackend) -> bool:
        if not worker.active:
            return False
        if self._registry is None:
            return True
        await self._registry.expire_leases()
        workers = {item.worker_id: item for item in await self._registry.list_workers()}
        registered = workers.get(worker.worker_id)
        return registered is not None and registered.state is WorkerState.ACTIVE

    async def _healthy_workers(self) -> list[WorkerBackend]:
        if self._registry is not None:
            await self._registry.expire_leases()
            registered = {
                item.worker_id: item.state for item in await self._registry.list_workers()
            }
            return [
                worker
                for worker in self.workers
                if worker.active and registered.get(worker.worker_id) is WorkerState.ACTIVE
            ]
        return [worker for worker in self.workers if worker.active]

    async def _choose_worker(
        self,
        previous_workers: set[str],
    ) -> WorkerBackend:
        healthy = await self._healthy_workers()
        if not healthy:
            raise WorkerLostError("No healthy Worker is available.")
        preferred = [worker for worker in healthy if worker.worker_id not in previous_workers]
        candidates = preferred or healthy
        # Stable round-robin gives deterministic partition ownership while each
        # Worker's semaphore enforces its independently configured slot count.
        worker = candidates[self._next_worker % len(candidates)]
        self._next_worker += 1
        return worker

    async def _retry_backoff(
        self,
        attempt_number: int,
        cancellation: CancellationToken,
    ) -> None:
        delay = self._retry_policy.backoff_seconds * (2**attempt_number)
        if delay:
            await asyncio.sleep(delay)
        cancellation.check()

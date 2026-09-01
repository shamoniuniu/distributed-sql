"""HTTP client backend for executing scheduled Tasks on registered Workers."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
from pydantic import JsonValue

from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.exceptions import DistributedSQLError, status_code_for_error
from distributed_sql.common.protocol import (
    AttemptState,
    RemoteTaskOperation,
    RemoteTaskResult,
    RemoteTaskStatus,
    RemoteTaskSubmission,
    Worker,
    WorkerListResponse,
)
from distributed_sql.execution.operators import CancellationToken
from distributed_sql.execution.scheduler import (
    AttemptStarted,
    CancellationConfirmationError,
    WorkerLostError,
)


@dataclass(frozen=True, slots=True)
class RemoteTaskCommand:
    task_id: str
    query_id: str
    stage_id: str
    operation: RemoteTaskOperation
    payload: dict[str, JsonValue]
    output_root: str | Path


@dataclass(slots=True)
class RemoteWorker:
    worker_id: str
    slots: int
    endpoint: str
    poll_interval_seconds: float = 0.02
    cancellation_timeout_seconds: float = 5.0
    stores: ObjectStoreRouter | None = None
    auth_token: str | None = None
    _semaphore: asyncio.Semaphore = field(init=False, repr=False)
    _running: int = field(default=0, init=False, repr=False)
    _active: bool = field(default=True, init=False, repr=False)
    _outputs: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    max_running: int = field(default=0, init=False)

    def __post_init__(self) -> None:
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
        work: object,
        attempt_id: str,
        cancellation: CancellationToken,
        on_started: AttemptStarted,
    ) -> RemoteTaskResult:
        if not isinstance(work, RemoteTaskCommand):
            raise TypeError("RemoteWorker only accepts RemoteTaskCommand work")
        command = work
        async with self._semaphore:
            cancellation.check()
            self._running += 1
            self.max_running = max(self.max_running, self._running)
            query_key = hashlib.sha256(command.query_id.encode()).hexdigest()[:12]
            attempt_key = hashlib.sha256(attempt_id.encode()).hexdigest()[:16]
            output = _join_location(
                str(command.output_root),
                query_key,
                attempt_key,
                "result.parquet",
            )
            submission = RemoteTaskSubmission(
                task_id=command.task_id,
                attempt_id=attempt_id,
                query_id=command.query_id,
                stage_id=command.stage_id,
                operation=command.operation,
                payload=command.payload,
                output_location=output,
            )
            try:
                async with httpx.AsyncClient(
                    base_url=self.endpoint,
                    timeout=max(2.0, self.cancellation_timeout_seconds + 1.0),
                    trust_env=False,
                    headers=self._headers(),
                ) as client:
                    response = await client.post(
                        "/api/v1/tasks",
                        json=submission.model_dump(mode="json"),
                    )
                    response.raise_for_status()
                    status = RemoteTaskStatus.model_validate(response.json())
                    while True:
                        if cancellation.cancelled:
                            await self._cancel(client, attempt_id)
                            cancellation.check()
                        if status.state is not AttemptState.CREATED:
                            on_started(status.started_at or datetime.now(UTC))
                        if status.state is AttemptState.SUCCEEDED:
                            if status.result is None:
                                raise RuntimeError("Worker succeeded without a Task result")
                            if status.result.artifact is None:
                                return status.result
                            remote_artifact = status.result.artifact
                            stores = self.stores or ObjectStoreRouter(LocalObjectStore())
                            payload = stores.for_location(remote_artifact.location).read_bytes(
                                remote_artifact.location
                            )
                            checksum = hashlib.sha256(payload).hexdigest()
                            if checksum != remote_artifact.checksum:
                                raise RuntimeError("Worker result artifact checksum mismatch")
                            self._outputs[attempt_id] = remote_artifact.location
                            return status.result
                        if status.state is AttemptState.CANCELED:
                            cancellation.check()
                            raise RuntimeError(f"Remote attempt {attempt_id!r} was canceled")
                        if status.state is AttemptState.FAILED:
                            if status.error is None:
                                raise RuntimeError("Worker Task failed without an error detail")
                            context = dict(status.error.context)
                            context.update(
                                {
                                    "attempt_id": attempt_id,
                                    "worker_id": self.worker_id,
                                }
                            )
                            raise DistributedSQLError(
                                status.error.code,
                                status.error.message,
                                status_code=status_code_for_error(status.error.code),
                                context=context,
                            )
                        await asyncio.sleep(self.poll_interval_seconds)
                        response = await client.get(f"/api/v1/tasks/{attempt_id}")
                        response.raise_for_status()
                        status = RemoteTaskStatus.model_validate(response.json())
            except asyncio.CancelledError:
                async with httpx.AsyncClient(
                    base_url=self.endpoint,
                    timeout=max(2.0, self.cancellation_timeout_seconds + 1.0),
                    trust_env=False,
                    headers=self._headers(),
                ) as client:
                    await asyncio.shield(self._cancel(client, attempt_id))
                raise
            except httpx.HTTPError as exc:
                raise WorkerLostError(f"Worker {self.worker_id!r} endpoint failed: {exc}") from exc
            finally:
                self._running -= 1

    async def discard(self, attempt_id: str) -> None:
        async with httpx.AsyncClient(
            base_url=self.endpoint,
            timeout=max(2.0, self.cancellation_timeout_seconds + 1.0),
            trust_env=False,
            headers=self._headers(),
        ) as client:
            await self._cancel(client, attempt_id)
        location = self._outputs.pop(attempt_id, None)
        if location is not None:
            stores = self.stores or ObjectStoreRouter(LocalObjectStore())
            stores.for_location(location).delete(location)

    def _headers(self) -> dict[str, str]:
        if self.auth_token is None:
            return {}
        return {"Authorization": f"Bearer {self.auth_token}"}

    @staticmethod
    async def _cancel(client: httpx.AsyncClient, attempt_id: str) -> None:
        try:
            response = await client.delete(f"/api/v1/tasks/{attempt_id}")
            response.raise_for_status()
            status = RemoteTaskStatus.model_validate(response.json())
            if status.state not in {
                AttemptState.CANCELED,
                AttemptState.SUCCEEDED,
                AttemptState.FAILED,
            }:
                raise CancellationConfirmationError(
                    f"Remote attempt {attempt_id!r} did not reach a terminal state."
                )
        except (httpx.HTTPError, ValueError) as exc:
            raise CancellationConfirmationError(
                f"Remote attempt {attempt_id!r} cancellation could not be confirmed: {exc}"
            ) from exc


def table_payload(tables: dict[str, Any]) -> list[JsonValue]:
    return [
        table.model_dump(mode="json", by_alias=True)
        for table in sorted(tables.values(), key=lambda item: (item.namespace, item.name))
    ]


@dataclass(slots=True)
class HTTPWorkerRegistry:
    coordinator_url: str

    async def expire_leases(self) -> list[str]:
        return []

    async def list_workers(self) -> list[Worker]:
        async with httpx.AsyncClient(
            base_url=self.coordinator_url,
            timeout=2.0,
            trust_env=False,
        ) as client:
            response = await client.get("/api/v1/workers")
            response.raise_for_status()
            return WorkerListResponse.model_validate(response.json()).workers


def _join_location(base: str, *parts: str) -> str:
    parsed = urlsplit(base)
    relative = PurePosixPath(*parts).as_posix()
    if parsed.scheme in {"file", "s3"}:
        path = f"{parsed.path.rstrip('/')}/{relative}"
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return str(Path(base).joinpath(*parts).resolve())

"""In-memory Worker registry with renewable leases."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import (
    Worker,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
)


class WorkerRegistry:
    """Owns Worker membership until persistent scheduling state is introduced."""

    def __init__(
        self,
        lease_ttl_seconds: float,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._lease_ttl = timedelta(seconds=lease_ttl_seconds)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._workers: dict[str, Worker] = {}
        self._lock = asyncio.Lock()

    @property
    def lease_ttl_seconds(self) -> float:
        return self._lease_ttl.total_seconds()

    async def register(self, registration: WorkerRegistration) -> Worker:
        now = self._clock()
        worker = Worker(
            worker_id=registration.worker_id,
            endpoint=registration.endpoint,
            state=WorkerState.ACTIVE,
            slots=registration.slots,
            available_slots=registration.slots,
            memory_limit_bytes=registration.memory_limit_bytes,
            lease_id=str(uuid4()),
            lease_expires_at=now + self._lease_ttl,
            registered_at=now,
            last_heartbeat_at=now,
            labels=registration.labels,
        )
        async with self._lock:
            self._workers[worker.worker_id] = worker
        return worker.model_copy(deep=True)

    async def heartbeat(self, worker_id: str, heartbeat: WorkerHeartbeat) -> Worker:
        now = self._clock()
        async with self._lock:
            worker = self._workers.get(worker_id)
            if worker is None:
                raise DistributedSQLError(
                    ErrorCode.NOT_FOUND,
                    f"Worker {worker_id!r} is not registered.",
                    status_code=404,
                    context={"worker_id": worker_id},
                )
            if worker.lease_id != heartbeat.lease_id:
                raise DistributedSQLError(
                    ErrorCode.LEASE_REJECTED,
                    f"Lease for Worker {worker_id!r} is not valid.",
                    status_code=409,
                    context={"worker_id": worker_id},
                )
            if worker.lease_expires_at is None or worker.lease_expires_at <= now:
                worker.state = WorkerState.LOST
                raise DistributedSQLError(
                    ErrorCode.LEASE_REJECTED,
                    f"Lease for Worker {worker_id!r} has expired.",
                    status_code=409,
                    context={"worker_id": worker_id},
                )
            if heartbeat.available_slots > worker.slots:
                raise DistributedSQLError(
                    ErrorCode.INVALID_REQUEST,
                    "Available slots cannot exceed configured Worker slots.",
                    status_code=422,
                    context={"worker_id": worker_id, "slots": worker.slots},
                )
            worker.available_slots = heartbeat.available_slots
            worker.state = heartbeat.state
            worker.last_heartbeat_at = now
            worker.lease_expires_at = now + self._lease_ttl
            return worker.model_copy(deep=True)

    async def expire_leases(self) -> list[str]:
        now = self._clock()
        expired: list[str] = []
        async with self._lock:
            for worker in self._workers.values():
                if (
                    worker.state is not WorkerState.LOST
                    and worker.lease_expires_at is not None
                    and worker.lease_expires_at <= now
                ):
                    worker.state = WorkerState.LOST
                    worker.available_slots = 0
                    expired.append(worker.worker_id)
        return expired

    async def list_workers(self) -> list[Worker]:
        async with self._lock:
            return [
                worker.model_copy(deep=True)
                for worker in sorted(self._workers.values(), key=lambda item: item.worker_id)
            ]

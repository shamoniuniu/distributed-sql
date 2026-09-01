"""Worker registration and lease-heartbeat background agent."""

import asyncio
from contextlib import suppress

import httpx

from distributed_sql.common.config import WorkerSettings
from distributed_sql.common.protocol import (
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerRegistrationResponse,
    WorkerState,
)


class WorkerAgent:
    """Maintains this Worker's renewable lease with the Coordinator."""

    def __init__(
        self,
        settings: WorkerSettings,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings
        self.worker_id = settings.worker_id or f"worker-{settings.port}"
        self._client = client or httpx.AsyncClient(
            base_url=settings.coordinator_url,
            timeout=5.0,
        )
        self._owns_client = client is None
        self._lease_id: str | None = None
        self._stop = asyncio.Event()
        self.registered = asyncio.Event()
        self.last_error: str | None = None

    async def run(self) -> None:
        try:
            while not self._stop.is_set():
                if self._lease_id is None:
                    await self._try_register()
                    delay = (
                        self.settings.heartbeat_interval_seconds
                        if self._lease_id
                        else self.settings.registration_retry_seconds
                    )
                else:
                    await self._try_heartbeat()
                    delay = self.settings.heartbeat_interval_seconds
                await self._wait_or_stop(delay)
        finally:
            self.registered.clear()
            if self._owns_client:
                await self._client.aclose()

    async def stop(self) -> None:
        self._stop.set()

    async def _try_register(self) -> None:
        registration = WorkerRegistration(
            worker_id=self.worker_id,
            endpoint=self.settings.endpoint,
            slots=self.settings.slots,
            memory_limit_bytes=self.settings.memory_limit_bytes,
        )
        try:
            response = await self._client.post(
                "/api/v1/workers/register",
                json=registration.model_dump(mode="json"),
            )
            response.raise_for_status()
            registered = WorkerRegistrationResponse.model_validate(response.json())
            if registered.worker.lease_id is None:
                raise ValueError("Coordinator registration response did not include a lease")
        except (httpx.HTTPError, ValueError) as exc:
            self.last_error = str(exc)
            self.registered.clear()
            return
        self._lease_id = registered.worker.lease_id
        self.last_error = None
        self.registered.set()

    async def _try_heartbeat(self) -> None:
        assert self._lease_id is not None
        heartbeat = WorkerHeartbeat(
            lease_id=self._lease_id,
            available_slots=self.settings.slots,
            state=WorkerState.ACTIVE,
        )
        try:
            response = await self._client.post(
                f"/api/v1/workers/{self.worker_id}/heartbeat",
                json=heartbeat.model_dump(mode="json"),
            )
            if response.status_code in {404, 409}:
                self._lease_id = None
                self.registered.clear()
                self.last_error = f"Coordinator rejected lease with HTTP {response.status_code}"
                return
            response.raise_for_status()
        except httpx.HTTPError as exc:
            self.last_error = str(exc)
            return
        self.last_error = None

    async def _wait_or_stop(self, delay: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=delay)

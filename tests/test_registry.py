from datetime import UTC, datetime, timedelta

import httpx
import pytest

from distributed_sql.common.config import CoordinatorSettings
from distributed_sql.common.protocol import (
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
)
from distributed_sql.coordinator.app import create_app
from distributed_sql.coordinator.registry import WorkerRegistry


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 31, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


@pytest.mark.asyncio
async def test_registry_registers_renews_and_expires_worker_lease() -> None:
    clock = Clock()
    registry = WorkerRegistry(lease_ttl_seconds=6, clock=clock)
    worker = await registry.register(
        WorkerRegistration(
            worker_id="worker-1",
            endpoint="http://127.0.0.1:8091",
            slots=2,
            memory_limit_bytes=64 * 1024 * 1024,
        )
    )

    assert worker.state is WorkerState.ACTIVE
    assert worker.lease_id is not None
    first_expiration = worker.lease_expires_at

    clock.now += timedelta(seconds=2)
    renewed = await registry.heartbeat(
        worker.worker_id,
        WorkerHeartbeat(
            lease_id=worker.lease_id,
            available_slots=1,
        ),
    )
    assert renewed.lease_expires_at is not None
    assert first_expiration is not None
    assert renewed.lease_expires_at > first_expiration
    assert renewed.available_slots == 1

    clock.now += timedelta(seconds=7)
    assert await registry.expire_leases() == ["worker-1"]
    assert (await registry.list_workers())[0].state is WorkerState.LOST
    assert await registry.expire_leases() == []


@pytest.mark.asyncio
async def test_coordinator_rejects_invalid_lease_with_stable_error_envelope() -> None:
    settings = CoordinatorSettings(lease_ttl_seconds=6)
    app = create_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://coordinator") as client:
        registration = await client.post(
            "/api/v1/workers/register",
            json={
                "worker_id": "worker-1",
                "endpoint": "http://127.0.0.1:8091",
                "slots": 1,
                "memory_limit_bytes": 1024,
            },
        )
        assert registration.status_code == 201

        response = await client.post(
            "/api/v1/workers/worker-1/heartbeat",
            json={
                "lease_id": "wrong-lease",
                "available_slots": 1,
                "state": "active",
                "metrics": {},
            },
        )

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "LEASE_REJECTED",
            "message": "Lease for Worker 'worker-1' is not valid.",
            "context": {"worker_id": "worker-1"},
        }
    }


@pytest.mark.asyncio
async def test_coordinator_wraps_request_validation_errors() -> None:
    app = create_app(CoordinatorSettings())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://coordinator") as client:
        response = await client.post(
            "/api/v1/workers/register",
            json={
                "worker_id": "worker-1",
                "endpoint": "http://127.0.0.1:8091",
                "slots": 0,
                "memory_limit_bytes": 1024,
            },
        )

    assert response.status_code == 422
    payload = response.json()
    assert payload["error"]["code"] == "INVALID_REQUEST"
    assert payload["error"]["message"] == "Request validation failed."
    assert payload["error"]["context"]["errors"][0]["loc"] == ["body", "slots"]


@pytest.mark.asyncio
async def test_coordinator_wraps_unknown_routes() -> None:
    app = create_app(CoordinatorSettings())
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://coordinator") as client:
        response = await client.get("/missing")

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "NOT_FOUND",
            "message": "Not Found",
            "context": {"status_code": 404},
        }
    }

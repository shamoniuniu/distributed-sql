import asyncio

import httpx
import pytest

from distributed_sql.common.config import CoordinatorSettings, WorkerSettings
from distributed_sql.coordinator.app import create_app as create_coordinator_app
from distributed_sql.worker.agent import WorkerAgent
from distributed_sql.worker.app import create_app as create_worker_app


@pytest.mark.asyncio
async def test_coordinator_and_worker_lifecycle_registers_and_renews_lease() -> None:
    coordinator = create_coordinator_app(
        CoordinatorSettings(
            lease_ttl_seconds=0.3,
            lease_check_interval_seconds=0.02,
        )
    )
    coordinator_transport = httpx.ASGITransport(app=coordinator)

    async with coordinator.router.lifespan_context(coordinator):
        async with httpx.AsyncClient(
            transport=coordinator_transport,
            base_url="http://coordinator",
        ) as coordinator_client:
            coordinator_health = await coordinator_client.get("/health")
            assert coordinator_health.status_code == 200
            assert coordinator_health.json()["status"] == "healthy"

            worker_settings = WorkerSettings(
                worker_id="worker-lifecycle",
                port=8099,
                coordinator_url="http://coordinator",
                heartbeat_interval_seconds=0.02,
                registration_retry_seconds=0.01,
            )
            agent = WorkerAgent(worker_settings, client=coordinator_client)
            worker = create_worker_app(worker_settings, agent)

            async with worker.router.lifespan_context(worker):
                await asyncio.wait_for(agent.registered.wait(), timeout=1)
                workers_response = await coordinator_client.get("/api/v1/workers")
                first_worker = workers_response.json()["workers"][0]
                first_expiration = first_worker["lease_expires_at"]

                await asyncio.sleep(0.06)
                workers_response = await coordinator_client.get("/api/v1/workers")
                renewed_worker = workers_response.json()["workers"][0]

                assert renewed_worker["worker_id"] == "worker-lifecycle"
                assert renewed_worker["state"] == "active"
                assert renewed_worker["lease_expires_at"] > first_expiration

                worker_transport = httpx.ASGITransport(app=worker)
                async with httpx.AsyncClient(
                    transport=worker_transport,
                    base_url="http://worker",
                ) as worker_client:
                    worker_health = await worker_client.get("/health")
                assert worker_health.status_code == 200
                assert worker_health.json()["dependencies"]["coordinator"] == "registered"

                agent.last_error = "heartbeat failed"
                async with httpx.AsyncClient(
                    transport=worker_transport,
                    base_url="http://worker",
                ) as worker_client:
                    degraded_health = await worker_client.get("/health")
                assert degraded_health.json()["status"] == "degraded"
                assert degraded_health.json()["dependencies"]["coordinator"] == "unavailable"

            assert worker.state.heartbeat_task.done()
            assert not agent.registered.is_set()

        assert not coordinator.state.lease_monitor_task.done()

    assert coordinator.state.lease_monitor_task.done()

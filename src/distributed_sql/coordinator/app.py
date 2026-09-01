"""FastAPI application for Coordinator control-plane endpoints."""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from importlib.metadata import version

from fastapi import FastAPI, status

from distributed_sql import __version__
from distributed_sql.catalog.api import create_catalog_router
from distributed_sql.catalog.importer import DataImporter
from distributed_sql.catalog.repository import SQLiteCatalog
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter, S3ObjectStore
from distributed_sql.common.config import CoordinatorSettings, get_coordinator_settings
from distributed_sql.common.exceptions import install_exception_handlers
from distributed_sql.common.protocol import (
    HealthResponse,
    HealthStatus,
    WorkerHeartbeat,
    WorkerHeartbeatResponse,
    WorkerListResponse,
    WorkerRegistration,
    WorkerRegistrationResponse,
)
from distributed_sql.coordinator.queries import QueryService
from distributed_sql.coordinator.query_api import create_query_router
from distributed_sql.coordinator.registry import WorkerRegistry
from distributed_sql.web.app import create_web_router


async def _lease_monitor(registry: WorkerRegistry, check_interval_seconds: float) -> None:
    while True:
        await asyncio.sleep(check_interval_seconds)
        await registry.expire_leases()


def create_app(
    settings: CoordinatorSettings | None = None,
    registry: WorkerRegistry | None = None,
    catalog: SQLiteCatalog | None = None,
    object_stores: ObjectStoreRouter | None = None,
) -> FastAPI:
    service_settings = settings or get_coordinator_settings()
    worker_registry = registry or WorkerRegistry(service_settings.lease_ttl_seconds)
    catalog_repository = catalog or SQLiteCatalog(service_settings.catalog_path)
    if object_stores is None:
        s3_store = None
        if (
            service_settings.object_store_access_key is not None
            and service_settings.object_store_secret_key is not None
        ):
            s3_store = S3ObjectStore(
                access_key=service_settings.object_store_access_key,
                secret_key=service_settings.object_store_secret_key,
                endpoint=service_settings.object_store_endpoint,
                region=service_settings.object_store_region,
                secure=service_settings.object_store_secure,
            )
            if (
                service_settings.object_store_create_bucket
                and service_settings.object_store_bucket is not None
            ):
                s3_store.create_bucket(service_settings.object_store_bucket)
        object_stores = ObjectStoreRouter(LocalObjectStore(), s3_store)
    data_importer = DataImporter(catalog_repository, object_stores)
    query_service = QueryService(
        catalog_repository,
        object_stores,
        worker_registry,
        runtime_root=service_settings.runtime_root,
        remote_task_auth_token=service_settings.remote_task_auth_token,
        cancellation_timeout_seconds=service_settings.cancellation_timeout_seconds,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        catalog_repository.initialize()
        app.state.health_status = HealthStatus.HEALTHY
        monitor = asyncio.create_task(
            _lease_monitor(worker_registry, service_settings.lease_check_interval_seconds),
            name="worker-lease-monitor",
        )
        app.state.lease_monitor_task = monitor
        try:
            yield
        finally:
            app.state.health_status = HealthStatus.STOPPING
            await query_service.close()
            monitor.cancel()
            with suppress(asyncio.CancelledError):
                await monitor

    app = FastAPI(
        title="Distributed SQL Coordinator",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.health_status = HealthStatus.STARTING
    app.state.registry = worker_registry
    app.state.catalog = catalog_repository
    app.state.object_stores = object_stores
    app.state.query_service = query_service
    install_exception_handlers(app)
    app.include_router(create_catalog_router(catalog_repository, data_importer))
    app.include_router(create_query_router(query_service, worker_registry))
    app.include_router(create_web_router())

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(
            service="coordinator",
            status=app.state.health_status,
            version=__version__,
            process_id=os.getpid(),
            dependencies={
                "fastapi": version("fastapi"),
                "registry": "ready",
                "catalog": "ready",
            },
        )

    @app.post(
        "/api/v1/workers/register",
        response_model=WorkerRegistrationResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def register_worker(
        registration: WorkerRegistration,
    ) -> WorkerRegistrationResponse:
        worker = await worker_registry.register(registration)
        return WorkerRegistrationResponse(
            worker=worker,
            lease_ttl_seconds=worker_registry.lease_ttl_seconds,
        )

    @app.post(
        "/api/v1/workers/{worker_id}/heartbeat",
        response_model=WorkerHeartbeatResponse,
    )
    async def heartbeat_worker(
        worker_id: str,
        heartbeat: WorkerHeartbeat,
    ) -> WorkerHeartbeatResponse:
        worker = await worker_registry.heartbeat(worker_id, heartbeat)
        assert worker.lease_expires_at is not None
        return WorkerHeartbeatResponse(
            worker_id=worker.worker_id,
            accepted=True,
            lease_expires_at=worker.lease_expires_at,
        )

    @app.get("/api/v1/workers", response_model=WorkerListResponse)
    async def list_workers() -> WorkerListResponse:
        return WorkerListResponse(workers=await worker_registry.list_workers())

    return app


app = create_app()

"""FastAPI application for a Worker process."""

import asyncio
import os
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from importlib.metadata import version

from fastapi import FastAPI, Header, HTTPException, Response, status

from distributed_sql import __version__
from distributed_sql.common.config import WorkerSettings, get_worker_settings
from distributed_sql.common.exceptions import install_exception_handlers
from distributed_sql.common.protocol import (
    AttemptState,
    HealthResponse,
    HealthStatus,
    RemoteTaskListResponse,
    RemoteTaskOperation,
    RemoteTaskStatus,
    RemoteTaskSubmission,
)
from distributed_sql.worker.agent import WorkerAgent
from distributed_sql.worker.tasks import WorkerTaskManager


def create_app(
    settings: WorkerSettings | None = None,
    agent: WorkerAgent | None = None,
) -> FastAPI:
    service_settings = settings or get_worker_settings()
    worker_agent = agent or WorkerAgent(service_settings)
    task_manager = WorkerTaskManager(service_settings, worker_agent.worker_id)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.health_status = HealthStatus.HEALTHY
        heartbeat_task = asyncio.create_task(
            worker_agent.run(),
            name=f"{worker_agent.worker_id}-heartbeat",
        )
        app.state.heartbeat_task = heartbeat_task
        try:
            yield
        finally:
            app.state.health_status = HealthStatus.STOPPING
            await task_manager.close()
            await worker_agent.stop()
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task

    app = FastAPI(
        title="Distributed SQL Worker",
        version=__version__,
        lifespan=lifespan,
    )
    app.state.health_status = HealthStatus.STARTING
    app.state.agent = worker_agent
    app.state.task_manager = task_manager
    install_exception_handlers(app)

    def authorize(authorization: str | None, *, plan_task: bool = False) -> None:
        expected = service_settings.remote_task_auth_token
        supplied = (
            authorization.removeprefix("Bearer ")
            if authorization and authorization.startswith("Bearer ")
            else ""
        )
        if expected is None:
            if plan_task:
                raise HTTPException(
                    status_code=503,
                    detail="Plan Task execution requires a configured authentication token",
                )
            return
        if not secrets.compare_digest(supplied, expected):
            raise HTTPException(status_code=401, detail="Invalid Task authentication")

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        if worker_agent.last_error:
            coordinator_status = "unavailable"
            service_status = HealthStatus.DEGRADED
        elif worker_agent.registered.is_set():
            coordinator_status = "registered"
            service_status = app.state.health_status
        else:
            coordinator_status = "registering"
            service_status = app.state.health_status
        return HealthResponse(
            service="worker",
            status=service_status,
            version=__version__,
            process_id=os.getpid(),
            dependencies={
                "pyarrow": version("pyarrow"),
                "coordinator": coordinator_status,
            },
        )

    @app.post(
        "/api/v1/tasks",
        response_model=RemoteTaskStatus,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def submit_task(
        submission: RemoteTaskSubmission,
        authorization: str | None = Header(default=None),
    ) -> RemoteTaskStatus:
        authorize(
            authorization,
            plan_task=submission.operation
            in {
                RemoteTaskOperation.SCAN,
                RemoteTaskOperation.UNARY,
                RemoteTaskOperation.JOIN,
            },
        )
        try:
            return await task_manager.submit(submission)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/api/v1/tasks/{attempt_id}", response_model=RemoteTaskStatus)
    async def get_task(
        attempt_id: str,
        authorization: str | None = Header(default=None),
    ) -> RemoteTaskStatus:
        authorize(authorization)
        task_status = await task_manager.status(attempt_id)
        if task_status is None:
            raise HTTPException(status_code=404, detail="Task attempt not found")
        return task_status

    @app.get("/api/v1/tasks", response_model=RemoteTaskListResponse)
    async def list_tasks(
        authorization: str | None = Header(default=None),
    ) -> RemoteTaskListResponse:
        authorize(authorization)
        return RemoteTaskListResponse(tasks=await task_manager.list_statuses())

    @app.delete("/api/v1/tasks/{attempt_id}", response_model=RemoteTaskStatus)
    async def cancel_task(
        attempt_id: str,
        authorization: str | None = Header(default=None),
    ) -> RemoteTaskStatus:
        authorize(authorization)
        task_status = await task_manager.cancel(attempt_id)
        if task_status is None:
            raise HTTPException(status_code=404, detail="Task attempt not found")
        return task_status

    @app.get("/api/v1/tasks/{attempt_id}/result")
    async def get_task_result(
        attempt_id: str,
        authorization: str | None = Header(default=None),
    ) -> Response:
        authorize(authorization)
        task_status = await task_manager.status(attempt_id)
        if task_status is None:
            raise HTTPException(status_code=404, detail="Task attempt not found")
        if task_status.state is not AttemptState.SUCCEEDED:
            raise HTTPException(status_code=409, detail="Task result is not ready")
        return Response(
            content=task_manager.result_bytes(task_status),
            media_type="application/vnd.apache.parquet",
        )

    return app


app = create_app()

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI

from distributed_sql.common.config import CoordinatorSettings, WorkerSettings
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import (
    AttemptState,
    RemoteTaskOperation,
    RemoteTaskSubmission,
)
from distributed_sql.coordinator.app import create_app as create_coordinator_app
from distributed_sql.worker.tasks import WorkerTaskManager

TASK_AUTH_TOKEN = "task19-contract-secret"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_worker(port: int, temp_directory: Path) -> subprocess.Popen[bytes]:
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": os.pathsep.join(
                [
                    str(Path.cwd() / "src"),
                    str(Path.cwd() / ".venv" / "Lib" / "site-packages"),
                ]
            ),
            "DISTRIBUTED_SQL_WORKER_ID": "worker-errors",
            "DISTRIBUTED_SQL_WORKER_PORT": str(port),
            "DISTRIBUTED_SQL_WORKER_COORDINATOR_URL": "http://127.0.0.1:1",
            "DISTRIBUTED_SQL_WORKER_REGISTRATION_RETRY_SECONDS": "0.05",
            "DISTRIBUTED_SQL_WORKER_MEMORY_LIMIT_BYTES": "1",
            "DISTRIBUTED_SQL_WORKER_TEMP_DIRECTORY": str(temp_directory),
            "DISTRIBUTED_SQL_WORKER_REMOTE_TASK_AUTH_TOKEN": TASK_AUTH_TOKEN,
        }
    )
    return subprocess.Popen(
        [
            getattr(sys, "_base_executable", sys.executable),
            "-m",
            "distributed_sql.worker.main",
        ],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for_worker(port: int, process: subprocess.Popen[bytes]) -> None:
    for _ in range(200):
        assert process.poll() is None
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=0.2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.025)
    raise TimeoutError("Worker did not start")


async def _wait_for_terminal(
    client: httpx.AsyncClient,
    query_id: str,
) -> dict[str, Any]:
    for _ in range(1000):
        response = await client.get(f"/api/v1/queries/{query_id}")
        payload: dict[str, Any] = response.json()
        if payload["state"] in {"succeeded", "failed", "canceled"}:
            return payload
        await asyncio.sleep(0.01)
    raise AssertionError("Query did not reach a terminal state")


async def _submit_and_wait(client: httpx.AsyncClient, sql: str) -> dict[str, Any]:
    response = await client.post("/api/v1/queries", json={"sql": sql})
    assert response.status_code == 202
    return await _wait_for_terminal(client, response.json()["query_id"])


def _assert_public_error(
    query: dict[str, Any],
    expected_code: ErrorCode,
    forbidden_values: tuple[str, ...],
) -> None:
    assert query["state"] == "failed"
    assert query["error"]["code"] == expected_code.value
    serialized = json.dumps(query).casefold()
    for forbidden in ("traceback", "stack", *forbidden_values):
        assert forbidden.casefold() not in serialized


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        pytest.param(
            DistributedSQLError(
                ErrorCode.RESOURCE_EXHAUSTED,
                "Worker resources were exhausted.",
                status_code=507,
                context={
                    "query_id": "query-domain",
                    "temp_root": "C:/private/spill",
                    "secret": "do-not-expose",
                },
            ),
            ErrorCode.RESOURCE_EXHAUSTED,
            id="domain-error",
        ),
        pytest.param(
            RuntimeError("failed at C:/private/worker.py with secret=do-not-expose"),
            ErrorCode.TASK_FAILED,
            id="unclassified-error",
        ),
    ],
)
async def test_worker_task_failure_uses_structured_sanitized_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    expected_code: ErrorCode,
) -> None:
    manager = WorkerTaskManager(
        WorkerSettings(worker_id="worker-errors", temp_directory=tmp_path),
        "worker-errors",
    )

    def fail(*_args: object) -> None:
        raise error

    monkeypatch.setattr(manager, "_execute_sync", fail)
    submission = RemoteTaskSubmission(
        task_id="task-error",
        attempt_id="attempt-error",
        query_id="query-domain",
        stage_id="stage-error",
        operation=RemoteTaskOperation.SLEEP,
        payload={"seconds": 0},
        output_location=str(tmp_path / "result.parquet"),
    )
    await manager.submit(submission)
    for _ in range(100):
        status = await manager.status(submission.attempt_id)
        assert status is not None
        if status.state is AttemptState.FAILED:
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("Task did not fail")

    assert status.error is not None
    assert status.error.code is expected_code
    assert status.error.context["query_id"] == "query-domain"
    serialized = status.error.model_dump_json().casefold()
    assert "c:/private" not in serialized
    assert "do-not-expose" not in serialized
    assert "traceback" not in serialized
    assert "stack" not in serialized
    await manager.close()


@pytest.mark.integration
@pytest.mark.fault
@pytest.mark.timeout(40)
@pytest.mark.asyncio
async def test_real_worker_query_api_distinguishes_five_error_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_file = tmp_path / "orders.parquet"
    pq.write_table(pa.table({"id": [3, 1, 2]}), data_file)
    blocked_temp_root = tmp_path / "private-worker-spill"
    blocked_temp_root.write_text("not a directory", encoding="utf-8")
    worker_port = _free_port()
    worker = await asyncio.to_thread(_start_worker, worker_port, blocked_temp_root)
    await asyncio.to_thread(_wait_for_worker, worker_port, worker)

    app: FastAPI = create_coordinator_app(
        CoordinatorSettings(
            catalog_path=tmp_path / "catalog.db",
            lease_ttl_seconds=30,
            remote_task_auth_token=TASK_AUTH_TOKEN,
        )
    )
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://coordinator",
    )
    try:
        async with app.router.lifespan_context(app), client:
            registration = await client.post(
                "/api/v1/workers/register",
                json={
                    "worker_id": "worker-errors",
                    "endpoint": f"http://127.0.0.1:{worker_port}",
                    "slots": 1,
                    "memory_limit_bytes": 1,
                },
            )
            assert registration.status_code == 201
            assert (
                await client.post(
                    "/api/v1/catalog/namespaces",
                    json={"name": "default"},
                )
            ).status_code == 201
            assert (
                await client.post(
                    "/api/v1/catalog/namespaces/default/tables",
                    json={
                        "name": "orders",
                        "schema": {
                            "fields": [
                                {
                                    "name": "id",
                                    "data_type": "int64",
                                    "nullable": False,
                                }
                            ]
                        },
                        "format": "parquet",
                        "location": str(data_file),
                    },
                )
            ).status_code == 201

            syntax = await _submit_and_wait(client, "SELECT id FROM orders WHERE )")
            _assert_public_error(syntax, ErrorCode.SYNTAX_ERROR, (TASK_AUTH_TOKEN,))

            binding = await _submit_and_wait(client, "SELECT missing FROM orders")
            _assert_public_error(binding, ErrorCode.BINDING_ERROR, (TASK_AUTH_TOKEN,))

            resource = await _submit_and_wait(client, "SELECT id FROM orders ORDER BY id")
            _assert_public_error(
                resource,
                ErrorCode.RESOURCE_EXHAUSTED,
                (TASK_AUTH_TOKEN, str(blocked_temp_root)),
            )

            service = app.state.query_service

            def fail_in_coordinator() -> None:
                raise RuntimeError(
                    f"internal failure at {tmp_path / 'private.py'} secret={TASK_AUTH_TOKEN}"
                )

            with monkeypatch.context() as scoped:
                scoped.setattr(service, "_catalog_snapshot", fail_in_coordinator)
                internal = await _submit_and_wait(client, "SELECT id FROM orders")
            _assert_public_error(
                internal,
                ErrorCode.INTERNAL_ERROR,
                (TASK_AUTH_TOKEN, str(tmp_path)),
            )

            worker.terminate()
            await asyncio.to_thread(worker.wait, 5)
            task_failed = await _submit_and_wait(client, "SELECT id FROM orders")
            _assert_public_error(
                task_failed,
                ErrorCode.TASK_FAILED,
                (TASK_AUTH_TOKEN, str(tmp_path)),
            )
    finally:
        if worker.poll() is None:
            worker.terminate()
            await asyncio.to_thread(worker.wait, 5)

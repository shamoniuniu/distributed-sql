from __future__ import annotations

import asyncio
import io
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import FastAPI

from distributed_sql.cli import main as cli_main
from distributed_sql.common.config import CoordinatorSettings
from distributed_sql.coordinator.app import create_app

TASK_AUTH_TOKEN = "interface-test-task-token"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_worker(port: int, tmp_path: Path) -> subprocess.Popen[bytes]:
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": os.pathsep.join(
                [
                    str(Path.cwd() / "src"),
                    str(Path.cwd() / ".venv" / "Lib" / "site-packages"),
                ]
            ),
            "DISTRIBUTED_SQL_WORKER_ID": "worker-api",
            "DISTRIBUTED_SQL_WORKER_PORT": str(port),
            "DISTRIBUTED_SQL_WORKER_COORDINATOR_URL": "http://127.0.0.1:1",
            "DISTRIBUTED_SQL_WORKER_REGISTRATION_RETRY_SECONDS": "0.05",
            "DISTRIBUTED_SQL_WORKER_TEMP_DIRECTORY": str(tmp_path / "worker"),
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
    raise AssertionError("query did not reach a terminal state")


async def _prepare_query_app(
    tmp_path: Path,
) -> tuple[FastAPI, httpx.AsyncClient]:
    data_file = tmp_path / "orders.parquet"
    pq.write_table(
        pa.table({"id": [1, 2, 3], "amount": [10, 20, 30]}),
        data_file,
    )
    app = create_app(
        CoordinatorSettings(
            catalog_path=tmp_path / "catalog.db",
            lease_ttl_seconds=30,
            remote_task_auth_token=TASK_AUTH_TOKEN,
        )
    )
    transport = httpx.ASGITransport(app=app)
    client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")
    return app, client


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_query_api_contract_executes_real_engine_and_pages_results(
    tmp_path: Path,
) -> None:
    worker_port = _free_port()
    worker = await asyncio.to_thread(_start_worker, worker_port, tmp_path)
    await asyncio.to_thread(_wait_for_worker, worker_port, worker)
    app, client = await _prepare_query_app(tmp_path)
    try:
        async with app.router.lifespan_context(app), client:
            assert (
                await client.post(
                    "/api/v1/workers/register",
                    json={
                        "worker_id": "worker-api",
                        "endpoint": f"http://127.0.0.1:{worker_port}",
                        "slots": 2,
                        "memory_limit_bytes": 64 * 1024 * 1024,
                    },
                )
            ).status_code == 201
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
                                },
                                {"name": "amount", "data_type": "int64"},
                            ]
                        },
                        "format": "parquet",
                        "location": str(tmp_path / "orders.parquet"),
                    },
                )
            ).status_code == 201

            submitted = await client.post(
                "/api/v1/queries",
                json={"sql": "SELECT id, amount FROM orders ORDER BY id"},
            )
            assert submitted.status_code == 202
            query_id = submitted.json()["query_id"]
            completed = await _wait_for_terminal(client, query_id)
            assert completed["state"] == "succeeded", completed.get("error")

            first = await client.get(
                f"/api/v1/queries/{query_id}/results",
                params={"offset": 0, "limit": 2},
            )
            assert first.status_code == 200
            assert first.json() == {
                "query_id": query_id,
                "columns": ["id", "amount"],
                "rows": [{"id": 1, "amount": 10}, {"id": 2, "amount": 20}],
                "offset": 0,
                "limit": 2,
                "returned": 2,
                "total_rows": 3,
                "next_offset": 2,
            }
            second = await client.get(
                f"/api/v1/queries/{query_id}/results",
                params={"offset": 2, "limit": 2},
            )
            assert second.json()["rows"] == [{"id": 3, "amount": 30}]
            assert second.json()["next_offset"] is None

            plan = (await client.get(f"/api/v1/queries/{query_id}/plan")).json()
            assert plan["original_logical_plan"]["node_type"] == "order"
            assert plan["optimized_logical_plan"]["node_type"] == "order"
            assert plan["physical_plan"]["node_type"] == "order"
            assert "Cost-Based Optimized Plan" in plan["explain"]

            metrics = (await client.get(f"/api/v1/queries/{query_id}/metrics")).json()
            assert metrics["diagnostics"]["runtime"]["result_rows"] == 3
            assert metrics["diagnostics"]["stages"]
            advisor = (await client.get(f"/api/v1/queries/{query_id}/advisor")).json()
            assert advisor["query_id"] == query_id
            assert advisor["status"] in {"recommendations", "no_recommendations"}
            assert (await client.get("/api/v1/nodes")).json()["workers"][0][
                "worker_id"
            ] == "worker-api"

            explained = await client.post(
                "/api/v1/queries/explain",
                json={"sql": "SELECT id FROM orders LIMIT 1"},
            )
            assert explained.status_code == 200
            assert '"node_type": "limit"' in json.dumps(explained.json()["physical_plan"])
    finally:
        worker.terminate()
        await asyncio.to_thread(worker.wait, 5)


@pytest.mark.asyncio
async def test_query_api_failure_cancel_and_not_ready_contract(tmp_path: Path) -> None:
    app, client = await _prepare_query_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with client:
            missing = await client.get("/api/v1/queries/does-not-exist")
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "NOT_FOUND"

            await client.post(
                "/api/v1/workers/register",
                json={
                    "worker_id": "worker-cancel",
                    "endpoint": "http://worker-cancel",
                    "slots": 1,
                    "memory_limit_bytes": 64 * 1024 * 1024,
                },
            )
            invalid = await client.post(
                "/api/v1/queries",
                json={"sql": "SELECT * FROM missing_table"},
            )
            invalid_id = invalid.json()["query_id"]
            failed = await _wait_for_terminal(client, invalid_id)
            assert failed["state"] == "failed"
            assert failed["error"]["code"] == "BINDING_ERROR"
            assert "traceback" not in json.dumps(failed).casefold()
            unavailable = await client.get(f"/api/v1/queries/{invalid_id}/results")
            assert unavailable.status_code == 409

            submitted = await client.post(
                "/api/v1/queries",
                json={"sql": "SELECT * FROM missing_table"},
            )
            query_id = submitted.json()["query_id"]
            canceled = await client.delete(f"/api/v1/queries/{query_id}")
            assert canceled.status_code == 200
            assert canceled.json()["state"] == "canceled"
            assert (await _wait_for_terminal(client, query_id))["state"] == "canceled"


@pytest.mark.parametrize(
    "arguments",
    [
        ["query", "SELECT * FROM t", "--no-wait"],
        ["explain", "SELECT * FROM t"],
        ["status", "query-1"],
        ["cancel", "query-1"],
        ["catalog", "namespaces"],
        ["catalog", "tables", "default"],
        ["catalog", "create-namespace", "sales"],
        [
            "catalog",
            "create-table",
            "default",
            "orders",
            "--format",
            "parquet",
            "--location",
            "orders.parquet",
            "--schema",
            '{"fields":[{"name":"id","data_type":"int64"}]}',
        ],
        ["catalog", "delete-table", "default", "orders"],
        ["import", "default", "orders", "orders.csv", "--partitions", "2"],
    ],
)
def test_cli_command_smoke(
    monkeypatch: pytest.MonkeyPatch,
    arguments: Sequence[str],
) -> None:
    calls: list[tuple[str, str, dict[str, Any] | None]] = []

    def fake_request(
        _base_url: str,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        calls.append((method, path, payload))
        return {"query_id": "query-1", "state": "queued"}

    monkeypatch.setattr(cli_main, "_request", fake_request)
    output = io.StringIO()

    assert cli_main.main(arguments, output=output) == 0
    assert calls
    assert json.loads(output.getvalue())["query_id"] == "query-1"


@pytest.mark.asyncio
async def test_webui_serves_compact_operational_workflow(tmp_path: Path) -> None:
    app, client = await _prepare_query_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with client:
            page = await client.get("/")
            css = await client.get("/web/app.css")
            javascript = await client.get("/web/app.js")

    assert page.status_code == css.status_code == javascript.status_code == 200
    for element_id in {
        "sql",
        "run",
        "cancel",
        "result-table",
        "plan-output",
        "metrics-output",
        "nodes",
        "catalog-tree",
        "catalog-form",
    }:
        assert f'id="{element_id}"' in page.text
    assert "@media (max-width: 760px)" in css.text
    for endpoint in {
        "/api/v1/queries",
        "/results?",
        "/plan",
        "/metrics",
        "/advisor",
        "/api/v1/nodes",
        "/api/v1/catalog/namespaces",
    }:
        assert endpoint in javascript.text

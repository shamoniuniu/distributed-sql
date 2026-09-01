from __future__ import annotations

import asyncio
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

TASK_AUTH_TOKEN = "task20-metrics-token"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_process(module: str, environment: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", module],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for_workers(
    coordinator_url: str,
    expected: int,
    processes: list[subprocess.Popen[bytes]],
) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        assert all(process.poll() is None for process in processes)
        try:
            response = httpx.get(
                f"{coordinator_url}/api/v1/workers",
                timeout=0.5,
                trust_env=False,
            )
            workers = response.json()["workers"]
            if sum(worker["state"] == "active" for worker in workers) == expected:
                return
        except (httpx.HTTPError, KeyError, ValueError):
            pass
        time.sleep(0.05)
    raise TimeoutError("Coordinator and remote Workers did not become ready")


def _stop(processes: list[subprocess.Popen[bytes]]) -> None:
    if sys.platform == "win32":
        for process in reversed(processes):
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                capture_output=True,
                check=False,
            )
        return
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
    for process in reversed(processes):
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


async def _wait_for_query(
    client: httpx.AsyncClient,
    query_id: str,
) -> dict[str, Any]:
    for _ in range(1_000):
        response = await client.get(f"/api/v1/queries/{query_id}")
        payload: dict[str, Any] = response.json()
        if payload["state"] in {"succeeded", "failed", "canceled"}:
            return payload
        await asyncio.sleep(0.01)
    raise TimeoutError("Query did not reach a terminal state")


@pytest.mark.integration
@pytest.mark.timeout(45)
@pytest.mark.asyncio
async def test_real_coordinator_and_two_workers_expose_auditable_task_metrics(
    tmp_path: Path,
) -> None:
    coordinator_port, worker_1_port, worker_2_port = (
        _free_port(),
        _free_port(),
        _free_port(),
    )
    coordinator_url = f"http://127.0.0.1:{coordinator_port}"
    source = tmp_path / "orders-source.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array([4, 1, 3, 2], type=pa.int64()),
                "amount": pa.array([40, 10, 30, 20], type=pa.int64()),
            }
        ),
        source,
    )
    base_environment = os.environ.copy()
    base_environment.update(
        {
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": os.pathsep.join(
                [
                    str(Path.cwd() / "src"),
                        os.environ.get("PYTHONPATH", ""),
                ]
            ),
        }
    )
    coordinator_environment = base_environment | {
        "DISTRIBUTED_SQL_COORDINATOR_PORT": str(coordinator_port),
        "DISTRIBUTED_SQL_COORDINATOR_CATALOG_PATH": str(tmp_path / "catalog.db"),
        "DISTRIBUTED_SQL_COORDINATOR_REMOTE_TASK_AUTH_TOKEN": TASK_AUTH_TOKEN,
        "DISTRIBUTED_SQL_COORDINATOR_LEASE_TTL_SECONDS": "2",
        "DISTRIBUTED_SQL_COORDINATOR_LEASE_CHECK_INTERVAL_SECONDS": "0.05",
    }
    processes = [
        await asyncio.to_thread(
            _start_process,
            "distributed_sql.coordinator.main",
            coordinator_environment,
        )
    ]
    try:
        for index, port in enumerate((worker_1_port, worker_2_port), start=1):
            worker_environment = base_environment | {
                "DISTRIBUTED_SQL_WORKER_ID": f"worker-{index}",
                "DISTRIBUTED_SQL_WORKER_PORT": str(port),
                "DISTRIBUTED_SQL_WORKER_COORDINATOR_URL": coordinator_url,
                "DISTRIBUTED_SQL_WORKER_HEARTBEAT_INTERVAL_SECONDS": "0.05",
                "DISTRIBUTED_SQL_WORKER_REGISTRATION_RETRY_SECONDS": "0.05",
                "DISTRIBUTED_SQL_WORKER_TEMP_DIRECTORY": str(
                    tmp_path / f"worker-{index}"
                ),
                "DISTRIBUTED_SQL_WORKER_REMOTE_TASK_AUTH_TOKEN": TASK_AUTH_TOKEN,
            }
            processes.append(
                await asyncio.to_thread(
                    _start_process,
                    "distributed_sql.worker.main",
                    worker_environment,
                )
            )
        await asyncio.to_thread(_wait_for_workers, coordinator_url, 2, processes)

        async with httpx.AsyncClient(
            base_url=coordinator_url,
            timeout=10,
            trust_env=False,
        ) as client:
            namespace = await client.post(
                "/api/v1/catalog/namespaces",
                json={"name": "default"},
            )
            assert namespace.status_code == 201
            table = await client.post(
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
                            {
                                "name": "amount",
                                "data_type": "int64",
                                "nullable": False,
                            },
                        ]
                    },
                    "format": "parquet",
                    "location": str(tmp_path / "orders"),
                },
            )
            assert table.status_code == 201
            imported = await client.post(
                "/api/v1/catalog/namespaces/default/tables/orders/imports",
                json={
                    "source_location": str(source),
                    "source_format": "parquet",
                    "partition_count": 2,
                },
            )
            assert imported.status_code == 201

            submitted = await client.post(
                "/api/v1/queries",
                json={"sql": "SELECT id, amount FROM orders ORDER BY id"},
            )
            assert submitted.status_code == 202
            query_id = submitted.json()["query_id"]
            completed = await _wait_for_query(client, query_id)
            assert completed["state"] == "succeeded", completed.get("error")

            response = await client.get(f"/api/v1/queries/{query_id}/metrics")
            assert response.status_code == 200
            payload = response.json()

        diagnostics = payload["diagnostics"]
        tasks = diagnostics["tasks"]
        stages = diagnostics["stages"]
        assert tasks
        assert stages
        assert {
            attempt["worker_id"] for attempt in diagnostics["attempts"]
        } == {"worker-1", "worker-2"}

        numeric_task_fields = {
            "input_rows",
            "input_bytes",
            "output_rows",
            "output_bytes",
            "duration_seconds",
            "shuffle_records_written",
            "shuffle_bytes_written",
            "shuffle_write_seconds",
            "shuffle_records_read",
            "shuffle_bytes_read",
            "shuffle_read_seconds",
            "spill_bytes",
            "spill_files",
            "spill_count",
            "peak_memory_bytes",
            "external_sort_runs",
            "hash_partitions",
            "sort_merge_fallbacks",
            "sort_aggregate_runs",
        }
        for task in tasks:
            assert all(task[field] is not None for field in numeric_task_fields)
            assert all(task[field] >= 0 for field in numeric_task_fields)
            assert task["input_bytes"] > 0
            assert task["output_bytes"] > 0
            assert task["duration_seconds"] > 0

        tasks_by_stage = {
            stage["stage_id"]: [
                task for task in tasks if task["stage_id"] == stage["stage_id"]
            ]
            for stage in stages
        }
        additive_fields = {
            "input_rows",
            "input_bytes",
            "output_rows",
            "output_bytes",
            "shuffle_records_written",
            "shuffle_bytes_written",
            "shuffle_write_seconds",
            "shuffle_records_read",
            "shuffle_bytes_read",
            "shuffle_read_seconds",
            "spill_bytes",
            "spill_files",
            "spill_count",
            "external_sort_runs",
            "hash_partitions",
            "sort_merge_fallbacks",
            "sort_aggregate_runs",
        }
        for stage in stages:
            stage_tasks = tasks_by_stage[stage["stage_id"]]
            assert stage_tasks
            for field in additive_fields:
                assert stage[field] == pytest.approx(
                    sum(task[field] for task in stage_tasks)
                )
            durations = [task["duration_seconds"] for task in stage_tasks]
            assert stage["max_task_duration_seconds"] == max(durations)
            assert stage["task_duration_sum_seconds"] == pytest.approx(sum(durations))
            assert stage["wall_duration_seconds"] >= max(durations)
            assert stage["peak_memory_bytes"] == max(
                task["peak_memory_bytes"] for task in stage_tasks
            )

        explain = payload["explain_analyze"]
        assert "== Stage Metrics ==" in explain
        assert "== Task Metrics ==" in explain
        assert "input_rows=" in explain
        assert "task_sum_seconds=" in explain
        assert "shuffle_read_bytes=" in explain
        assert "spill_count=" in explain
        assert "unknown" not in explain
    finally:
        await asyncio.to_thread(_stop, processes)

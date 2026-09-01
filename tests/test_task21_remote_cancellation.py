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

TASK_AUTH_TOKEN = "task21-cancellation-token"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _start_process(module: str, environment: dict[str, str]) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [getattr(sys, "_base_executable", sys.executable), "-m", module],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


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


async def _worker_tasks(port: int) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}",
        timeout=1,
        trust_env=False,
        headers={"Authorization": f"Bearer {TASK_AUTH_TOKEN}"},
    ) as client:
        response = await client.get("/api/v1/tasks")
    response.raise_for_status()
    return response.json()["tasks"]  # type: ignore[no-any-return]


async def _wait_for_first_stage_running(worker_ports: tuple[int, int]) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        statuses = await asyncio.gather(*(_worker_tasks(port) for port in worker_ports))
        if all(
            len(worker_statuses) == 1
            and worker_statuses[0]["state"] == "running"
            for worker_statuses in statuses
        ):
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("First Stage did not start on both Workers")


async def _wait_until_empty(paths: list[Path]) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if all(not path.exists() or not any(path.rglob("*")) for path in paths):
            return
        await asyncio.sleep(0.02)
    remaining = [str(item) for path in paths if path.exists() for item in path.rglob("*")]
    raise AssertionError(f"Temporary query data was not cleaned: {remaining}")


@pytest.mark.integration
@pytest.mark.fault
@pytest.mark.timeout(45)
@pytest.mark.asyncio
async def test_real_query_cancel_waits_for_workers_and_stops_later_stages(
    tmp_path: Path,
) -> None:
    coordinator_port, worker_1_port, worker_2_port = (
        _free_port(),
        _free_port(),
        _free_port(),
    )
    worker_ports = (worker_1_port, worker_2_port)
    coordinator_url = f"http://127.0.0.1:{coordinator_port}"
    source = tmp_path / "orders-source.parquet"
    pq.write_table(
        pa.table(
            {
                "id": pa.array(range(100), type=pa.int64()),
                "amount": pa.array(range(100, 200), type=pa.int64()),
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
                    str(Path.cwd() / ".venv" / "Lib" / "site-packages"),
                ]
            ),
        }
    )
    coordinator_environment = base_environment | {
        "DISTRIBUTED_SQL_COORDINATOR_PORT": str(coordinator_port),
        "DISTRIBUTED_SQL_COORDINATOR_CATALOG_PATH": str(tmp_path / "catalog.db"),
        "DISTRIBUTED_SQL_COORDINATOR_REMOTE_TASK_AUTH_TOKEN": TASK_AUTH_TOKEN,
        "DISTRIBUTED_SQL_COORDINATOR_CANCELLATION_TIMEOUT_SECONDS": "8",
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
        for index, port in enumerate(worker_ports, start=1):
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
                "DISTRIBUTED_SQL_WORKER_TASK_START_DELAY_SECONDS": "2",
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
            timeout=15,
            trust_env=False,
        ) as client:
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
                                {"name": "id", "data_type": "int64", "nullable": False},
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
            ).status_code == 201
            assert (
                await client.post(
                    "/api/v1/catalog/namespaces/default/tables/orders/imports",
                    json={
                        "source_location": str(source),
                        "source_format": "parquet",
                        "partition_count": 2,
                    },
                )
            ).status_code == 201

            submitted = await client.post(
                "/api/v1/queries",
                json={"sql": "SELECT id, amount FROM orders ORDER BY id"},
            )
            assert submitted.status_code == 202
            query_id = submitted.json()["query_id"]
            await _wait_for_first_stage_running(worker_ports)

            canceled = await client.delete(f"/api/v1/queries/{query_id}")
            assert canceled.status_code == 200
            assert canceled.json()["state"] == "canceled"
            final = (await client.get(f"/api/v1/queries/{query_id}")).json()
            assert final["state"] == "canceled"

        statuses = await asyncio.gather(*(_worker_tasks(port) for port in worker_ports))
        attempts = [attempt for worker_statuses in statuses for attempt in worker_statuses]
        assert len(attempts) == 2
        assert all(attempt["state"] == "canceled" for attempt in attempts)
        await asyncio.sleep(0.2)
        assert [
            len(await _worker_tasks(port))
            for port in worker_ports
        ] == [1, 1]

        await _wait_until_empty(
            [
                tmp_path / "runtime" / "results",
                tmp_path / "runtime" / "shuffle",
                tmp_path / "worker-1" / query_id,
                tmp_path / "worker-2" / query_id,
            ]
        )
    finally:
        await asyncio.to_thread(_stop, processes)

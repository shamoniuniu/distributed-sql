from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import (
    AttemptState,
    DataType,
    Partition,
    PlanNode,
    PlanNodeType,
    RemoteTaskOperation,
    RemoteTaskResult,
    Schema,
    SchemaField,
    Stage,
    Task,
)
from distributed_sql.coordinator.remote import (
    RemoteTaskCommand,
    RemoteWorker,
)
from distributed_sql.coordinator.remote_execution import RemoteDistributedExecutor
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import (
    RetryPolicy,
    ShuffleManifest,
    StageGraph,
    TaskScheduler,
    materialize_exchanges,
)
from distributed_sql.planner import Binder
from tests.test_distributed_execution import _partitioned_table


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_for_workers(url: str, count: int, processes: list[subprocess.Popen[bytes]]) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        assert all(process.poll() is None for process in processes)
        try:
            response = httpx.get(f"{url}/api/v1/workers", timeout=0.5)
            active = [
                worker for worker in response.json()["workers"] if worker["state"] == "active"
            ]
            if len(active) == count:
                return
        except (httpx.HTTPError, KeyError, ValueError):
            pass
        time.sleep(0.05)
    raise TimeoutError("Remote Worker processes did not register")


def _graph(query_id: str, task_count: int) -> StageGraph:
    stage = Stage(
        stage_id=f"{query_id}-stage",
        query_id=query_id,
        plan=PlanNode(node_id="output", node_type=PlanNodeType.OUTPUT),
        partition_count=task_count,
    )
    tasks = tuple(
        Task(
            task_id=f"{stage.stage_id}-task-{ordinal}",
            query_id=query_id,
            stage_id=stage.stage_id,
            partition=Partition(
                partition_id=f"partition-{ordinal}",
                ordinal=ordinal,
                location="",
            ),
        )
        for ordinal in range(task_count)
    )
    return StageGraph(stage.stage_id, (stage,), tasks)


def _commands(graph: StageGraph, root: Path, seconds: float) -> dict[str, object]:
    return {
        task.task_id: RemoteTaskCommand(
            task_id=task.task_id,
            query_id=task.query_id,
            stage_id=task.stage_id,
            operation=RemoteTaskOperation.SLEEP,
            payload={"seconds": seconds},
            output_root=root,
        )
        for task in graph.tasks
    }


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


def _worker_process_ids(ports: tuple[int, int]) -> dict[str, int]:
    return {
        f"worker-{index}": int(
            httpx.get(f"http://127.0.0.1:{port}/health", timeout=1).json()["process_id"]
        )
        for index, port in enumerate(ports, start=1)
    }


def _kill_process_tree(process_id: int) -> None:
    if sys.platform == "win32":
        subprocess.run(
            ["taskkill", "/PID", str(process_id), "/T", "/F"],
            capture_output=True,
            check=True,
        )
    else:
        os.kill(process_id, 15)


def _wait_for_attempt_running(port: int, attempt_id: str, auth_token: str) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            response = httpx.get(
                f"http://127.0.0.1:{port}/api/v1/tasks/{attempt_id}",
                timeout=0.2,
                headers={"Authorization": f"Bearer {auth_token}"},
            )
            if response.status_code == 200 and response.json()["state"] == "running":
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.01)
    raise TimeoutError("Attempt did not enter running state")


def _start_process(
    module: str,
    environment: dict[str, str],
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", module],
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@pytest.mark.integration
@pytest.mark.fault
@pytest.mark.timeout(40)
@pytest.mark.asyncio
async def test_real_worker_processes_execute_and_retry_after_worker_death(
    tmp_path: Path,
) -> None:
    coordinator_port, worker_1_port, worker_2_port = (
        _free_port(),
        _free_port(),
        _free_port(),
    )
    coordinator_url = f"http://127.0.0.1:{coordinator_port}"
    base_environment = os.environ.copy()
    base_environment["PYTHONUNBUFFERED"] = "1"
    task_auth_token = "task18-integration-token"
    base_environment["PYTHONPATH"] = os.pathsep.join(
        [str(Path.cwd() / "src"), os.environ.get("PYTHONPATH", "")]
    )
    coordinator_environment = base_environment | {
        "DISTRIBUTED_SQL_COORDINATOR_PORT": str(coordinator_port),
        "DISTRIBUTED_SQL_COORDINATOR_CATALOG_PATH": str(tmp_path / "catalog.db"),
        "DISTRIBUTED_SQL_COORDINATOR_LEASE_TTL_SECONDS": "0.5",
        "DISTRIBUTED_SQL_COORDINATOR_LEASE_CHECK_INTERVAL_SECONDS": "0.05",
    }
    coordinator_process = await asyncio.to_thread(
        _start_process,
        "distributed_sql.coordinator.main",
        coordinator_environment,
    )
    processes: list[subprocess.Popen[bytes]] = [coordinator_process]
    worker_processes: list[subprocess.Popen[bytes]] = []
    try:
        for index, port in enumerate((worker_1_port, worker_2_port), start=1):
            environment = base_environment | {
                "DISTRIBUTED_SQL_WORKER_ID": f"worker-{index}",
                "DISTRIBUTED_SQL_WORKER_PORT": str(port),
                "DISTRIBUTED_SQL_WORKER_COORDINATOR_URL": coordinator_url,
                "DISTRIBUTED_SQL_WORKER_HEARTBEAT_INTERVAL_SECONDS": "0.05",
                "DISTRIBUTED_SQL_WORKER_REGISTRATION_RETRY_SECONDS": "0.05",
                "DISTRIBUTED_SQL_WORKER_TEMP_DIRECTORY": str(tmp_path / f"worker-{index}"),
                "DISTRIBUTED_SQL_WORKER_REMOTE_TASK_AUTH_TOKEN": task_auth_token,
            }
            process = await asyncio.to_thread(
                _start_process,
                "distributed_sql.worker.main",
                environment,
            )
            processes.append(process)
            worker_processes.append(process)
        await asyncio.to_thread(_wait_for_workers, coordinator_url, 2, processes)
        worker_process_ids = await asyncio.to_thread(
            _worker_process_ids,
            (worker_1_port, worker_2_port),
        )

        workers = [
            RemoteWorker(
                "worker-1",
                1,
                f"http://127.0.0.1:{worker_1_port}",
                auth_token=task_auth_token,
            ),
            RemoteWorker(
                "worker-2",
                1,
                f"http://127.0.0.1:{worker_2_port}",
                auth_token=task_auth_token,
            ),
        ]
        scheduler = TaskScheduler(
            workers,
            retry_policy=RetryPolicy(
                max_attempts=2,
                backoff_seconds=0,
                attempt_timeout_seconds=5,
                lease_poll_interval_seconds=0.02,
            ),
        )
        distribution_graph = _graph("distribution", 2)
        distributed = await scheduler.run(
            distribution_graph,
            _commands(distribution_graph, tmp_path / "results", 0.05),
        )
        process_ids = {
            outcome.worker_id: outcome.value.worker_process_id
            for outcome in distributed.outcomes.values()
            if isinstance(outcome.value, RemoteTaskResult)
        }
        assert process_ids == worker_process_ids

        schema = Schema(fields=[SchemaField(name="id", data_type=DataType.INT64, nullable=False)])
        table = _partitioned_table(
            tmp_path,
            "remote_items",
            [{"id": 3}, {"id": 1}, {"id": 2}, {"id": 4}],
            schema,
            partition_count=2,
        )
        tables = {"default.remote_items": table}
        physical = materialize_exchanges(
            Binder(tables).bind("SELECT id FROM remote_items ORDER BY id"),
            (),
            partition_count=2,
        )
        remote_executor = RemoteDistributedExecutor(
            tables,
            create_data_source_registry(ObjectStoreRouter(LocalObjectStore())),
            workers,
            ObjectStoreRouter(LocalObjectStore()),
            tmp_path / "query-runtime",
        )
        query_result = await remote_executor.execute("remote-query", physical)
        assert query_result.table.to_pylist() == [
            {"id": 1},
            {"id": 2},
            {"id": 3},
            {"id": 4},
        ]
        query_workers = {
            outcome.worker_id
            for schedule in query_result.schedules
            for outcome in schedule.outcomes.values()
        }
        assert query_workers == {"worker-1", "worker-2"}

        recovery_graph = _graph("recovery", 1)
        recovery = asyncio.create_task(
            TaskScheduler(
                workers,
                retry_policy=RetryPolicy(
                    max_attempts=2,
                    backoff_seconds=0,
                    attempt_timeout_seconds=5,
                    lease_poll_interval_seconds=0.02,
                ),
            ).run(
                recovery_graph,
                _commands(recovery_graph, tmp_path / "results", 2.0),
            )
        )
        await asyncio.to_thread(
            _wait_for_attempt_running,
            worker_1_port,
            "recovery-stage-task-0-attempt-000",
            task_auth_token,
        )
        await asyncio.to_thread(_kill_process_tree, worker_process_ids["worker-1"])
        recovered = await recovery

        task = recovered.tasks[recovery_graph.tasks[0].task_id]
        first, retry = [recovered.attempts[item] for item in task.attempt_ids]
        assert first.worker_id == "worker-1"
        assert first.state is AttemptState.LOST
        assert retry.worker_id == "worker-2"
        assert retry.state is AttemptState.SUCCEEDED
        result = recovered.outcomes[task.task_id].value
        assert isinstance(result, RemoteTaskResult)
        assert result.worker_process_id == worker_process_ids["worker-2"]
        assert result.artifact is not None
        assert Path(result.artifact.location).is_file()
        evidence_path = os.environ.get("DISTRIBUTED_SQL_TASK18_EVIDENCE")
        if evidence_path:
            coordinator_pid = int(
                (
                    await asyncio.to_thread(
                        httpx.get,
                        f"{coordinator_url}/health",
                        timeout=1,
                    )
                ).json()["process_id"]
            )
            shuffle_locations = [
                item.location
                for schedule in query_result.schedules
                for outcome in schedule.outcomes.values()
                if isinstance(outcome.value, RemoteTaskResult)
                for raw_manifest in outcome.value.shuffle_manifests
                for item in ShuffleManifest.model_validate(raw_manifest).files
            ]
            evidence = {
                "protocol_version": 1,
                "protocol": {
                    "payload_validation": "operation-specific Pydantic models",
                    "plan_format": "python-pickle-v5",
                    "plan_version": 1,
                    "task_api_authenticated": True,
                },
                "control_plane": "HTTP/JSON",
                "data_plane": "immutable Parquet files with SHA-256",
                "coordinator_pid": coordinator_pid,
                "worker_process_ids": worker_process_ids,
                "distribution": {
                    task_id: {
                        "worker_id": outcome.worker_id,
                        "worker_process_id": outcome.value.worker_process_id,
                    }
                    for task_id, outcome in distributed.outcomes.items()
                    if isinstance(outcome.value, RemoteTaskResult)
                },
                "remote_query": {
                    "rows": query_result.table.to_pylist(),
                    "workers": sorted(query_workers),
                    "shuffle_records_written": (
                        query_result.shuffle_metrics.records_written
                    ),
                    "shuffle_records_read": query_result.shuffle_metrics.records_read,
                    "artifact_locations": [
                        outcome.value.artifact.location
                        for schedule in query_result.schedules
                        for outcome in schedule.outcomes.values()
                        if isinstance(outcome.value, RemoteTaskResult)
                        and outcome.value.artifact is not None
                    ],
                    "shuffle_locations": shuffle_locations,
                },
                "recovery": {
                    "killed_worker_id": "worker-1",
                    "attempts": [
                        {
                            "attempt_id": attempt.attempt_id,
                            "worker_id": attempt.worker_id,
                            "state": attempt.state.value,
                        }
                        for attempt in (first, retry)
                    ],
                    "result_worker_id": result.worker_id,
                    "result_worker_process_id": result.worker_process_id,
                    "artifact_checksum": result.artifact.checksum,
                },
                "logical_worker_used": False,
            }
            target = Path(evidence_path)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(
                json.dumps(evidence, indent=2, sort_keys=True),
                encoding="utf-8",
            )
    finally:
        _stop(processes)

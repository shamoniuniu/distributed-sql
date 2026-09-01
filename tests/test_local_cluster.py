import os
import subprocess

import pytest

from distributed_sql.common.config import WorkerSettings
from distributed_sql.local_cluster import _parse_args, main


def test_local_cluster_defaults_to_two_workers() -> None:
    args = _parse_args([])
    assert args.workers == 2


def test_local_cluster_rejects_single_worker() -> None:
    with pytest.raises(SystemExit):
        _parse_args(["--workers", "1"])


def test_worker_id_uses_launcher_environment_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DISTRIBUTED_SQL_WORKER_ID", "worker-2")
    assert WorkerSettings().worker_id == "worker-2"


class FakeProcess:
    next_pid = 100

    def __init__(self) -> None:
        self.pid = self.next_pid
        FakeProcess.next_pid += 1
        self.returncode: int | None = None

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.returncode = 0

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        assert self.returncode is not None
        return self.returncode

    def kill(self) -> None:
        self.returncode = -1


def test_local_cluster_starts_coordinator_and_distinct_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launches: list[tuple[list[str], dict[str, str]]] = []
    processes: list[FakeProcess] = []

    def fake_popen(command: list[str], *, env: dict[str, str]) -> FakeProcess:
        launches.append((command, env))
        process = FakeProcess()
        processes.append(process)
        return process

    def stop_after_startup(
        coordinator_url: str,
        worker_count: int,
        started_processes: list[subprocess.Popen[bytes]],
        timeout_seconds: float,
    ) -> None:
        del coordinator_url, worker_count, started_processes, timeout_seconds
        raise KeyboardInterrupt

    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    monkeypatch.setattr(
        "distributed_sql.local_cluster._wait_until_ready",
        stop_after_startup,
    )

    main(["--workers", "2", "--worker-start-port", "9101"])

    assert len(launches) == 3
    assert launches[0][0][-1] == "distributed_sql.coordinator.main"
    assert [launch[1]["DISTRIBUTED_SQL_WORKER_ID"] for launch in launches[1:]] == [
        "worker-1",
        "worker-2",
    ]
    assert [launch[1]["DISTRIBUTED_SQL_WORKER_PORT"] for launch in launches[1:]] == [
        "9101",
        "9102",
    ]
    assert all(process.returncode == 0 for process in processes)
    assert launches[0][1] == os.environ

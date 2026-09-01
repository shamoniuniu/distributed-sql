"""One-command local Coordinator and multi-Worker launcher."""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from typing import Any

from distributed_sql.common.config import CoordinatorSettings, LocalClusterSettings


def _get_json(url: str) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=1.0) as response:
            payload: dict[str, Any] = json.load(response)
            return payload
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return None


def _wait_until_ready(
    coordinator_url: str,
    worker_count: int,
    processes: Sequence[subprocess.Popen[bytes]],
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        failed = [process.pid for process in processes if process.poll() is not None]
        if failed:
            raise RuntimeError(f"Cluster process exited during startup: {failed}")
        health = _get_json(f"{coordinator_url}/health")
        workers = _get_json(f"{coordinator_url}/api/v1/workers")
        active_workers = [
            worker
            for worker in (workers or {}).get("workers", [])
            if worker.get("state") == "active"
        ]
        if health and health.get("status") == "healthy" and len(active_workers) >= worker_count:
            return
        time.sleep(0.2)
    raise TimeoutError(
        f"Cluster did not register {worker_count} Workers within {timeout_seconds} seconds."
    )


def _stop_processes(processes: Sequence[subprocess.Popen[bytes]]) -> None:
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
    deadline = time.monotonic() + 5.0
    for process in reversed(processes):
        remaining = max(0.0, deadline - time.monotonic())
        try:
            process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    defaults = LocalClusterSettings()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=defaults.worker_count)
    parser.add_argument("--worker-start-port", type=int, default=defaults.worker_start_port)
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=defaults.startup_timeout_seconds,
    )
    args = parser.parse_args(argv)
    if args.workers < 2:
        parser.error("--workers must be at least 2")
    if args.worker_start_port + args.workers - 1 > 65535:
        parser.error("Worker port range exceeds 65535")
    return args


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    coordinator = CoordinatorSettings()
    coordinator_url = f"http://{coordinator.host}:{coordinator.port}"
    processes: list[subprocess.Popen[bytes]] = []
    try:
        processes.append(
            subprocess.Popen(
                [sys.executable, "-m", "distributed_sql.coordinator.main"],
                env=os.environ.copy(),
            )
        )
        for index in range(args.workers):
            port = args.worker_start_port + index
            environment = os.environ.copy()
            environment.update(
                {
                    "DISTRIBUTED_SQL_WORKER_ID": f"worker-{index + 1}",
                    "DISTRIBUTED_SQL_WORKER_PORT": str(port),
                    "DISTRIBUTED_SQL_WORKER_COORDINATOR_URL": coordinator_url,
                }
            )
            processes.append(
                subprocess.Popen(
                    [sys.executable, "-m", "distributed_sql.worker.main"],
                    env=environment,
                )
            )
        _wait_until_ready(
            coordinator_url,
            args.workers,
            processes,
            args.startup_timeout,
        )
        print(
            f"Local cluster ready: {coordinator_url}, {args.workers} Workers. Press Ctrl+C to stop."
        )
        while all(process.poll() is None for process in processes):
            time.sleep(0.5)
        raise RuntimeError("A cluster process exited unexpectedly.")
    except KeyboardInterrupt:
        print("Stopping local cluster...")
    finally:
        _stop_processes(processes)


if __name__ == "__main__":
    main()

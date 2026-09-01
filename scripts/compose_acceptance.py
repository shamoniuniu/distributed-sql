"""Run the Task 23 Docker Compose recovery acceptance."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from deployment_smoke import ensure_smoke_table, request_json, wait_for_cluster


def docker(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["docker", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def compose(root: Path, compose_file: Path, *arguments: str) -> str:
    return docker(root, "compose", "-f", str(compose_file), *arguments)


def worker_tasks(
    root: Path,
    compose_file: Path,
    service: str,
    auth_token: str,
) -> list[dict[str, Any]]:
    script = (
        "import json,urllib.request;"
        "r=urllib.request.Request("
        "'http://127.0.0.1:8091/api/v1/tasks',"
        f"headers={{'Authorization':'Bearer {auth_token}'}});"
        "print(json.dumps(json.load(urllib.request.urlopen(r,timeout=2))))"
    )
    payload = compose(root, compose_file, "exec", "-T", service, "python", "-c", script)
    parsed = json.loads(payload)
    tasks = parsed.get("tasks")
    if not isinstance(tasks, list):
        raise RuntimeError(f"Worker returned invalid Task list: {parsed}")
    return tasks


def wait_for_new_running_attempt(
    root: Path,
    compose_file: Path,
    service: str,
    auth_token: str,
    previous_attempts: set[str],
    deadline: float,
) -> dict[str, Any]:
    while time.monotonic() < deadline:
        tasks = worker_tasks(root, compose_file, service, auth_token)
        running = [
            task
            for task in tasks
            if task.get("attempt_id") not in previous_attempts
            and task.get("state") == "running"
        ]
        if running:
            return running[0]
        time.sleep(0.1)
    raise RuntimeError("No new running attempt was observed on worker-1 before timeout")


def submit_query(base_url: str) -> str:
    status, query = request_json(
        base_url,
        "POST",
        "/api/v1/queries",
        {"sql": "SELECT id, value FROM deployment_smoke.shared_numbers ORDER BY id"},
    )
    if status != 202:
        raise RuntimeError(f"Query submission failed: HTTP {status}: {query}")
    return str(query["query_id"])


def wait_for_query(
    base_url: str,
    query_id: str,
    deadline: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    while time.monotonic() < deadline:
        status, query = request_json(base_url, "GET", f"/api/v1/queries/{query_id}")
        if status != 200:
            raise RuntimeError(f"Query status failed: HTTP {status}: {query}")
        if query["state"] == "failed":
            raise RuntimeError(f"Query failed: {query.get('error')}")
        if query["state"] == "canceled":
            raise RuntimeError("Recovery query was unexpectedly canceled")
        if query["state"] == "succeeded":
            break
        time.sleep(0.2)
    else:
        raise RuntimeError(f"Query {query_id} did not finish before timeout")

    status, result = request_json(
        base_url,
        "GET",
        f"/api/v1/queries/{query_id}/results?offset=0&limit=10",
    )
    expected = [
        {"id": 1, "value": 10},
        {"id": 2, "value": 20},
        {"id": 3, "value": 30},
    ]
    if status != 200 or result.get("rows") != expected:
        raise RuntimeError(f"Unexpected query result: HTTP {status}: {result}")
    return query, result


def query_metrics(base_url: str, query_id: str) -> dict[str, Any]:
    status, response = request_json(
        base_url,
        "GET",
        f"/api/v1/queries/{query_id}/metrics",
    )
    if status != 200:
        raise RuntimeError(f"Query metrics failed: HTTP {status}: {response}")
    diagnostics = response.get("diagnostics")
    if not isinstance(diagnostics, dict):
        raise RuntimeError(f"Query metrics omitted diagnostics: {response}")
    return diagnostics


def assert_recovery(
    diagnostics: dict[str, Any],
    killed_worker: str,
    observed_attempt_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    attempts = diagnostics.get("attempts", [])
    retries = diagnostics.get("retries", [])
    lost = next(
        (
            attempt
            for attempt in attempts
            if attempt.get("worker_id") == killed_worker
            and attempt.get("attempt_id") == observed_attempt_id
            and attempt.get("state") == "lost"
        ),
        None,
    )
    if lost is None:
        raise RuntimeError(
            "The attempt observed running before kill was not recorded LOST: "
            f"worker={killed_worker}, attempt={observed_attempt_id}, attempts={attempts}"
        )
    retry = next(
        (
            event
            for event in retries
            if event.get("previous_attempt_id") == lost.get("attempt_id")
            and event.get("previous_state") == "lost"
            and event.get("worker_id") != killed_worker
        ),
        None,
    )
    if retry is None:
        raise RuntimeError(f"No retry on another Worker followed LOST attempt: {retries}")
    succeeded = next(
        (
            attempt
            for attempt in attempts
            if attempt.get("attempt_id") == retry.get("attempt_id")
            and attempt.get("state") == "succeeded"
            and attempt.get("worker_id") == retry.get("worker_id")
        ),
        None,
    )
    if succeeded is None:
        raise RuntimeError(f"Recovery retry did not succeed: {attempts}")
    shuffle = diagnostics.get("runtime", {})
    if (
        shuffle.get("shuffle_records_written", 0) <= 0
        or shuffle.get("shuffle_bytes_written", 0) <= 0
        or shuffle.get("shuffle_records_read", 0) <= 0
        or shuffle.get("shuffle_bytes_read", 0) <= 0
    ):
        raise RuntimeError(f"Remote query did not prove Shuffle read/write: {shuffle}")
    return lost, retry, succeeded


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--compose-file", default="compose.yaml")
    parser.add_argument("--worker-service", default="worker-1")
    parser.add_argument("--worker-id", default="worker-1")
    parser.add_argument("--auth-token", default="local-compose-task-token")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    compose_file = (root / args.compose_file).resolve()
    deadline = time.monotonic() + args.timeout
    active = wait_for_cluster(args.url, 2, deadline)
    active_ids = sorted(str(worker["worker_id"]) for worker in active)
    if active_ids != ["worker-1", "worker-2"]:
        raise RuntimeError(f"Unexpected registered Workers: {active_ids}")

    ensure_smoke_table(args.url, require_existing=False)
    table_status, table = request_json(
        args.url,
        "GET",
        "/api/v1/catalog/namespaces/deployment_smoke/tables/shared_numbers",
    )
    if table_status != 200:
        raise RuntimeError(f"Imported table is unavailable: HTTP {table_status}: {table}")

    previous_attempts = {
        str(task["attempt_id"])
        for task in worker_tasks(
            root,
            compose_file,
            args.worker_service,
            args.auth_token,
        )
    }
    container_id = compose(root, compose_file, "ps", "-q", args.worker_service)
    if not container_id:
        raise RuntimeError(f"Could not resolve container for {args.worker_service}")
    docker(root, "update", "--restart=no", container_id)
    query_id = submit_query(args.url)
    running = wait_for_new_running_attempt(
        root,
        compose_file,
        args.worker_service,
        args.auth_token,
        previous_attempts,
        deadline,
    )
    killed_at = datetime.now(UTC).isoformat()
    docker(root, "kill", container_id)

    query, result = wait_for_query(args.url, query_id, deadline)
    diagnostics = query_metrics(args.url, query_id)
    lost, retry, succeeded = assert_recovery(
        diagnostics,
        args.worker_id,
        str(running["attempt_id"]),
    )

    compose(root, compose_file, "restart", "coordinator")
    persisted_workers = wait_for_cluster(args.url, 1, deadline)
    ensure_smoke_table(args.url, require_existing=True)
    persisted_query_id = submit_query(args.url)
    persisted_query, persisted_result = wait_for_query(
        args.url,
        persisted_query_id,
        deadline,
    )

    rows = result["rows"]
    result_checksum = hashlib.sha256(
        json.dumps(rows, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    runtime = diagnostics["runtime"]
    evidence = {
        "status": "passed",
        "worker_registration": {
            "active_worker_ids": active_ids,
            "count": len(active_ids),
        },
        "csv_import": {
            "format": table["format"],
            "location": table["location"],
            "partition_count": len(table["partitions"]),
            "row_count": table["statistics"]["row_count"],
        },
        "remote_query": {
            "query_id": query_id,
            "state": query["state"],
            "sql": "SELECT id, value FROM deployment_smoke.shared_numbers ORDER BY id",
        },
        "fault_injection": {
            "container_id": container_id,
            "killed_at": killed_at,
            "killed_worker_id": args.worker_id,
            "observed_running_attempt_id": running["attempt_id"],
            "observed_state_before_kill": running["state"],
        },
        "recovery": {
            "lost_attempt": lost,
            "retry_event": retry,
            "succeeded_attempt": succeeded,
            "all_attempts": diagnostics["attempts"],
        },
        "shuffle": {
            "records_written": runtime["shuffle_records_written"],
            "bytes_written": runtime["shuffle_bytes_written"],
            "records_read": runtime["shuffle_records_read"],
            "bytes_read": runtime["shuffle_bytes_read"],
            "partition_count": runtime["shuffle_partition_count"],
            "partition_rows": diagnostics["shuffle_partition_rows"],
            "partition_bytes": diagnostics["shuffle_partition_bytes"],
        },
        "result": {
            "columns": result["columns"],
            "rows": rows,
            "row_count": result["total_rows"],
            "sha256": result_checksum,
        },
        "catalog_restart": {
            "coordinator_restarted": True,
            "catalog_persisted": True,
            "active_worker_ids": sorted(
                str(worker["worker_id"]) for worker in persisted_workers
            ),
            "query_id": persisted_query_id,
            "query_state": persisted_query["state"],
            "rows": persisted_result["rows"],
        },
    }
    target = (root / args.evidence).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(evidence, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

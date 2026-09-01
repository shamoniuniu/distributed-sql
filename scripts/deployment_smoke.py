"""Run an end-to-end query through a deployed Coordinator."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def request_json(
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        error_payload: dict[str, Any] = json.load(exc)
        return exc.code, error_payload


def wait_for_cluster(base_url: str, workers: int, deadline: float) -> list[dict[str, Any]]:
    last_error = "cluster did not answer"
    while time.monotonic() < deadline:
        try:
            health_status, health = request_json(base_url, "GET", "/health")
            nodes_status, nodes = request_json(base_url, "GET", "/api/v1/nodes")
            active = [
                worker
                for worker in nodes.get("workers", [])
                if worker.get("state") == "active"
            ]
            if (
                health_status == 200
                and health.get("status") == "healthy"
                and nodes_status == 200
                and len(active) >= workers
            ):
                return active
            last_error = (
                f"health={health.get('status')!r}, active_workers={len(active)}/{workers}"
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            last_error = str(exc)
        time.sleep(1)
    raise RuntimeError(f"Cluster was not ready before timeout: {last_error}")


def ensure_smoke_table(
    base_url: str,
    require_existing: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    namespace_path = "/api/v1/catalog/namespaces/deployment_smoke"
    table_path = f"{namespace_path}/tables/shared_numbers"
    namespace_status, namespace = request_json(base_url, "GET", namespace_path)
    table_status, table = request_json(base_url, "GET", table_path)

    if require_existing:
        if namespace_status != 200 or table_status != 200:
            raise RuntimeError("Persistent smoke Catalog objects were lost")
        if (
            table.get("statistics", {}).get("row_count") != 3
            or len(table.get("partitions", [])) != 2
        ):
            raise RuntimeError("Persistent imported table metadata was lost")
        return namespace, table

    if namespace_status == 404:
        status, namespace = request_json(
            base_url,
            "POST",
            "/api/v1/catalog/namespaces",
            {"name": "deployment_smoke", "properties": {"purpose": "deployment-smoke"}},
        )
        if status != 201:
            raise RuntimeError(f"Could not create smoke namespace: HTTP {status}: {namespace}")
    elif namespace_status != 200:
        raise RuntimeError(f"Could not read smoke namespace: HTTP {namespace_status}")

    if table_status == 404:
        status, table = request_json(
            base_url,
            "POST",
            f"{namespace_path}/tables",
            {
                "name": "shared_numbers",
                "schema": {
                    "fields": [
                        {"name": "id", "data_type": "int64", "nullable": False},
                        {"name": "value", "data_type": "int64", "nullable": False},
                    ]
                },
                "format": "csv",
                "location": "s3://distributed-sql/deployment-smoke/imported-numbers",
            },
        )
        if status != 201:
            raise RuntimeError(f"Could not create smoke table: HTTP {status}: {table}")
        status, imported = request_json(
            base_url,
            "POST",
            f"{table_path}/imports",
            {
                "source_location": "/opt/distributed-sql/examples/smoke-data.csv",
                "source_format": "csv",
                "partition_count": 2,
                "partition_key": "id",
            },
        )
        if (
            status != 201
            or imported.get("table", {}).get("statistics", {}).get("row_count") != 3
            or len(imported.get("table", {}).get("partitions", [])) != 2
        ):
            raise RuntimeError(f"Could not import smoke table: HTTP {status}: {imported}")
    elif table_status != 200:
        raise RuntimeError(f"Could not read smoke table: HTTP {table_status}")

    namespace_status, namespace = request_json(base_url, "GET", namespace_path)
    table_status, table = request_json(base_url, "GET", table_path)
    if namespace_status != 200 or table_status != 200:
        raise RuntimeError("Could not read smoke Catalog objects after import")
    if (
        table.get("statistics", {}).get("row_count") != 3
        or len(table.get("partitions", [])) != 2
    ):
        raise RuntimeError(f"Unexpected imported table metadata: {table}")
    return namespace, table


def run_query(
    base_url: str,
    deadline: float,
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    status, query = request_json(
        base_url,
        "POST",
        "/api/v1/queries",
        {"sql": "SELECT id, value FROM deployment_smoke.shared_numbers ORDER BY id"},
    )
    if status != 202:
        raise RuntimeError(f"Query submission failed: HTTP {status}: {query}")
    query_id = str(query["query_id"])

    while time.monotonic() < deadline:
        status, query = request_json(base_url, "GET", f"/api/v1/queries/{query_id}")
        if status != 200:
            raise RuntimeError(f"Query status failed: HTTP {status}: {query}")
        if query["state"] == "failed":
            raise RuntimeError(f"Query failed: {query.get('error')}")
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
    return query_id, query, result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--require-existing-catalog", action="store_true")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    deadline = time.monotonic() + args.timeout

    active = wait_for_cluster(args.url, args.workers, deadline)
    namespace, table = ensure_smoke_table(args.url, args.require_existing_catalog)
    query_id, query, query_result = run_query(args.url, deadline)
    result = {
        "schema_version": 1,
        "status": "passed",
        "query_id": query_id,
        "active_workers": [worker["worker_id"] for worker in active],
        "catalog_persisted": args.require_existing_catalog,
        "catalog": {
            "namespace": namespace,
            "table": table,
        },
        "query": query,
        "query_result": query_result,
        "table_location": "s3://distributed-sql/deployment-smoke/imported-numbers",
        "remote_runtime_root": "s3://distributed-sql/runtime",
    }
    if args.evidence:
        target = Path(args.evidence)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(result, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

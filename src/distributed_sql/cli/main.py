"""Command-line client for the Distributed SQL Coordinator."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from typing import Any, TextIO, cast

import httpx

_TERMINAL_STATES = {"succeeded", "failed", "canceled"}


def _request(
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any] | None:
    response = httpx.request(
        method,
        f"{base_url.rstrip('/')}{path}",
        json=payload,
        timeout=30.0,
    )
    try:
        body = cast(dict[str, Any] | list[Any] | None, response.json())
    except json.JSONDecodeError:
        body = None
    if response.is_error:
        if isinstance(body, dict) and isinstance(body.get("error"), dict):
            error = cast(dict[str, Any], body["error"])
            raise RuntimeError(f"{error.get('code')}: {error.get('message')}")
        raise RuntimeError(f"HTTP {response.status_code}: {response.text}")
    return body


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError("Coordinator returned an invalid response.")
    return cast(dict[str, Any], value)


def _print_json(value: object, output: TextIO) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2), file=output)


def _wait_for_query(
    base_url: str,
    query_id: str,
    interval: float,
) -> dict[str, Any]:
    while True:
        query = _object(_request(base_url, "GET", f"/api/v1/queries/{query_id}"))
        if query.get("state") in _TERMINAL_STATES:
            return query
        time.sleep(interval)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.getenv("DISTRIBUTED_SQL_URL", "http://127.0.0.1:8080"),
        help="Coordinator base URL",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    query = commands.add_parser("query", help="Submit SQL and print its result")
    query.add_argument("sql")
    query.add_argument("--no-wait", action="store_true")
    query.add_argument("--page-size", type=int, default=100)
    query.add_argument("--poll-interval", type=float, default=0.1)

    explain = commands.add_parser("explain", help="Print logical and physical plans")
    explain.add_argument("sql")

    status = commands.add_parser("status", help="Show query status")
    status.add_argument("query_id")

    cancel = commands.add_parser("cancel", help="Cancel a query")
    cancel.add_argument("query_id")

    catalog = commands.add_parser("catalog", help="Manage Catalog metadata")
    catalog_commands = catalog.add_subparsers(dest="catalog_command", required=True)
    catalog_commands.add_parser("namespaces", help="List namespaces")
    tables = catalog_commands.add_parser("tables", help="List tables")
    tables.add_argument("namespace")
    create_namespace = catalog_commands.add_parser(
        "create-namespace",
        help="Create a namespace",
    )
    create_namespace.add_argument("name")
    delete_namespace = catalog_commands.add_parser(
        "delete-namespace",
        help="Delete an empty namespace",
    )
    delete_namespace.add_argument("name")
    create_table = catalog_commands.add_parser("create-table", help="Register a table")
    create_table.add_argument("namespace")
    create_table.add_argument("name")
    create_table.add_argument(
        "--format",
        required=True,
        choices=["csv", "parquet", "avro", "orc", "iceberg"],
    )
    create_table.add_argument("--location", required=True)
    create_table.add_argument(
        "--schema",
        required=True,
        help='JSON schema, for example {"fields":[{"name":"id","data_type":"int64"}]}',
    )
    delete_table = catalog_commands.add_parser("delete-table", help="Delete a table")
    delete_table.add_argument("namespace")
    delete_table.add_argument("name")

    import_command = commands.add_parser("import", help="Import and partition table data")
    import_command.add_argument("namespace")
    import_command.add_argument("table")
    import_command.add_argument("source")
    import_command.add_argument(
        "--source-format",
        choices=["csv", "parquet", "orc"],
    )
    import_command.add_argument("--partitions", type=int, default=1)
    import_command.add_argument("--key")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    output: TextIO = sys.stdout,
    error: TextIO = sys.stderr,
) -> int:
    args = _parser().parse_args(argv)
    try:
        result = _dispatch(args)
        _print_json(result, output)
        return 0
    except (httpx.HTTPError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(str(exc), file=error)
        return 1


def _dispatch(args: argparse.Namespace) -> object:
    if args.command == "query":
        submitted = _object(
            _request(args.url, "POST", "/api/v1/queries", {"sql": args.sql})
        )
        if args.no_wait:
            return submitted
        query_id = str(submitted["query_id"])
        query = _wait_for_query(args.url, query_id, args.poll_interval)
        if query["state"] != "succeeded":
            return query
        return _request(
            args.url,
            "GET",
            f"/api/v1/queries/{query_id}/results?offset=0&limit={args.page_size}",
        )
    if args.command == "explain":
        return _request(args.url, "POST", "/api/v1/queries/explain", {"sql": args.sql})
    if args.command == "status":
        return _request(args.url, "GET", f"/api/v1/queries/{args.query_id}")
    if args.command == "cancel":
        return _request(args.url, "DELETE", f"/api/v1/queries/{args.query_id}")
    if args.command == "import":
        payload = {
            "source_location": args.source,
            "source_format": args.source_format,
            "partition_count": args.partitions,
            "partition_key": args.key,
        }
        return _request(
            args.url,
            "POST",
            f"/api/v1/catalog/namespaces/{args.namespace}/tables/{args.table}/imports",
            payload,
        )
    return _catalog_command(args)


def _catalog_command(args: argparse.Namespace) -> object:
    command = args.catalog_command
    if command == "namespaces":
        return _request(args.url, "GET", "/api/v1/catalog/namespaces")
    if command == "tables":
        return _request(
            args.url,
            "GET",
            f"/api/v1/catalog/namespaces/{args.namespace}/tables",
        )
    if command == "create-namespace":
        return _request(
            args.url,
            "POST",
            "/api/v1/catalog/namespaces",
            {"name": args.name},
        )
    if command == "delete-namespace":
        return _request(
            args.url,
            "DELETE",
            f"/api/v1/catalog/namespaces/{args.name}",
        )
    if command == "create-table":
        return _request(
            args.url,
            "POST",
            f"/api/v1/catalog/namespaces/{args.namespace}/tables",
            {
                "name": args.name,
                "schema": json.loads(args.schema),
                "format": args.format,
                "location": args.location,
            },
        )
    if command == "delete-table":
        return _request(
            args.url,
            "DELETE",
            f"/api/v1/catalog/namespaces/{args.namespace}/tables/{args.name}",
        )
    raise RuntimeError(f"Unsupported catalog command: {command}")


if __name__ == "__main__":
    raise SystemExit(main())

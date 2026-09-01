"""Generate deterministic data and run the Task 16 acceptance workloads."""

from __future__ import annotations

import argparse
import asyncio
import ctypes
import gc
import hashlib
import importlib
import json
import math
import os
import shutil
import sys
import tempfile
import threading
import time
from ctypes import wintypes
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from importlib.resources import files as resource_files
from pathlib import Path
from typing import Any, ClassVar, cast

import duckdb
import pyarrow as pa
import pyarrow.csv as arrow_csv
import pyarrow.orc as arrow_orc
import pyarrow.parquet as pq
from fastavro import reader as avro_reader
from fastavro import writer as avro_writer

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter
from distributed_sql.common.protocol import (
    DataType,
    Partition,
    PartitionStrategy,
    Schema,
    SchemaField,
    Statistics,
)
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import (
    DistributedExecutor,
    LogicalWorker,
    ShuffleStore,
    materialize_exchanges,
)
from distributed_sql.optimizer import CostBasedOptimizer, JoinStrategy
from distributed_sql.planner import Binder

GIB = 1024**3
DEFAULT_MEMORY_BUDGET = 64 * 1024**2
DATASET_NAMES = ("sort_input", "join_left", "join_right", "aggregate_input")
STRESS_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("sort_key", pa.int64(), nullable=False),
        pa.field("group_key", pa.int64(), nullable=False),
        pa.field("amount", pa.int64(), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
    ]
)
CATALOG_SCHEMA = Schema(
    fields=[
        SchemaField(name="id", data_type=DataType.INT64, nullable=False),
        SchemaField(name="sort_key", data_type=DataType.INT64, nullable=False),
        SchemaField(name="group_key", data_type=DataType.INT64, nullable=False),
        SchemaField(name="amount", data_type=DataType.INT64, nullable=False),
        SchemaField(name="payload", data_type=DataType.STRING, nullable=False),
    ]
)


@dataclass(frozen=True)
class GeneratedData:
    root: Path
    manifest_path: Path
    total_bytes: int
    total_rows: int
    files: dict[str, tuple[Path, ...]]


class RSSMonitor:
    """Poll process RSS; execution accounting remains a separate metric."""

    def __init__(self, interval_seconds: float = 0.05) -> None:
        self.interval_seconds = interval_seconds
        self.baseline_bytes = current_rss_bytes()
        self.peak_bytes = self.baseline_bytes
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> RSSMonitor:
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        self._thread.join()
        self.peak_bytes = max(self.peak_bytes, current_rss_bytes())

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            self.peak_bytes = max(self.peak_bytes, current_rss_bytes())


def current_rss_bytes() -> int:
    if os.name == "nt":
        class ProcessMemoryCounters(ctypes.Structure):
            _fields_: ClassVar[list[tuple[str, Any]]] = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        win_dll = getattr(ctypes, "WinDLL", None)
        if win_dll is None:
            raise OSError("WinDLL is unavailable")
        kernel32 = win_dll("kernel32", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        get_memory_info = kernel32.K32GetProcessMemoryInfo
        get_memory_info.argtypes = [
            wintypes.HANDLE,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        get_memory_info.restype = wintypes.BOOL
        handle = kernel32.GetCurrentProcess()
        ok = get_memory_info(
            handle, ctypes.byref(counters), counters.cb
        )
        if not ok:
            raise OSError("GetProcessMemoryInfo failed")
        return int(counters.WorkingSetSize)
    statm = Path("/proc/self/statm")
    if statm.exists():
        resident_pages = int(statm.read_text(encoding="ascii").split()[1])
        sysconf = getattr(os, "sysconf", None)
        if sysconf is None:
            raise OSError("sysconf is unavailable")
        return resident_pages * int(sysconf("SC_PAGE_SIZE"))
    resource = importlib.import_module("resource")

    rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return rss if sys.platform == "darwin" else rss * 1024


def generate_sample_data(root: Path) -> dict[str, Path]:
    root.mkdir(parents=True, exist_ok=True)
    rows = [
        {"id": 1, "region": "north", "amount": 10.5},
        {"id": 2, "region": "south", "amount": 20.0},
        {"id": 3, "region": None, "amount": 30.5},
        {"id": 4, "region": "west", "amount": 40.0},
    ]
    table = pa.Table.from_pylist(rows)
    paths = {suffix: root / f"orders.{suffix}" for suffix in ("csv", "parquet", "avro", "orc")}
    with paths["csv"].open("wb") as output:
        arrow_csv.write_csv(table, output)
    pq.write_table(table, paths["parquet"])
    arrow_orc.write_table(table, paths["orc"])
    avro_schema = {
        "type": "record",
        "name": "order",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "region", "type": ["null", "string"]},
            {"name": "amount", "type": "double"},
        ],
    }
    with paths["avro"].open("wb") as output:
        avro_writer(
            output,
            avro_schema,
            rows,
            sync_marker=b"distributed-sql!",
        )
    return paths


def read_sample_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix == ".csv":
        return cast(
            list[dict[str, Any]],
            arrow_csv.read_csv(
                path,
                convert_options=arrow_csv.ConvertOptions(
                    null_values=[""],
                    strings_can_be_null=True,
                ),
            ).to_pylist(),
        )
    if path.suffix == ".parquet":
        return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())
    if path.suffix == ".orc":
        if sys.platform == "win32":
            os.environ.setdefault(
                "TZDIR",
                str(resource_files("tzdata").joinpath("zoneinfo")),
            )
        return cast(
            list[dict[str, Any]],
            arrow_orc.ORCFile(path).read().to_pylist(),
        )
    with path.open("rb") as source:
        return cast(list[dict[str, Any]], list(avro_reader(source)))


def generate_stress_data(
    root: Path,
    *,
    target_bytes: int = GIB,
    payload_bytes: int = 64 * 1024,
    partition_count: int = 8,
    batch_rows: int = 64,
    timeout_seconds: float = 900,
) -> GeneratedData:
    if target_bytes < 1 or payload_bytes < 128 or partition_count < 1 or batch_rows < 1:
        raise ValueError("stress generation arguments must be positive and payload >= 128")
    signature = {
        "version": 1,
        "target_bytes": target_bytes,
        "payload_bytes": payload_bytes,
        "partition_count": partition_count,
        "batch_rows": batch_rows,
        "datasets": list(DATASET_NAMES),
    }
    manifest_path = root / "manifest.json"
    cached = _load_generated_data(root, manifest_path, signature)
    if cached is not None:
        return cached

    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    deadline = time.monotonic() + timeout_seconds
    per_table_target = math.ceil(target_bytes / len(DATASET_NAMES))
    row_count = math.ceil(per_table_target / payload_bytes)
    files: dict[str, tuple[Path, ...]] = {}
    file_records: list[dict[str, Any]] = []
    for dataset_index, name in enumerate(DATASET_NAMES):
        paths: list[Path] = []
        dataset_root = root / name
        dataset_root.mkdir()
        for partition in range(partition_count):
            if time.monotonic() >= deadline:
                raise TimeoutError("stress data generation exceeded its stage timeout")
            start = row_count * partition // partition_count
            end = row_count * (partition + 1) // partition_count
            path = dataset_root / f"part-{partition:05d}.parquet"
            _write_stress_partition(
                path,
                name,
                dataset_index,
                start,
                end,
                row_count,
                payload_bytes,
                batch_rows,
                deadline,
            )
            paths.append(path)
            file_records.append(
                {
                    "dataset": name,
                    "path": path.relative_to(root).as_posix(),
                    "rows": end - start,
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
        files[name] = tuple(paths)
    total_bytes = sum(int(item["bytes"]) for item in file_records)
    if total_bytes < target_bytes:
        raise RuntimeError(
            f"generated files total {total_bytes} bytes, below requested {target_bytes}"
        )
    payload = signature | {
        "generated_at": datetime.now(UTC).isoformat(),
        "total_bytes": total_bytes,
        "total_rows": row_count * len(DATASET_NAMES),
        "rows_per_dataset": row_count,
        "files": file_records,
    }
    manifest_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return GeneratedData(
        root,
        manifest_path,
        total_bytes,
        row_count * len(DATASET_NAMES),
        files,
    )


def _write_stress_partition(
    path: Path,
    name: str,
    dataset_index: int,
    start: int,
    end: int,
    row_count: int,
    payload_bytes: int,
    batch_rows: int,
    deadline: float,
) -> None:
    writer = pq.ParquetWriter(
        path,
        STRESS_SCHEMA,
        compression="NONE",
        use_dictionary=False,
        write_statistics=False,
    )
    try:
        for offset in range(start, end, batch_rows):
            if time.monotonic() >= deadline:
                raise TimeoutError("stress data generation exceeded its stage timeout")
            stop = min(offset + batch_rows, end)
            ids = list(range(offset, stop))
            if name == "join_right":
                ids = [value + row_count - 16 for value in ids]
            rows = [
                {
                    "id": row_id,
                    "sort_key": (row_count - source_id) * 17 + dataset_index,
                    "group_key": source_id % 64,
                    "amount": source_id % 997,
                    "payload": _fixed_payload(name, source_id % 64, payload_bytes),
                }
                for source_id, row_id in zip(range(offset, stop), ids, strict=True)
            ]
            writer.write_table(pa.Table.from_pylist(rows, schema=STRESS_SCHEMA))
    finally:
        writer.close()


def _fixed_payload(dataset: str, group: int, size: int) -> str:
    prefix = f"{dataset}:{group:04d}:"
    pattern = hashlib.sha256(prefix.encode()).hexdigest()
    repeats = (size - len(prefix) + len(pattern) - 1) // len(pattern)
    return (prefix + pattern * repeats)[:size]


def _load_generated_data(
    root: Path,
    manifest_path: Path,
    signature: dict[str, Any],
) -> GeneratedData | None:
    if not manifest_path.is_file():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if any(manifest.get(key) != value for key, value in signature.items()):
            return None
        files: dict[str, list[Path]] = {name: [] for name in DATASET_NAMES}
        for item in manifest["files"]:
            path = root / item["path"]
            if not path.is_file() or path.stat().st_size != item["bytes"]:
                return None
            files[item["dataset"]].append(path)
        if manifest["total_bytes"] < signature["target_bytes"]:
            return None
        return GeneratedData(
            root,
            manifest_path,
            manifest["total_bytes"],
            manifest["total_rows"],
            {name: tuple(paths) for name, paths in files.items()},
        )
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _catalog_tables(data: GeneratedData) -> dict[str, CatalogTable]:
    now = datetime.now(UTC)
    tables: dict[str, CatalogTable] = {}
    for name, paths in data.files.items():
        partitions = [
            Partition(
                partition_id=f"{name}-{ordinal:05d}",
                ordinal=ordinal,
                location=str(path),
                strategy=PartitionStrategy.ROUND_ROBIN,
                row_count=pq.ParquetFile(path).metadata.num_rows,
                size_bytes=path.stat().st_size,
            )
            for ordinal, path in enumerate(paths)
        ]
        tables[f"default.{name}"] = CatalogTable(
            namespace="default",
            name=name,
            schema=CATALOG_SCHEMA,
            format=TableFormat.PARQUET,
            location=str(data.root / name),
            partition_strategy=PartitionStrategy.ROUND_ROBIN,
            partitions=partitions,
            statistics=Statistics(
                row_count=sum(item.row_count or 0 for item in partitions),
                size_bytes=sum(item.size_bytes or 0 for item in partitions),
                source="task16-generator",
            ),
            created_at=now,
            updated_at=now,
        )
    return tables


def _duckdb_result(data: GeneratedData, sql: str, temp_root: Path) -> pa.Table:
    connection = duckdb.connect()
    try:
        connection.execute(f"SET temp_directory='{temp_root.as_posix()}'")
        for name in DATASET_NAMES:
            pattern = (data.root / name / "*.parquet").as_posix().replace("'", "''")
            connection.execute(
                f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{pattern}')"
            )
        return connection.execute(sql).fetch_arrow_table()
    finally:
        connection.close()


async def _engine_result(
    data: GeneratedData,
    sql: str,
    query_id: str,
    runtime_root: Path,
    memory_budget_bytes: int,
    timeout_seconds: float,
) -> tuple[pa.Table, dict[str, Any]]:
    tables = _catalog_tables(data)
    optimization = CostBasedOptimizer(
        tables,
        memory_budget_bytes=memory_budget_bytes,
        broadcast_threshold_bytes=0,
    ).optimize(Binder(tables).bind(sql))
    decisions = optimization.join_decisions
    if decisions:
        decisions = tuple(
            replace(
                decision,
                strategy=JoinStrategy.REPARTITION_BOTH,
                build_side="right",
            )
            for decision in decisions
        )
    physical = materialize_exchanges(
        optimization.optimized_plan,
        decisions,
        partition_count=2,
    )
    stores = ObjectStoreRouter(LocalObjectStore())
    executor = DistributedExecutor(
        tables,
        create_data_source_registry(stores),
        [LogicalWorker("worker-1", 1), LogicalWorker("worker-2", 1)],
        ShuffleStore(str(runtime_root / "shuffle"), stores),
        memory_limit_bytes=memory_budget_bytes,
        temp_root=runtime_root / "spill",
    )
    result = await asyncio.wait_for(
        executor.execute(query_id, physical),
        timeout=timeout_seconds,
    )
    spill = result.spill_metrics
    shuffle = result.shuffle_metrics
    return result.table, {
        "execution_account_budget_bytes": memory_budget_bytes,
        "execution_account_peak_bytes": spill.peak_memory_bytes,
        "spill_bytes": spill.spill_bytes,
        "spill_files": spill.spill_files,
        "spill_count": spill.spill_count,
        "external_sort_runs": spill.external_sort_runs,
        "hash_partitions": spill.hash_partitions,
        "sort_merge_fallbacks": spill.sort_merge_fallbacks,
        "sort_aggregate_runs": spill.sort_aggregate_runs,
        "shuffle_records_written": shuffle.records_written,
        "shuffle_bytes_written": shuffle.bytes_written,
        "shuffle_records_read": shuffle.records_read,
        "shuffle_bytes_read": shuffle.bytes_read,
        "shuffle_partition_count": shuffle.partition_count,
    }


def _signature(table: pa.Table) -> dict[str, Any]:
    digest = hashlib.sha256()
    for row in table.to_pylist():
        digest.update(
            json.dumps(
                _normalize_result(row),
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        )
        digest.update(b"\n")
    return {
        "columns": table.column_names,
        "rows": table.num_rows,
        "sha256": digest.hexdigest(),
    }


def _normalize_result(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else str(value.normalize())
    if isinstance(value, dict):
        return {key: _normalize_result(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_result(item) for item in value]
    return value


def run_acceptance(
    root: Path,
    *,
    target_bytes: int = GIB,
    payload_bytes: int = 64 * 1024,
    partition_count: int = 8,
    memory_budget_bytes: int = DEFAULT_MEMORY_BUDGET,
    generation_timeout_seconds: float = 900,
    query_timeout_seconds: float = 900,
    report_path: Path | None = None,
    summary_path: Path | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    sample_paths = generate_sample_data(root / "examples")
    data = generate_stress_data(
        root / "data",
        target_bytes=target_bytes,
        payload_bytes=payload_bytes,
        partition_count=partition_count,
        timeout_seconds=generation_timeout_seconds,
    )
    runtime_root = (
        Path(tempfile.gettempdir())
        / f"dsql16-{hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:10]}"
    )
    shutil.rmtree(runtime_root, ignore_errors=True)
    runtime_root.mkdir(parents=True)
    queries = {
        "sort": """
            SELECT id, sort_key, payload
            FROM sort_input
            ORDER BY sort_key, id
            LIMIT 16
        """,
        "join": """
            SELECT l.id AS id, l.payload AS left_payload, r.payload AS right_payload
            FROM join_left l JOIN join_right r ON l.id = r.id
            ORDER BY id
        """,
        "aggregate": """
            SELECT group_key, payload, COUNT(*) AS row_count, SUM(amount) AS amount_sum
            FROM aggregate_input
            GROUP BY group_key, payload
            ORDER BY group_key
        """,
    }
    workloads: dict[str, Any] = {}
    for name, sql in queries.items():
        duck_started = time.perf_counter()
        expected = _duckdb_result(data, sql, runtime_root / "duckdb")
        duck_seconds = time.perf_counter() - duck_started
        with RSSMonitor() as rss:
            engine_started = time.perf_counter()
            actual, metrics = asyncio.run(
                _engine_result(
                    data,
                    sql,
                    f"task16-{name}",
                    runtime_root / name,
                    memory_budget_bytes,
                    query_timeout_seconds,
                )
            )
            engine_seconds = time.perf_counter() - engine_started
        actual_signature = _signature(actual)
        expected_signature = _signature(expected)
        if actual_signature != expected_signature:
            raise AssertionError(
                f"{name} differs from DuckDB: actual={actual_signature}, "
                f"expected={expected_signature}"
            )
        if metrics["execution_account_peak_bytes"] > memory_budget_bytes:
            raise AssertionError(f"{name} exceeded the execution account budget")
        if metrics["spill_bytes"] <= 0:
            raise AssertionError(f"{name} did not spill")
        if name == "sort" and metrics["external_sort_runs"] <= 0:
            raise AssertionError("sort did not use external runs")
        if name == "join" and metrics["hash_partitions"] <= 0:
            raise AssertionError("join did not use spilled hash partitions")
        if name == "aggregate" and metrics["sort_aggregate_runs"] <= 0:
            raise AssertionError("aggregate did not use spilled sort runs")
        workloads[name] = {
            "status": "passed",
            "engine_seconds": round(engine_seconds, 6),
            "duckdb_reference_seconds": round(duck_seconds, 6),
            "result": actual_signature,
            "process_rss_baseline_bytes": rss.baseline_bytes,
            "process_rss_peak_bytes": rss.peak_bytes,
            "process_rss_margin_over_execution_budget_bytes": max(
                0, rss.peak_bytes - memory_budget_bytes
            ),
            **metrics,
        }
        del actual, expected
        shutil.rmtree(runtime_root / name, ignore_errors=True)
        gc.collect()
    report = {
        "task": 16,
        "status": "passed",
        "generated_at": datetime.now(UTC).isoformat(),
        "duration_seconds": round(time.perf_counter() - started, 6),
        "data": {
            "root": str(data.root.resolve()),
            "manifest": str(data.manifest_path.resolve()),
            "example_files": {
                name: str(path.resolve()) for name, path in sample_paths.items()
            },
            "target_bytes": target_bytes,
            "actual_bytes": data.total_bytes,
            "actual_gib": round(data.total_bytes / GIB, 6),
            "rows": data.total_rows,
            "files": sum(len(paths) for paths in data.files.values()),
        },
        "workers": {
            "count": 2,
            "execution_account_budget_bytes_each": memory_budget_bytes,
            "note": (
                "Execution-account peaks cover charged operator rows. Process RSS also "
                "includes Python, PyArrow, scans, results, and runtime allocator retention."
            ),
        },
        "reference": {
            "engine": "DuckDB",
            "usage": "reference result only; tested path is DistributedExecutor",
        },
        "workloads": workloads,
    }
    actual_report = report_path or root / "task16-results.json"
    actual_summary = summary_path or root / "task16-summary.md"
    actual_report.parent.mkdir(parents=True, exist_ok=True)
    actual_report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    actual_summary.write_text(_markdown_summary(report), encoding="utf-8")
    shutil.rmtree(runtime_root, ignore_errors=True)
    return report


def _markdown_summary(report: dict[str, Any]) -> str:
    data = report["data"]
    lines = [
        "# Task 16 自动化验收摘要",
        "",
        f"- 状态: {report['status']}",
        f"- 总耗时: {report['duration_seconds']:.3f} 秒",
        f"- 压力数据: {data['actual_bytes']} 字节 ({data['actual_gib']:.6f} GiB)",
        f"- Worker: 2 个; 每个执行账户预算: {DEFAULT_MEMORY_BUDGET} 字节 (64 MiB)",
        "- 正确性: 排序、Join、聚合结果摘要均与 DuckDB 参考结果一致",
        "",
        "| 工作负载 | 引擎耗时(s) | 账户峰值(B) | 进程RSS峰值(B) | Spill(B) | Shuffle写(B) |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, item in report["workloads"].items():
        lines.append(
            f"| {name} | {item['engine_seconds']:.3f} | "
            f"{item['execution_account_peak_bytes']} | {item['process_rss_peak_bytes']} | "
            f"{item['spill_bytes']} | {item['shuffle_bytes_written']} |"
        )
    lines.extend(
        [
            "",
            "> 执行账户预算只统计算子显式 charge 的行对象; 进程 RSS 还包含 Python,",
            "> PyArrow、扫描输入、结果表及分配器保留内存, 两者不可混同。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("artifacts/task16"))
    parser.add_argument("--target-bytes", type=int, default=GIB)
    parser.add_argument("--payload-bytes", type=int, default=64 * 1024)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--memory-budget-bytes", type=int, default=DEFAULT_MEMORY_BUDGET)
    parser.add_argument("--generation-timeout", type=float, default=900)
    parser.add_argument("--query-timeout", type=float, default=900)
    args = parser.parse_args()
    report = run_acceptance(
        args.root,
        target_bytes=args.target_bytes,
        payload_bytes=args.payload_bytes,
        partition_count=args.partitions,
        memory_budget_bytes=args.memory_budget_bytes,
        generation_timeout_seconds=args.generation_timeout,
        query_timeout_seconds=args.query_timeout,
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

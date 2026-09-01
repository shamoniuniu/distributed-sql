"""Immutable Parquet shuffle files with attempt-isolated atomic manifests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from time import perf_counter
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import Field

from distributed_sql.catalog.storage import ObjectStoreRouter
from distributed_sql.common.protocol import ProtocolModel
from distributed_sql.planner.expressions import Expression, SQLValue


class ShuffleFile(ProtocolModel):
    partition: int = Field(ge=0)
    location: str
    row_count: int = Field(ge=0)
    size_bytes: int = Field(ge=0)
    checksum: str


class ShuffleMetrics(ProtocolModel):
    records_written: int = Field(default=0, ge=0)
    bytes_written: int = Field(default=0, ge=0)
    records_read: int = Field(default=0, ge=0)
    bytes_read: int = Field(default=0, ge=0)
    partition_count: int = Field(default=0, ge=0)
    write_seconds: float = Field(default=0.0, ge=0)
    read_seconds: float = Field(default=0.0, ge=0)
    spill_bytes: int = Field(default=0, ge=0)

    def add(self, other: ShuffleMetrics) -> ShuffleMetrics:
        return ShuffleMetrics(
            records_written=self.records_written + other.records_written,
            bytes_written=self.bytes_written + other.bytes_written,
            records_read=self.records_read + other.records_read,
            bytes_read=self.bytes_read + other.bytes_read,
            partition_count=max(self.partition_count, other.partition_count),
            write_seconds=self.write_seconds + other.write_seconds,
            read_seconds=self.read_seconds + other.read_seconds,
            spill_bytes=self.spill_bytes + other.spill_bytes,
        )


class ShuffleManifest(ProtocolModel):
    version: Literal[1] = 1
    query_id: str
    stage_id: str
    task_id: str
    attempt_id: str
    files: list[ShuffleFile]
    metrics: ShuffleMetrics


class CancellationCheck(Protocol):
    def check(self) -> None: ...


@dataclass(slots=True)
class ShuffleStore:
    root: str
    stores: ObjectStoreRouter

    def write(
        self,
        *,
        query_id: str,
        stage_id: str,
        task_id: str,
        attempt_id: str,
        table: pa.Table,
        partition_count: int,
        keys: tuple[Expression, ...] = (),
        broadcast: bool = False,
        cancellation: CancellationCheck | None = None,
    ) -> ShuffleManifest:
        if partition_count < 1:
            raise ValueError("partition_count must be positive")
        if not broadcast and partition_count > 1 and not keys:
            raise ValueError("partitioned shuffle requires keys")
        started = perf_counter()
        assignments: list[list[int]] = [[] for _ in range(partition_count)]
        rows = table.to_pylist()
        for index, row in enumerate(rows):
            if cancellation is not None:
                cancellation.check()
            targets = (
                range(partition_count)
                if broadcast
                else (_partition(row, keys, partition_count),)
            )
            for target in targets:
                assignments[target].append(index)
        files: list[ShuffleFile] = []
        for partition, indexes in enumerate(assignments):
            if cancellation is not None:
                cancellation.check()
            output = table.take(pa.array(indexes, type=pa.int64()))
            sink = pa.BufferOutputStream()
            pq.write_table(output, sink)
            payload = sink.getvalue().to_pybytes()
            location = self.data_location(
                query_id, stage_id, task_id, attempt_id, partition
            )
            self.stores.for_location(location).write_bytes(location, payload)
            files.append(
                ShuffleFile(
                    partition=partition,
                    location=location,
                    row_count=output.num_rows,
                    size_bytes=len(payload),
                    checksum=hashlib.sha256(payload).hexdigest(),
                )
            )
        metrics = ShuffleMetrics(
            records_written=sum(item.row_count for item in files),
            bytes_written=sum(item.size_bytes for item in files),
            partition_count=partition_count,
            write_seconds=perf_counter() - started,
        )
        manifest = ShuffleManifest(
            query_id=query_id,
            stage_id=stage_id,
            task_id=task_id,
            attempt_id=attempt_id,
            files=files,
            metrics=metrics,
        )
        location = self.manifest_location(query_id, stage_id, task_id, attempt_id)
        if cancellation is not None:
            cancellation.check()
        payload = json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self.stores.for_location(location).publish_bytes(location, payload)
        if cancellation is not None:
            cancellation.check()
        return manifest

    def read_partition(
        self,
        manifests: list[ShuffleManifest],
        partition: int,
        cancellation: CancellationCheck | None = None,
    ) -> tuple[pa.Table, ShuffleMetrics]:
        started = perf_counter()
        tables: list[pa.Table] = []
        bytes_read = 0
        rows_read = 0
        for manifest in manifests:
            if cancellation is not None:
                cancellation.check()
            matches = [item for item in manifest.files if item.partition == partition]
            if len(matches) != 1:
                raise ValueError(
                    f"Shuffle manifest {manifest.attempt_id!r} does not contain "
                    f"exactly one file for partition {partition}."
                )
            item = matches[0]
            payload = self.stores.for_location(item.location).read_bytes(item.location)
            if len(payload) != item.size_bytes:
                raise ValueError(f"Shuffle size mismatch for {item.location!r}")
            if hashlib.sha256(payload).hexdigest() != item.checksum:
                raise ValueError(f"Shuffle checksum mismatch for {item.location!r}")
            table = pq.read_table(pa.BufferReader(payload))
            if table.num_rows != item.row_count:
                raise ValueError(f"Shuffle row count mismatch for {item.location!r}")
            tables.append(table)
            bytes_read += len(payload)
            rows_read += table.num_rows
        combined = pa.concat_tables(tables) if tables else pa.table({})
        return combined, ShuffleMetrics(
            records_read=rows_read,
            bytes_read=bytes_read,
            partition_count=1,
            read_seconds=perf_counter() - started,
        )

    def load_manifest(
        self,
        query_id: str,
        stage_id: str,
        task_id: str,
        attempt_id: str,
    ) -> ShuffleManifest:
        location = self.manifest_location(query_id, stage_id, task_id, attempt_id)
        payload = self.stores.for_location(location).read_bytes(location)
        return ShuffleManifest.model_validate_json(payload)

    def data_location(
        self,
        query_id: str,
        stage_id: str,
        task_id: str,
        attempt_id: str,
        partition: int,
    ) -> str:
        return _join(
            self.root,
            query_id,
            stage_id,
            task_id,
            attempt_id,
            f"part-{partition:05d}.parquet",
        )

    def manifest_location(
        self,
        query_id: str,
        stage_id: str,
        task_id: str,
        attempt_id: str,
    ) -> str:
        return _join(self.root, query_id, stage_id, task_id, attempt_id, "manifest.json")


def _partition(
    row: dict[str, Any],
    keys: tuple[Expression, ...],
    partition_count: int,
) -> int:
    values = [expression.evaluate(cast(dict[str, SQLValue], row)) for expression in keys]
    encoded = json.dumps(
        [_stable_json(value) for value in values],
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return int.from_bytes(hashlib.sha256(encoded).digest()[:8], "big") % partition_count


def _stable_json(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def _join(base: str, *parts: str) -> str:
    parsed = urlsplit(base)
    safe_parts = tuple(_safe_segment(part) for part in parts)
    relative = PurePosixPath(*safe_parts).as_posix()
    if parsed.scheme in {"file", "s3"}:
        path = f"{parsed.path.rstrip('/')}/{relative}"
        return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))
    return str(Path(base).joinpath(*safe_parts))


def _safe_segment(value: str) -> str:
    """Bound path components while retaining a readable, collision-safe prefix."""

    if len(value) <= 16:
        return value
    digest = hashlib.sha256(value.encode()).hexdigest()[:12]
    return f"{value[:3]}-{digest}"

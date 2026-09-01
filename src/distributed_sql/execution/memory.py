"""Hierarchical memory accounting and task-scoped spill file management."""

from __future__ import annotations

import errno
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode

DEFAULT_MEMORY_LIMIT_BYTES = 64 * 1024 * 1024


class MemoryLimitExceeded(RuntimeError):
    """Raised when a reservation would exceed a query or task budget."""


class MemoryAccount:
    """A thread-safe memory account which can be nested under a query account."""

    def __init__(
        self,
        name: str,
        limit_bytes: int = DEFAULT_MEMORY_LIMIT_BYTES,
        *,
        parent: MemoryAccount | None = None,
        lock: RLock | None = None,
    ) -> None:
        if limit_bytes <= 0:
            raise ValueError("memory limit must be positive")
        self.name = name
        self.limit_bytes = limit_bytes
        self.parent = parent
        self.current_bytes = 0
        self.peak_bytes = 0
        self._lock: RLock = lock or (
            parent._lock if parent is not None else RLock()
        )

    def child(self, name: str, limit_bytes: int | None = None) -> MemoryAccount:
        return MemoryAccount(
            name,
            self.limit_bytes if limit_bytes is None else limit_bytes,
            parent=self,
            lock=self._lock,
        )

    def reserve(self, size_bytes: int) -> None:
        if size_bytes < 0:
            raise ValueError("reservation size cannot be negative")
        if size_bytes == 0:
            return
        with self._lock:
            chain = self._chain()
            exceeded = next(
                (
                    account
                    for account in chain
                    if account.current_bytes + size_bytes > account.limit_bytes
                ),
                None,
            )
            if exceeded is not None:
                raise MemoryLimitExceeded(
                    f"Memory account {exceeded.name!r} would exceed {exceeded.limit_bytes} bytes."
                )
            for account in chain:
                account.current_bytes += size_bytes
                account.peak_bytes = max(account.peak_bytes, account.current_bytes)

    def try_reserve(self, size_bytes: int) -> bool:
        try:
            self.reserve(size_bytes)
        except MemoryLimitExceeded:
            return False
        return True

    def release(self, size_bytes: int) -> None:
        if size_bytes < 0:
            raise ValueError("release size cannot be negative")
        if size_bytes == 0:
            return
        with self._lock:
            if size_bytes > self.current_bytes:
                raise ValueError(
                    f"Cannot release {size_bytes} bytes from account {self.name!r} "
                    f"with {self.current_bytes} bytes reserved."
                )
            for account in self._chain():
                account.current_bytes -= size_bytes

    def _chain(self) -> list[MemoryAccount]:
        chain = [self]
        account = self.parent
        while account is not None:
            chain.append(account)
            account = account.parent
        return chain


@dataclass(slots=True)
class SpillMetrics:
    spill_bytes: int = 0
    spill_files: int = 0
    spill_count: int = 0
    peak_memory_bytes: int = 0
    external_sort_runs: int = 0
    hash_partitions: int = 0
    sort_merge_fallbacks: int = 0
    sort_aggregate_runs: int = 0

    def add(self, other: SpillMetrics) -> SpillMetrics:
        return SpillMetrics(
            spill_bytes=self.spill_bytes + other.spill_bytes,
            spill_files=self.spill_files + other.spill_files,
            spill_count=self.spill_count + other.spill_count,
            peak_memory_bytes=max(self.peak_memory_bytes, other.peak_memory_bytes),
            external_sort_runs=self.external_sort_runs + other.external_sort_runs,
            hash_partitions=self.hash_partitions + other.hash_partitions,
            sort_merge_fallbacks=(self.sort_merge_fallbacks + other.sort_merge_fallbacks),
            sort_aggregate_runs=self.sort_aggregate_runs + other.sort_aggregate_runs,
        )


class TempFileManager:
    """Owns one attempt directory and removes it, including empty parents."""

    def __init__(self, root: Path, query_id: str, task_id: str) -> None:
        self.root = Path(root)
        self.query_id = _safe_name(query_id)
        self.task_id = _safe_name(task_id)
        self.directory = self.root / self.query_id / self.task_id / f"attempt-{uuid4().hex}"
        self._files: list[Path] = []

    def write_table(self, table: pa.Table, prefix: str) -> Path:
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            path = self.directory / f"{_safe_name(prefix)}-{len(self._files):05d}.parquet"
            pq.write_table(table, path)
            self._files.append(path)
            return path
        except OSError as exc:
            self.cleanup()
            message = (
                "Temporary spill storage is full."
                if exc.errno == errno.ENOSPC
                else f"Temporary spill write failed: {exc}"
            )
            raise DistributedSQLError(
                ErrorCode.RESOURCE_EXHAUSTED,
                message,
                status_code=507,
                context={
                    "query_id": self.query_id,
                    "task_id": self.task_id,
                    "temp_root": str(self.root),
                },
            ) from exc

    def cleanup(self) -> None:
        shutil.rmtree(self.directory, ignore_errors=True)
        for parent in (self.directory.parent, self.directory.parent.parent):
            try:
                parent.rmdir()
            except OSError:
                pass


def default_temp_root() -> Path:
    return Path(tempfile.gettempdir()) / "distributed-sql"


def estimate_row_size(row: dict[str, object]) -> int:
    """Conservative, deterministic charge for Python row materialization."""

    return max(64, len(repr(row).encode("utf-8")) + 64)


def _safe_name(value: str) -> str:
    cleaned = "".join(
        character if character.isalnum() or character in "-_" else "_" for character in value
    )
    return cleaned[:80] or "unknown"

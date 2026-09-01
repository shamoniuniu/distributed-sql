"""Remote Task execution and immutable result publication."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os
import pickle
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from distributed_sql.catalog.models import CatalogTable, TableFormat
from distributed_sql.catalog.storage import LocalObjectStore, ObjectStoreRouter, S3ObjectStore
from distributed_sql.common.config import WorkerSettings
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode, public_task_error
from distributed_sql.common.protocol import (
    ArtifactReference,
    AttemptState,
    DataType,
    RemoteTaskMetrics,
    RemoteTaskOperation,
    RemoteTaskResult,
    RemoteTaskStatus,
    RemoteTaskSubmission,
    SerializedPlan,
)
from distributed_sql.data_source import FileScanTask, ScanRequest, create_data_source_registry
from distributed_sql.execution import (
    BatchOperator,
    CancellationToken,
    ExecutionContext,
    FilterOperator,
    GroupingSetsOperator,
    HashAggregateOperator,
    HashJoinOperator,
    InMemorySorter,
    LimitOperator,
    LocalExecutor,
    OrderOperator,
    ProjectOperator,
    RecordBatchSource,
    ShuffleManifest,
    ShuffleStore,
    SortAggregateOperator,
    WindowOperator,
)
from distributed_sql.execution.distributed import _operator_table
from distributed_sql.execution.engine import _hash_join_keys, _runtime_names
from distributed_sql.execution.memory import SpillMetrics
from distributed_sql.execution.operators import (
    arrow_schema_for_aggregate,
    arrow_schema_for_grouping_sets,
    arrow_schema_for_window,
)
from distributed_sql.optimizer import CostBasedOptimizer
from distributed_sql.planner import Binder
from distributed_sql.planner.expressions import Column
from distributed_sql.planner.logical import (
    Aggregate,
    Filter,
    GroupingSets,
    Join,
    Limit,
    Order,
    Project,
    Scan,
    Window,
)
from distributed_sql.planner.types import TypeInfo


@dataclass(slots=True)
class _TaskRecord:
    submission: RemoteTaskSubmission
    status: RemoteTaskStatus
    cancellation: CancellationToken
    future: asyncio.Task[None]


class WorkerTaskManager:
    """Own Task attempts for one Worker process."""

    def __init__(
        self,
        settings: WorkerSettings,
        worker_id: str,
        stores: ObjectStoreRouter | None = None,
    ) -> None:
        self._settings = settings
        self._worker_id = worker_id
        if stores is None:
            s3_store = None
            if (
                settings.object_store_access_key is not None
                and settings.object_store_secret_key is not None
            ):
                s3_store = S3ObjectStore(
                    access_key=settings.object_store_access_key,
                    secret_key=settings.object_store_secret_key,
                    endpoint=settings.object_store_endpoint,
                    region=settings.object_store_region,
                    secure=settings.object_store_secure,
                )
            stores = ObjectStoreRouter(LocalObjectStore(), s3_store)
        self._stores = stores
        self._records: dict[str, _TaskRecord] = {}
        self._lock = asyncio.Lock()

    async def submit(self, submission: RemoteTaskSubmission) -> RemoteTaskStatus:
        async with self._lock:
            existing = self._records.get(submission.attempt_id)
            if existing is not None:
                if existing.submission != submission:
                    raise ValueError(
                        f"Attempt {submission.attempt_id!r} was submitted with different content"
                    )
                return existing.status.model_copy(deep=True)
            status = RemoteTaskStatus(
                task_id=submission.task_id,
                attempt_id=submission.attempt_id,
                state=AttemptState.CREATED,
            )
            cancellation = CancellationToken()
            placeholder = asyncio.create_task(asyncio.sleep(0))
            record = _TaskRecord(submission, status, cancellation, placeholder)
            record.future = asyncio.create_task(
                self._execute(record),
                name=f"worker-task-{submission.attempt_id}",
            )
            self._records[submission.attempt_id] = record
            return status.model_copy(deep=True)

    async def status(self, attempt_id: str) -> RemoteTaskStatus | None:
        async with self._lock:
            record = self._records.get(attempt_id)
            return record.status.model_copy(deep=True) if record else None

    async def list_statuses(self) -> list[RemoteTaskStatus]:
        async with self._lock:
            return [
                record.status.model_copy(deep=True)
                for record in sorted(
                    self._records.values(),
                    key=lambda item: item.submission.attempt_id,
                )
            ]

    async def cancel(self, attempt_id: str) -> RemoteTaskStatus | None:
        async with self._lock:
            record = self._records.get(attempt_id)
            if record is None:
                return None
            if record.status.state in {
                AttemptState.CREATED,
                AttemptState.RUNNING,
            }:
                record.cancellation.cancel()
                future = record.future
            else:
                return record.status.model_copy(deep=True)
        try:
            await asyncio.wait_for(
                asyncio.shield(future),
                timeout=self._settings.cancellation_timeout_seconds,
            )
        except TimeoutError as exc:
            raise DistributedSQLError(
                ErrorCode.TASK_FAILED,
                "Worker Task cancellation was not confirmed before the timeout.",
                status_code=504,
                context={
                    "query_id": record.submission.query_id,
                    "stage_id": record.submission.stage_id,
                    "task_id": record.submission.task_id,
                    "attempt_id": attempt_id,
                    "worker_id": self._worker_id,
                },
            ) from exc
        async with self._lock:
            return record.status.model_copy(deep=True)

    async def close(self) -> None:
        async with self._lock:
            records = list(self._records.values())
        for record in records:
            record.cancellation.cancel()
        if records:
            await asyncio.wait_for(
                asyncio.gather(
                    *(record.future for record in records),
                    return_exceptions=True,
                ),
                timeout=self._settings.cancellation_timeout_seconds,
            )

    async def _execute(self, record: _TaskRecord) -> None:
        record.status.state = AttemptState.RUNNING
        record.status.started_at = datetime.now(UTC)
        submission = record.submission
        output_location = self._output_location(submission)
        try:
            result = await asyncio.to_thread(
                self._execute_sync,
                submission,
                record.cancellation,
            )
            if record.cancellation.cancelled:
                self._delete_result(result)
                self._stores.for_location(output_location).delete(output_location)
                record.status.state = AttemptState.CANCELED
                return
            record.status.result = result
            record.status.state = AttemptState.SUCCEEDED
        except asyncio.CancelledError:
            self._cleanup_submission(submission)
            self._stores.for_location(output_location).delete(output_location)
            record.status.state = AttemptState.CANCELED
            raise
        except Exception as exc:
            self._cleanup_submission(submission)
            self._stores.for_location(output_location).delete(output_location)
            record.status.error = public_task_error(
                exc,
                context={
                    "query_id": submission.query_id,
                    "stage_id": submission.stage_id,
                    "task_id": submission.task_id,
                    "attempt_id": submission.attempt_id,
                    "worker_id": self._worker_id,
                },
            )
            record.status.state = (
                AttemptState.CANCELED if record.cancellation.cancelled else AttemptState.FAILED
            )

    def _execute_sync(
        self,
        submission: RemoteTaskSubmission,
        cancellation: CancellationToken,
    ) -> RemoteTaskResult:
        started = perf_counter()
        input_rows = 0
        input_bytes = 0
        shuffle_records_written = 0
        shuffle_bytes_written = 0
        shuffle_write_seconds = 0.0
        shuffle_records_read = 0
        shuffle_bytes_read = 0
        shuffle_read_seconds = 0.0
        spill = SpillMetrics()
        self._cooperative_delay(cancellation)
        if submission.operation is RemoteTaskOperation.SLEEP:
            import time

            raw_duration = submission.payload.get("seconds", 0)
            if not isinstance(raw_duration, int | float | str):
                raise ValueError("sleep seconds must be numeric")
            duration = float(raw_duration)
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline:
                cancellation.check()
                time.sleep(min(0.01, deadline - time.monotonic()))
            table = pa.table(
                {
                    "worker_id": [self._worker_id],
                    "process_id": [os.getpid()],
                }
            )
        elif submission.operation is RemoteTaskOperation.SHUFFLE_WRITE:
            source_location = self._string_payload(submission, "source_location")
            shuffle_root = self._string_payload(submission, "shuffle_root")
            raw_partition_count = submission.payload.get("partition_count", 1)
            if not isinstance(raw_partition_count, int):
                raise ValueError("partition_count must be an integer")
            raw_keys = submission.payload.get("keys", [])
            if not isinstance(raw_keys, list) or not all(
                isinstance(item, str) for item in raw_keys
            ):
                raise ValueError("keys must be a list of column names")
            columns = []
            for qualified_name in cast(list[str], raw_keys):
                source, separator, name = qualified_name.rpartition(".")
                columns.append(
                    Column(
                        name if separator else qualified_name,
                        source if separator else "",
                        TypeInfo(DataType.NULL),
                    )
                )
            table, input_bytes = self._read_parquet_with_size(source_location)
            input_rows = table.num_rows
            manifest = ShuffleStore(shuffle_root, self._stores).write(
                query_id=submission.query_id,
                stage_id=submission.stage_id,
                task_id=submission.task_id,
                attempt_id=submission.attempt_id,
                table=table,
                partition_count=raw_partition_count,
                keys=tuple(columns),
                broadcast=bool(submission.payload.get("broadcast", False)),
                cancellation=cancellation,
            )
            shuffle_records_written = manifest.metrics.records_written
            shuffle_bytes_written = manifest.metrics.bytes_written
            shuffle_write_seconds = manifest.metrics.write_seconds
            return RemoteTaskResult(
                task_id=submission.task_id,
                attempt_id=submission.attempt_id,
                worker_id=self._worker_id,
                worker_process_id=os.getpid(),
                shuffle_manifests=[manifest.model_dump(mode="json")],
                metrics=RemoteTaskMetrics(
                    input_rows=input_rows,
                    input_bytes=input_bytes,
                    output_rows=manifest.metrics.records_written,
                    output_bytes=manifest.metrics.bytes_written,
                    duration_seconds=perf_counter() - started,
                    shuffle_records_written=shuffle_records_written,
                    shuffle_bytes_written=shuffle_bytes_written,
                    shuffle_write_seconds=shuffle_write_seconds,
                ).model_dump(mode="json"),
            )
        elif submission.operation is RemoteTaskOperation.SHUFFLE_READ:
            raw_manifests = submission.payload.get("manifests")
            raw_partition = submission.payload.get("partition")
            shuffle_root = self._string_payload(submission, "shuffle_root")
            if not isinstance(raw_manifests, list) or not isinstance(raw_partition, int):
                raise ValueError("shuffle read requires manifests and integer partition")
            manifests = [ShuffleManifest.model_validate(item) for item in raw_manifests]
            table, metrics = ShuffleStore(shuffle_root, self._stores).read_partition(
                manifests,
                raw_partition,
                cancellation=cancellation,
            )
            input_rows = metrics.records_read
            input_bytes = metrics.bytes_read
            shuffle_records_read = metrics.records_read
            shuffle_bytes_read = metrics.bytes_read
            shuffle_read_seconds = metrics.read_seconds
        elif submission.operation is RemoteTaskOperation.SCAN:
            table = self._execute_scan(submission, cancellation)
            input_rows = table.num_rows
            input_bytes = self._scan_input_bytes(submission)
        elif submission.operation is RemoteTaskOperation.UNARY:
            table, spill, input_rows, input_bytes = self._execute_unary(
                submission,
                cancellation,
            )
        elif submission.operation is RemoteTaskOperation.JOIN:
            table, spill, input_rows, input_bytes = self._execute_join(
                submission,
                cancellation,
            )
        else:
            sql = cast(str, submission.payload["sql"])
            raw_tables = cast(list[dict[str, Any]], submission.payload["tables"])
            tables = {
                f"{table.namespace}.{table.name}": table
                for item in raw_tables
                for table in [CatalogTable.model_validate(item)]
            }
            logical = Binder(tables).bind(sql)
            optimized = CostBasedOptimizer(tables).optimize(logical).optimized_plan
            context = ExecutionContext(
                cancellation=cancellation,
                memory_limit_bytes=self._settings.memory_limit_bytes,
                temp_root=self._settings.temp_directory,
                query_id=submission.query_id,
                task_id=submission.attempt_id,
            )
            try:
                table = LocalExecutor(
                    tables,
                    create_data_source_registry(self._stores),
                ).execute_table(optimized, context)
                spill = context.spill_metrics
            finally:
                context.close()
        cancellation.check()
        output_location = self._output_location(submission)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        payload = sink.getvalue().to_pybytes()
        cancellation.check()
        self._stores.for_location(output_location).publish_bytes(
            output_location,
            payload,
        )
        cancellation.check()
        artifact = ArtifactReference(
            location=output_location,
            media_type="application/vnd.apache.parquet",
            size_bytes=len(payload),
            checksum=hashlib.sha256(payload).hexdigest(),
            row_count=table.num_rows,
        )
        result_metrics = RemoteTaskMetrics(
            input_rows=input_rows,
            input_bytes=input_bytes,
            output_rows=artifact.row_count or 0,
            output_bytes=artifact.size_bytes,
            duration_seconds=perf_counter() - started,
            shuffle_records_written=shuffle_records_written,
            shuffle_bytes_written=shuffle_bytes_written,
            shuffle_write_seconds=shuffle_write_seconds,
            shuffle_records_read=shuffle_records_read,
            shuffle_bytes_read=shuffle_bytes_read,
            shuffle_read_seconds=shuffle_read_seconds,
            spill_bytes=spill.spill_bytes,
            spill_files=spill.spill_files,
            spill_count=spill.spill_count,
            peak_memory_bytes=spill.peak_memory_bytes,
            external_sort_runs=spill.external_sort_runs,
            hash_partitions=spill.hash_partitions,
            sort_merge_fallbacks=spill.sort_merge_fallbacks,
            sort_aggregate_runs=spill.sort_aggregate_runs,
        )
        return RemoteTaskResult(
            task_id=submission.task_id,
            attempt_id=submission.attempt_id,
            worker_id=self._worker_id,
            worker_process_id=os.getpid(),
            artifact=artifact,
            metrics=result_metrics.model_dump(mode="json"),
        )

    def _cooperative_delay(self, cancellation: CancellationToken) -> None:
        duration = self._settings.task_start_delay_seconds
        if duration <= 0:
            return
        import time

        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            cancellation.check()
            time.sleep(min(0.01, deadline - time.monotonic()))

    def _execute_scan(
        self,
        submission: RemoteTaskSubmission,
        cancellation: CancellationToken,
    ) -> pa.Table:
        plan = self._plan_payload(submission)
        if not isinstance(plan, Scan):
            raise ValueError("scan Task requires a Scan plan")
        raw_table = submission.payload.get("table")
        raw_file_task = submission.payload.get("file_task")
        if not isinstance(raw_table, dict) or not isinstance(raw_file_task, dict):
            raise ValueError("scan Task requires table and file_task")
        table = CatalogTable.model_validate(raw_table)
        location = raw_file_task.get("location")
        file_format = raw_file_task.get("format")
        if not isinstance(location, str) or not isinstance(file_format, str):
            raise ValueError("file_task requires location and format")
        file_task = FileScanTask(
            location=location,
            format=TableFormat(file_format),
            start=_optional_int(raw_file_task.get("start"), default=0) or 0,
            length=_optional_int(raw_file_task.get("length")),
            record_count=_optional_int(raw_file_task.get("record_count")),
            partition_values=cast(
                dict[str, Any],
                raw_file_task.get("partition_values", {}),
            ),
            delete_files=tuple(cast(list[str], raw_file_task.get("delete_files", []))),
        )
        source = create_data_source_registry(self._stores).for_table(table)
        request = ScanRequest(
            projection=tuple(field.name for field in plan.output_schema.fields),
            file_tasks=(file_task,),
        )
        batches = []
        for batch in source.scan(table, request):
            cancellation.check()
            batches.append(
                batch.rename_columns([f"{plan.alias}.{name}" for name in batch.schema.names])
            )
        schema = source.plan_scan(table, request).schema
        qualified = pa.schema(
            [
                pa.field(
                    f"{plan.alias}.{field.name}",
                    arrow_field.type,
                    nullable=field.nullable,
                )
                for field, arrow_field in zip(
                    plan.output_schema.fields,
                    schema,
                    strict=True,
                )
            ]
        )
        return (
            pa.Table.from_batches(batches).cast(qualified)
            if batches
            else pa.Table.from_batches([], schema=qualified)
        )

    def _execute_unary(
        self,
        submission: RemoteTaskSubmission,
        cancellation: CancellationToken,
    ) -> tuple[pa.Table, SpillMetrics, int, int]:
        plan = self._plan_payload(submission)
        if not isinstance(
            plan,
            Project | Filter | Limit | Order | Aggregate | GroupingSets | Window,
        ):
            raise ValueError("unary Task requires a unary logical plan")
        input_table, input_bytes = self._read_parquet_with_size(
            self._string_payload(submission, "input_location")
        )
        context = self._context(submission, cancellation)
        source = RecordBatchSource(
            f"{plan.node_id}-input",
            input_table.to_batches(max_chunksize=context.batch_size),
        )
        operator: BatchOperator
        if isinstance(plan, Project):
            from distributed_sql.data_source import schema_to_arrow

            operator = ProjectOperator(
                plan.node_id,
                source,
                plan.expressions,
                schema_to_arrow(plan.output_schema),
            )
        elif isinstance(plan, Filter):
            operator = FilterOperator(plan.node_id, source, plan.predicate)
        elif isinstance(plan, Limit):
            operator = LimitOperator(plan.node_id, source, plan.count)
        elif isinstance(plan, Order):
            operator = OrderOperator(
                plan.node_id,
                source,
                plan.order_by,
                input_table.schema,
                InMemorySorter(),
            )
        elif isinstance(plan, Aggregate):
            if plan.group_by:
                operator = SortAggregateOperator(
                    plan.node_id,
                    source,
                    plan.group_by,
                    plan.aggregates,
                    input_table.schema,
                    arrow_schema_for_aggregate(plan.group_by, plan.aggregates),
                )
            else:
                operator = HashAggregateOperator(
                    plan.node_id,
                    source,
                    plan.group_by,
                    plan.aggregates,
                    arrow_schema_for_aggregate(plan.group_by, plan.aggregates),
                )
        elif isinstance(plan, GroupingSets):
            operator = GroupingSetsOperator(
                plan.node_id,
                source,
                plan.grouping_sets,
                plan.aggregates,
                arrow_schema_for_grouping_sets(plan.grouping_sets, plan.aggregates),
            )
        elif isinstance(plan, Window):
            operator = WindowOperator(
                plan.node_id,
                source,
                plan.expressions,
                arrow_schema_for_window(input_table.schema, plan.expressions),
                InMemorySorter(),
            )
        else:
            raise ValueError(f"Unsupported unary plan {type(plan).__name__}")
        try:
            table = _operator_table(operator, context, plan.output_schema)
            return table, context.spill_metrics, input_table.num_rows, input_bytes
        finally:
            context.close()

    def _execute_join(
        self,
        submission: RemoteTaskSubmission,
        cancellation: CancellationToken,
    ) -> tuple[pa.Table, SpillMetrics, int, int]:
        from distributed_sql.data_source import schema_to_arrow

        plan = self._plan_payload(submission)
        if not isinstance(plan, Join):
            raise ValueError("join Task requires a Join plan")
        left, left_bytes = self._read_parquet_with_size(
            self._string_payload(submission, "left_location")
        )
        right, right_bytes = self._read_parquet_with_size(
            self._string_payload(submission, "right_location")
        )
        left_keys, right_keys = _hash_join_keys(plan)
        context = self._context(submission, cancellation)
        operator = HashJoinOperator(
            plan.node_id,
            RecordBatchSource("left-input", left.to_batches()),
            RecordBatchSource("right-input", right.to_batches()),
            left_keys,
            right_keys,
            plan.join_type,
            schema_to_arrow(plan.output_schema),
            _runtime_names(plan.left),
            _runtime_names(plan.right),
            build_side=plan.build_side,
        )
        try:
            table = _operator_table(operator, context, plan.output_schema)
            return (
                table,
                context.spill_metrics,
                left.num_rows + right.num_rows,
                left_bytes + right_bytes,
            )
        finally:
            context.close()

    def _context(
        self,
        submission: RemoteTaskSubmission,
        cancellation: CancellationToken,
    ) -> ExecutionContext:
        return ExecutionContext(
            cancellation=cancellation,
            memory_limit_bytes=self._settings.memory_limit_bytes,
            temp_root=self._settings.temp_directory,
            query_id=submission.query_id,
            task_id=submission.attempt_id,
        )

    @staticmethod
    def _plan_payload(submission: RemoteTaskSubmission) -> object:
        fragment = SerializedPlan.model_validate(submission.payload.get("plan"))
        return pickle.loads(base64.b64decode(fragment.payload, validate=True))

    def result_bytes(self, status: RemoteTaskStatus) -> bytes:
        if status.result is None or status.result.artifact is None:
            raise ValueError("Task result is not available")
        location = status.result.artifact.location
        return self._stores.for_location(location).read_bytes(location)

    def _read_parquet(self, location: str) -> pa.Table:
        return self._read_parquet_with_size(location)[0]

    def _read_parquet_with_size(self, location: str) -> tuple[pa.Table, int]:
        payload = self._stores.for_location(location).read_bytes(location)
        return pq.read_table(pa.BufferReader(payload)), len(payload)

    def _scan_input_bytes(self, submission: RemoteTaskSubmission) -> int:
        raw_file_task = submission.payload.get("file_task")
        if not isinstance(raw_file_task, dict):
            raise ValueError("scan Task requires file_task")
        length = raw_file_task.get("length")
        if isinstance(length, int):
            return length
        location = raw_file_task.get("location")
        if not isinstance(location, str):
            raise ValueError("file_task requires location")
        return len(self._stores.for_location(location).read_bytes(location))

    def _output_location(self, submission: RemoteTaskSubmission) -> str:
        if submission.output_location.startswith("worker:///"):
            relative = submission.output_location.removeprefix("worker:///")
            return str((self._settings.temp_directory / "results" / relative).resolve())
        return submission.output_location

    def _delete_result(self, result: RemoteTaskResult) -> None:
        if result.artifact is not None:
            self._stores.for_location(result.artifact.location).delete(result.artifact.location)
        for raw_manifest in result.shuffle_manifests:
            manifest = ShuffleManifest.model_validate(raw_manifest)
            for item in manifest.files:
                self._stores.for_location(item.location).delete(item.location)
            first_location = manifest.files[0].location
            location = (
                str(Path(first_location).with_name("manifest.json"))
                if "://" not in first_location
                else f"{first_location.rsplit('/', 1)[0]}/manifest.json"
            )
            self._stores.for_location(location).delete(location)

    def _cleanup_submission(self, submission: RemoteTaskSubmission) -> None:
        if submission.operation is not RemoteTaskOperation.SHUFFLE_WRITE:
            return
        shuffle_root = submission.payload.get("shuffle_root")
        partition_count = submission.payload.get("partition_count")
        if not isinstance(shuffle_root, str) or not isinstance(partition_count, int):
            return
        shuffle = ShuffleStore(shuffle_root, self._stores)
        for partition in range(partition_count):
            location = shuffle.data_location(
                submission.query_id,
                submission.stage_id,
                submission.task_id,
                submission.attempt_id,
                partition,
            )
            self._stores.for_location(location).delete(location)
        location = shuffle.manifest_location(
            submission.query_id,
            submission.stage_id,
            submission.task_id,
            submission.attempt_id,
        )
        self._stores.for_location(location).delete(location)

    @staticmethod
    def _string_payload(submission: RemoteTaskSubmission, name: str) -> str:
        value = submission.payload.get(name)
        if not isinstance(value, str):
            raise ValueError(f"{name} must be a string")
        return value


def _optional_int(value: object, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    if not isinstance(value, int):
        raise ValueError("Expected an integer")
    return value

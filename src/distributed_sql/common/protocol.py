"""Versioned JSON protocols shared by the Coordinator and Workers."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from distributed_sql.common.exceptions import ErrorDetail


def utc_now() -> datetime:
    return datetime.now(UTC)


class ProtocolModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False)


class DataType(StrEnum):
    NULL = "null"
    BOOLEAN = "boolean"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    DECIMAL = "decimal"
    STRING = "string"
    BINARY = "binary"
    DATE = "date"
    TIMESTAMP = "timestamp"
    LIST = "list"
    STRUCT = "struct"


class SchemaField(ProtocolModel):
    name: str
    data_type: DataType
    nullable: bool = True
    metadata: dict[str, str] = Field(default_factory=dict)
    children: list["SchemaField"] = Field(default_factory=list)


class Schema(ProtocolModel):
    fields: list[SchemaField]
    metadata: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def field_names_are_unique(self) -> Self:
        names = [field.name for field in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("schema field names must be unique")
        return self


class PartitionStrategy(StrEnum):
    SINGLE = "single"
    ROUND_ROBIN = "round_robin"
    HASH = "hash"
    BROADCAST = "broadcast"
    UNKNOWN = "unknown"


class Partition(ProtocolModel):
    partition_id: str
    ordinal: int = Field(ge=0)
    location: str
    strategy: PartitionStrategy = PartitionStrategy.UNKNOWN
    keys: list[str] = Field(default_factory=list)
    size_bytes: int | None = Field(default=None, ge=0)
    row_count: int | None = Field(default=None, ge=0)
    checksum: str | None = None


class ColumnStatistics(ProtocolModel):
    column_name: str
    null_count: int | None = Field(default=None, ge=0)
    distinct_count: int | None = Field(default=None, ge=0)
    min_value: JsonValue | None = None
    max_value: JsonValue | None = None
    average_size_bytes: float | None = Field(default=None, ge=0)


class Statistics(ProtocolModel):
    row_count: int | None = Field(default=None, ge=0)
    size_bytes: int | None = Field(default=None, ge=0)
    columns: dict[str, ColumnStatistics] = Field(default_factory=dict)
    collected_at: datetime | None = None
    source: str = "unknown"


class PlanNodeType(StrEnum):
    SCAN = "scan"
    PROJECT = "project"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    LIMIT = "limit"
    ORDER = "order"
    WINDOW = "window"
    GROUPING_SETS = "grouping_sets"
    EXCHANGE = "exchange"
    OUTPUT = "output"


class PlanNode(ProtocolModel):
    node_id: str
    node_type: PlanNodeType
    children: list["PlanNode"] = Field(default_factory=list)
    output_schema: Schema | None = None
    properties: dict[str, JsonValue] = Field(default_factory=dict)
    statistics: Statistics | None = None


class QueryState(StrEnum):
    QUEUED = "queued"
    PLANNING = "planning"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class Query(ProtocolModel):
    query_id: str = Field(default_factory=lambda: str(uuid4()))
    sql: str
    state: QueryState = QueryState.QUEUED
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    logical_plan: PlanNode | None = None
    physical_plan: PlanNode | None = None
    error: dict[str, JsonValue] | None = None


class StageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class Stage(ProtocolModel):
    stage_id: str
    query_id: str
    state: StageState = StageState.PENDING
    plan: PlanNode
    dependency_stage_ids: list[str] = Field(default_factory=list)
    partition_count: int = Field(default=1, ge=1)


class TaskState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class Task(ProtocolModel):
    task_id: str
    query_id: str
    stage_id: str
    partition: Partition
    state: TaskState = TaskState.PENDING
    attempt_ids: list[str] = Field(default_factory=list)


class AttemptState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    LOST = "lost"


class Attempt(ProtocolModel):
    attempt_id: str
    task_id: str
    attempt_number: int = Field(ge=0)
    worker_id: str | None = None
    state: AttemptState = AttemptState.CREATED
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: dict[str, JsonValue] | None = None


class WorkerState(StrEnum):
    REGISTERING = "registering"
    ACTIVE = "active"
    DRAINING = "draining"
    LOST = "lost"


class Worker(ProtocolModel):
    worker_id: str
    endpoint: str
    state: WorkerState = WorkerState.REGISTERING
    slots: int = Field(default=1, ge=1)
    available_slots: int = Field(default=1, ge=0)
    memory_limit_bytes: int = Field(gt=0)
    lease_id: str | None = None
    lease_expires_at: datetime | None = None
    registered_at: datetime = Field(default_factory=utc_now)
    last_heartbeat_at: datetime | None = None
    labels: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def available_slots_do_not_exceed_capacity(self) -> Self:
        if self.available_slots > self.slots:
            raise ValueError("available Worker slots cannot exceed configured slots")
        return self


class HealthStatus(StrEnum):
    STARTING = "starting"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STOPPING = "stopping"


class HealthResponse(ProtocolModel):
    service: str
    status: HealthStatus
    version: str
    process_id: int
    dependencies: dict[str, str] = Field(default_factory=dict)


class WorkerRegistration(ProtocolModel):
    worker_id: str
    endpoint: str
    slots: int = Field(default=1, ge=1)
    memory_limit_bytes: int = Field(gt=0)
    labels: dict[str, str] = Field(default_factory=dict)


class WorkerRegistrationResponse(ProtocolModel):
    worker: Worker
    lease_ttl_seconds: float = Field(gt=0)


class WorkerHeartbeat(ProtocolModel):
    lease_id: str
    available_slots: int = Field(ge=0)
    state: WorkerState = WorkerState.ACTIVE
    metrics: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def state_is_heartbeat_eligible(self) -> Self:
        if self.state not in {WorkerState.ACTIVE, WorkerState.DRAINING}:
            raise ValueError("heartbeat state must be active or draining")
        return self


class WorkerHeartbeatResponse(ProtocolModel):
    worker_id: str
    accepted: bool
    lease_expires_at: datetime


class WorkerListResponse(ProtocolModel):
    workers: list[Worker]


class RemoteTaskOperation(StrEnum):
    QUERY = "query"
    SCAN = "scan"
    UNARY = "unary"
    JOIN = "join"
    SHUFFLE_WRITE = "shuffle_write"
    SHUFFLE_READ = "shuffle_read"
    SLEEP = "sleep"


class SerializedPlan(ProtocolModel):
    version: Literal[1] = 1
    format: Literal["python-pickle-v5"] = "python-pickle-v5"
    payload: str = Field(min_length=1)


class QueryTaskPayload(ProtocolModel):
    sql: str = Field(min_length=1)
    tables: list[dict[str, JsonValue]]


class ScanFileTaskPayload(ProtocolModel):
    location: str = Field(min_length=1)
    format: str = Field(min_length=1)
    start: int = Field(default=0, ge=0)
    length: int | None = Field(default=None, ge=0)
    record_count: int | None = Field(default=None, ge=0)
    partition_values: dict[str, JsonValue] = Field(default_factory=dict)
    delete_files: list[str] = Field(default_factory=list)


class ScanTaskPayload(ProtocolModel):
    plan: SerializedPlan
    table: dict[str, JsonValue]
    file_task: ScanFileTaskPayload


class UnaryTaskPayload(ProtocolModel):
    plan: SerializedPlan
    input_location: str = Field(min_length=1)


class JoinTaskPayload(ProtocolModel):
    plan: SerializedPlan
    left_location: str = Field(min_length=1)
    right_location: str = Field(min_length=1)


class ShuffleWriteTaskPayload(ProtocolModel):
    source_location: str = Field(min_length=1)
    shuffle_root: str = Field(min_length=1)
    partition_count: int = Field(ge=1)
    keys: list[str] = Field(default_factory=list)
    broadcast: bool = False


class ShuffleReadTaskPayload(ProtocolModel):
    shuffle_root: str = Field(min_length=1)
    partition: int = Field(ge=0)
    manifests: list[dict[str, JsonValue]]


class SleepTaskPayload(ProtocolModel):
    seconds: float = Field(default=0.0, ge=0)


class ArtifactReference(ProtocolModel):
    location: str
    media_type: str
    size_bytes: int = Field(ge=0)
    checksum: str
    row_count: int | None = Field(default=None, ge=0)


class RemoteTaskMetrics(ProtocolModel):
    input_rows: int = Field(ge=0)
    input_bytes: int = Field(ge=0)
    output_rows: int = Field(ge=0)
    output_bytes: int = Field(ge=0)
    duration_seconds: float = Field(ge=0)
    shuffle_records_written: int = Field(default=0, ge=0)
    shuffle_bytes_written: int = Field(default=0, ge=0)
    shuffle_write_seconds: float = Field(default=0.0, ge=0)
    shuffle_records_read: int = Field(default=0, ge=0)
    shuffle_bytes_read: int = Field(default=0, ge=0)
    shuffle_read_seconds: float = Field(default=0.0, ge=0)
    spill_bytes: int = Field(default=0, ge=0)
    spill_files: int = Field(default=0, ge=0)
    spill_count: int = Field(default=0, ge=0)
    peak_memory_bytes: int = Field(default=0, ge=0)
    external_sort_runs: int = Field(default=0, ge=0)
    hash_partitions: int = Field(default=0, ge=0)
    sort_merge_fallbacks: int = Field(default=0, ge=0)
    sort_aggregate_runs: int = Field(default=0, ge=0)


class RemoteTaskSubmission(ProtocolModel):
    version: Literal[1] = 1
    task_id: str
    attempt_id: str
    query_id: str
    stage_id: str
    operation: RemoteTaskOperation
    payload: dict[str, JsonValue] = Field(default_factory=dict)
    output_location: str

    @model_validator(mode="after")
    def payload_matches_operation(self) -> Self:
        payload_types: dict[RemoteTaskOperation, type[ProtocolModel]] = {
            RemoteTaskOperation.QUERY: QueryTaskPayload,
            RemoteTaskOperation.SCAN: ScanTaskPayload,
            RemoteTaskOperation.UNARY: UnaryTaskPayload,
            RemoteTaskOperation.JOIN: JoinTaskPayload,
            RemoteTaskOperation.SHUFFLE_WRITE: ShuffleWriteTaskPayload,
            RemoteTaskOperation.SHUFFLE_READ: ShuffleReadTaskPayload,
            RemoteTaskOperation.SLEEP: SleepTaskPayload,
        }
        validated = payload_types[self.operation].model_validate(self.payload)
        self.payload = validated.model_dump(mode="json")
        return self


class RemoteTaskResult(ProtocolModel):
    version: Literal[1] = 1
    task_id: str
    attempt_id: str
    worker_id: str
    worker_process_id: int = Field(gt=0)
    artifact: ArtifactReference | None = None
    shuffle_manifests: list[dict[str, JsonValue]] = Field(default_factory=list)
    metrics: dict[str, JsonValue] = Field(default_factory=dict)


class RemoteTaskStatus(ProtocolModel):
    version: Literal[1] = 1
    task_id: str
    attempt_id: str
    state: AttemptState
    started_at: datetime | None = None
    result: RemoteTaskResult | None = None
    error: ErrorDetail | None = None


class RemoteTaskListResponse(ProtocolModel):
    tasks: list[RemoteTaskStatus]


ProtocolValue = dict[str, Any]

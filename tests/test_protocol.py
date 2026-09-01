from datetime import UTC, datetime

import pytest
from pydantic import BaseModel, ValidationError

from distributed_sql.common.protocol import (
    Attempt,
    ColumnStatistics,
    DataType,
    Partition,
    PartitionStrategy,
    PlanNode,
    PlanNodeType,
    Query,
    RemoteTaskOperation,
    RemoteTaskSubmission,
    Schema,
    SchemaField,
    Stage,
    Statistics,
    Task,
    Worker,
    WorkerHeartbeat,
    WorkerState,
)


def protocol_examples() -> list[BaseModel]:
    schema = Schema(
        fields=[
            SchemaField(name="order_id", data_type=DataType.INT64, nullable=False),
            SchemaField(name="amount", data_type=DataType.DECIMAL),
        ]
    )
    statistics = Statistics(
        row_count=10,
        size_bytes=320,
        columns={
            "order_id": ColumnStatistics(
                column_name="order_id",
                null_count=0,
                distinct_count=10,
                min_value=1,
                max_value=10,
            )
        },
        collected_at=datetime(2026, 8, 31, tzinfo=UTC),
        source="catalog",
    )
    partition = Partition(
        partition_id="p0",
        ordinal=0,
        location="file:///data/orders/p0.parquet",
        strategy=PartitionStrategy.HASH,
        keys=["order_id"],
        row_count=10,
        size_bytes=320,
    )
    plan = PlanNode(
        node_id="output",
        node_type=PlanNodeType.OUTPUT,
        output_schema=schema,
        children=[
            PlanNode(
                node_id="scan",
                node_type=PlanNodeType.SCAN,
                output_schema=schema,
                properties={"table": "default.orders", "partitions": 1},
                statistics=statistics,
            )
        ],
    )
    return [
        schema,
        partition,
        statistics,
        plan,
        Query(query_id="q1", sql="SELECT * FROM orders", logical_plan=plan),
        Stage(stage_id="s1", query_id="q1", plan=plan),
        Task(task_id="t1", query_id="q1", stage_id="s1", partition=partition),
        Attempt(attempt_id="a1", task_id="t1", attempt_number=0),
        Worker(
            worker_id="w1",
            endpoint="http://127.0.0.1:8091",
            state=WorkerState.ACTIVE,
            memory_limit_bytes=64 * 1024 * 1024,
        ),
    ]


@pytest.mark.parametrize("model", protocol_examples(), ids=lambda model: type(model).__name__)
def test_protocol_models_round_trip_json(model: BaseModel) -> None:
    restored = type(model).model_validate_json(model.model_dump_json())
    assert restored == model


def test_schema_rejects_duplicate_field_names() -> None:
    with pytest.raises(ValidationError, match="must be unique"):
        Schema(
            fields=[
                SchemaField(name="id", data_type=DataType.INT64),
                SchemaField(name="id", data_type=DataType.STRING),
            ]
        )


def test_protocol_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        Partition.model_validate(
            {
                "partition_id": "p0",
                "ordinal": 0,
                "location": "file:///p0",
                "unexpected": True,
            }
        )


def test_remote_task_protocol_rejects_wrong_version_and_payload_type() -> None:
    common = {
        "task_id": "task",
        "attempt_id": "attempt",
        "query_id": "query",
        "stage_id": "stage",
        "operation": RemoteTaskOperation.SHUFFLE_WRITE,
        "output_location": "s3://bucket/results/result.parquet",
    }
    with pytest.raises(ValidationError, match="Input should be 1"):
        RemoteTaskSubmission.model_validate(
            common
            | {
                "version": 2,
                "payload": {
                    "source_location": "s3://bucket/source.parquet",
                    "shuffle_root": "s3://bucket/shuffle",
                    "partition_count": 1,
                },
            }
        )
    with pytest.raises(ValidationError, match="partition_count"):
        RemoteTaskSubmission.model_validate(
            common
            | {
                "payload": {
                    "source_location": "s3://bucket/source.parquet",
                    "shuffle_root": "s3://bucket/shuffle",
                    "partition_count": "two",
                },
            }
        )


def test_worker_rejects_available_slots_above_capacity() -> None:
    with pytest.raises(ValidationError, match="cannot exceed configured slots"):
        Worker(
            worker_id="w1",
            endpoint="http://127.0.0.1:8091",
            slots=1,
            available_slots=2,
            memory_limit_bytes=1024,
        )


@pytest.mark.parametrize("state", [WorkerState.REGISTERING, WorkerState.LOST])
def test_heartbeat_rejects_non_operational_state(state: WorkerState) -> None:
    with pytest.raises(ValidationError, match="active or draining"):
        WorkerHeartbeat(lease_id="lease", available_slots=0, state=state)

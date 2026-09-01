from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from distributed_sql.common.config import WorkerSettings
from distributed_sql.common.exceptions import DistributedSQLError
from distributed_sql.common.protocol import (
    AttemptState,
    RemoteTaskOperation,
    RemoteTaskSubmission,
)
from distributed_sql.execution import ShuffleManifest
from distributed_sql.worker.app import create_app
from distributed_sql.worker.tasks import WorkerTaskManager


async def _terminal(
    manager: WorkerTaskManager,
    attempt_id: str,
) -> AttemptState:
    for _ in range(200):
        status = await manager.status(attempt_id)
        assert status is not None
        if status.state not in {AttemptState.CREATED, AttemptState.RUNNING}:
            return status.state
        await asyncio.sleep(0.01)
    raise AssertionError("Task did not finish")


@pytest.mark.asyncio
async def test_worker_shuffle_manifest_round_trip(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    pq.write_table(pa.table({"key": [1, 2, 1], "value": ["a", "b", "c"]}), source)
    manager = WorkerTaskManager(
        WorkerSettings(worker_id="worker-test", temp_directory=tmp_path / "tmp"),
        "worker-test",
    )
    write = RemoteTaskSubmission(
        task_id="shuffle-write",
        attempt_id="shuffle-write-attempt-000",
        query_id="query",
        stage_id="stage",
        operation=RemoteTaskOperation.SHUFFLE_WRITE,
        payload={
            "source_location": str(source),
            "shuffle_root": str(tmp_path / "shuffle"),
            "partition_count": 2,
            "keys": ["key"],
        },
        output_location="worker:///unused.parquet",
    )
    await manager.submit(write)
    assert await _terminal(manager, write.attempt_id) is AttemptState.SUCCEEDED
    write_status = await manager.status(write.attempt_id)
    assert write_status is not None and write_status.result is not None
    manifest = ShuffleManifest.model_validate(write_status.result.shuffle_manifests[0])

    rows = 0
    for partition in range(2):
        read = RemoteTaskSubmission(
            task_id=f"shuffle-read-{partition}",
            attempt_id=f"shuffle-read-{partition}-attempt-000",
            query_id="query",
            stage_id="stage-read",
            operation=RemoteTaskOperation.SHUFFLE_READ,
            payload={
                "shuffle_root": str(tmp_path / "shuffle"),
                "partition": partition,
                "manifests": [manifest.model_dump(mode="json")],
            },
            output_location=f"worker:///read-{partition}.parquet",
        )
        await manager.submit(read)
        assert await _terminal(manager, read.attempt_id) is AttemptState.SUCCEEDED
        status = await manager.status(read.attempt_id)
        assert status is not None and status.result is not None
        assert status.result.artifact is not None
        rows += pq.read_table(status.result.artifact.location).num_rows
    assert rows == 3
    await manager.close()


@pytest.mark.asyncio
async def test_worker_cancel_cleans_unpublished_result(tmp_path: Path) -> None:
    manager = WorkerTaskManager(
        WorkerSettings(worker_id="worker-test", temp_directory=tmp_path),
        "worker-test",
    )
    submission = RemoteTaskSubmission(
        task_id="cancel-task",
        attempt_id="cancel-attempt",
        query_id="query",
        stage_id="stage",
        operation=RemoteTaskOperation.SLEEP,
        payload={"seconds": 5},
        output_location="worker:///cancel/result.parquet",
    )
    await manager.submit(submission)
    await manager.cancel(submission.attempt_id)
    assert await _terminal(manager, submission.attempt_id) is AttemptState.CANCELED
    assert not (tmp_path / "results" / "cancel" / "result.parquet").exists()
    await manager.close()


@pytest.mark.asyncio
async def test_worker_cancel_waits_for_thread_and_times_out_without_false_terminal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = threading.Event()
    manager = WorkerTaskManager(
        WorkerSettings(
            worker_id="worker-test",
            temp_directory=tmp_path,
            cancellation_timeout_seconds=0.05,
        ),
        "worker-test",
    )
    submission = RemoteTaskSubmission(
        task_id="stubborn-task",
        attempt_id="stubborn-attempt",
        query_id="query",
        stage_id="stage",
        operation=RemoteTaskOperation.SLEEP,
        payload={"seconds": 0},
        output_location="worker:///stubborn/result.parquet",
    )
    original = manager._execute_sync

    def delayed_execute(
        task: RemoteTaskSubmission,
        cancellation: object,
    ) -> object:
        release.wait(1)
        return original(task, cancellation)  # type: ignore[arg-type]

    monkeypatch.setattr(manager, "_execute_sync", delayed_execute)
    await manager.submit(submission)
    await asyncio.sleep(0.01)

    with pytest.raises(DistributedSQLError, match="not confirmed"):
        await manager.cancel(submission.attempt_id)
    status = await manager.status(submission.attempt_id)
    assert status is not None
    assert status.state is AttemptState.RUNNING

    release.set()
    assert await _terminal(manager, submission.attempt_id) is AttemptState.CANCELED
    assert not (tmp_path / "results" / "stubborn" / "result.parquet").exists()
    await manager.close()


@pytest.mark.asyncio
async def test_cancel_does_not_delete_another_published_attempt(tmp_path: Path) -> None:
    manager = WorkerTaskManager(
        WorkerSettings(worker_id="worker-test", temp_directory=tmp_path),
        "worker-test",
    )
    published = RemoteTaskSubmission(
        task_id="published-task",
        attempt_id="published-attempt",
        query_id="other-query",
        stage_id="stage",
        operation=RemoteTaskOperation.SLEEP,
        payload={"seconds": 0},
        output_location="worker:///published/result.parquet",
    )
    canceled = RemoteTaskSubmission(
        task_id="canceled-task",
        attempt_id="canceled-attempt",
        query_id="canceled-query",
        stage_id="stage",
        operation=RemoteTaskOperation.SLEEP,
        payload={"seconds": 1},
        output_location="worker:///canceled/result.parquet",
    )
    await manager.submit(published)
    assert await _terminal(manager, published.attempt_id) is AttemptState.SUCCEEDED
    await manager.submit(canceled)
    assert (await manager.cancel(canceled.attempt_id)).state is AttemptState.CANCELED  # type: ignore[union-attr]

    assert (tmp_path / "results" / "published" / "result.parquet").is_file()
    assert not (tmp_path / "results" / "canceled" / "result.parquet").exists()
    await manager.close()


@pytest.mark.asyncio
async def test_worker_rejects_unauthenticated_serialized_plan(tmp_path: Path) -> None:
    app = create_app(
        WorkerSettings(
            worker_id="worker-test",
            temp_directory=tmp_path,
            remote_task_auth_token="trusted-coordinator-token",
        )
    )
    submission = RemoteTaskSubmission(
        task_id="scan",
        attempt_id="scan-attempt",
        query_id="query",
        stage_id="stage",
        operation=RemoteTaskOperation.SCAN,
        payload={
            "plan": {"version": 1, "format": "python-pickle-v5", "payload": "AA=="},
            "table": {},
            "file_task": {"location": "s3://bucket/input.parquet", "format": "parquet"},
        },
        output_location="s3://bucket/result.parquet",
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://worker",
    ) as client:
        response = await client.post(
            "/api/v1/tasks",
            json=submission.model_dump(mode="json"),
        )
    assert response.status_code == 401

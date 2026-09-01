from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from scripts.task16_acceptance import (
    DATASET_NAMES,
    GIB,
    generate_sample_data,
    generate_stress_data,
    read_sample_rows,
    run_acceptance,
)


def _file_hashes(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != "manifest.json"
    }


@pytest.mark.property
def test_multiformat_examples_are_deterministic_and_equivalent(tmp_path: Path) -> None:
    first = generate_sample_data(tmp_path / "first")
    second = generate_sample_data(tmp_path / "second")

    expected: list[dict[str, Any]] | None = None
    for suffix in ("csv", "parquet", "avro", "orc"):
        rows = read_sample_rows(first[suffix])
        expected = rows if expected is None else expected
        assert rows == expected
        assert first[suffix].read_bytes() == second[suffix].read_bytes()


@pytest.mark.property
def test_stress_generator_meets_size_and_row_invariants(tmp_path: Path) -> None:
    target = 2 * 1024 * 1024
    generated = generate_stress_data(
        tmp_path / "data",
        target_bytes=target,
        payload_bytes=4096,
        partition_count=2,
        batch_rows=16,
    )
    original_hashes = _file_hashes(generated.root)
    cached = generate_stress_data(
        tmp_path / "data",
        target_bytes=target,
        payload_bytes=4096,
        partition_count=2,
        batch_rows=16,
    )

    assert generated.total_bytes >= target
    assert cached.total_bytes == generated.total_bytes
    assert _file_hashes(cached.root) == original_hashes
    assert set(cached.files) == set(DATASET_NAMES)
    for paths in cached.files.values():
        assert len(paths) == 2
        assert sum(pq.ParquetFile(path).metadata.num_rows for path in paths) > 0


@pytest.mark.integration
@pytest.mark.differential
@pytest.mark.timeout(120)
def test_small_acceptance_spills_and_matches_duckdb(tmp_path: Path) -> None:
    report = run_acceptance(
        tmp_path,
        target_bytes=2 * 1024 * 1024,
        payload_bytes=4096,
        partition_count=2,
        memory_budget_bytes=16 * 1024,
        generation_timeout_seconds=30,
        query_timeout_seconds=60,
    )

    assert report["status"] == "passed"
    assert report["data"]["actual_bytes"] >= 2 * 1024 * 1024
    assert set(report["workloads"]) == {"sort", "join", "aggregate"}
    assert all(item["spill_bytes"] > 0 for item in report["workloads"].values())
    assert (tmp_path / "task16-results.json").is_file()
    assert (tmp_path / "task16-summary.md").is_file()


@pytest.mark.stress
@pytest.mark.timeout(3600)
def test_one_gib_acceptance() -> None:
    if os.environ.get("RUN_TASK16_STRESS") != "1":
        pytest.skip("set RUN_TASK16_STRESS=1 to run the independent 1 GiB acceptance")
    root = Path(os.environ.get("TASK16_ARTIFACT_ROOT", "artifacts/task16"))
    report = run_acceptance(
        root,
        target_bytes=GIB,
        memory_budget_bytes=64 * 1024 * 1024,
        generation_timeout_seconds=float(
            os.environ.get("TASK16_GENERATION_TIMEOUT", "900")
        ),
        query_timeout_seconds=float(os.environ.get("TASK16_QUERY_TIMEOUT", "900")),
    )

    assert report["status"] == "passed"
    assert report["data"]["actual_bytes"] >= GIB
    for workload in report["workloads"].values():
        assert workload["execution_account_peak_bytes"] <= 64 * 1024 * 1024
        assert workload["spill_bytes"] > 0

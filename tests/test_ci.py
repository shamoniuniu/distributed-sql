from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

ROOT = Path(__file__).parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
SCANNER = ROOT / "scripts" / "verify_engine_independence.py"


def _workflow(name: str) -> dict[str, Any]:
    with (WORKFLOWS / name).open(encoding="utf-8") as stream:
        return cast(dict[str, Any], yaml.load(stream, Loader=yaml.BaseLoader))


def _steps(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    jobs = workflow["jobs"]
    return [step for job in jobs.values() for step in job["steps"]]


def _commands(workflow: dict[str, Any]) -> str:
    return "\n".join(step.get("run", "") for step in _steps(workflow))


@pytest.mark.parametrize("name", ["ci.yml", "stress.yml"])
def test_workflow_uses_pinned_actions_minimal_permissions_and_timeouts(name: str) -> None:
    workflow = _workflow(name)

    assert workflow["permissions"] == {"contents": "read"}
    assert all("timeout-minutes" in job for job in workflow["jobs"].values())
    actions = [step["uses"] for step in _steps(workflow) if "uses" in step]
    assert actions
    assert all(action.rsplit("@", 1)[-1].startswith("v") for action in actions)
    assert all(action.rsplit("@", 1)[-1][1:].isdigit() for action in actions)


def test_fast_workflow_runs_complete_locked_quality_gate_and_uploads_junit() -> None:
    workflow = _workflow("ci.yml")
    triggers = workflow["on"]
    commands = _commands(workflow)
    steps = _steps(workflow)

    assert {"push", "pull_request", "workflow_dispatch"} <= triggers.keys()
    assert "schedule" not in triggers
    assert "uv sync --dev --frozen" in commands
    assert "uv run ruff check ." in commands
    assert "uv run -- python -m mypy" in commands
    assert "uv run pytest -m \"not stress\"" in commands
    assert "--junitxml artifacts/ci/fast-junit.xml" in commands
    assert "./scripts/verify-docs.ps1" in commands
    assert "docker compose -f compose.yaml config --quiet" in commands
    assert "kubectl kustomize deploy/kubernetes" in commands
    assert "verify_engine_independence.py" in commands

    setup_uv = next(
        step for step in steps if step.get("uses", "").startswith("astral-sh/setup-uv@")
    )
    assert setup_uv["with"]["enable-cache"] == "true"
    assert setup_uv["with"]["cache-dependency-glob"] == "uv.lock"

    upload = next(
        step
        for step in steps
        if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    assert upload["if"] == "always()"
    assert upload["with"]["path"] == "artifacts/ci/fast-junit.xml"


def test_stress_workflow_is_independent_and_uploads_machine_readable_results() -> None:
    workflow = _workflow("stress.yml")
    triggers = workflow["on"]
    commands = _commands(workflow)
    steps = _steps(workflow)

    assert {"workflow_dispatch", "schedule"} <= triggers.keys()
    assert "push" not in triggers
    assert "pull_request" not in triggers
    stress_step = next(
        step
        for step in steps
        if "tests/test_task16_acceptance.py" in step.get("run", "")
    )
    assert stress_step["env"]["RUN_TASK16_STRESS"] == "1"
    assert "tests/test_task16_acceptance.py -m stress" in commands
    assert "--junitxml artifacts/task16/stress-junit.xml" in commands

    upload = next(
        step
        for step in steps
        if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    paths = upload["with"]["path"]
    assert "artifacts/task16/stress-junit.xml" in paths
    assert "artifacts/task16/task16-results.json" in paths
    assert "artifacts/task16/task16-summary.md" in paths


def _run_scanner(source_root: Path, mode: str = "all") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCANNER),
            "--source-root",
            str(source_root),
            "--mode",
            mode,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )


def test_engine_independence_scanner_accepts_production_tree() -> None:
    completed = _run_scanner(ROOT / "src" / "distributed_sql")

    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    "source",
    [
        "import duckdb\n",
        "from org.apache.calcite import Planner\n",
        "def execute():\n    return duckdb.connect(':memory:')\n",
        "import importlib\nimportlib.import_module('duckdb')\n",
    ],
)
def test_engine_independence_static_scan_rejects_forbidden_paths(
    tmp_path: Path,
    source: str,
) -> None:
    package = tmp_path / "distributed_sql"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "bad.py").write_text(source, encoding="utf-8")

    completed = _run_scanner(package, mode="static")

    assert completed.returncode == 1
    assert "bad.py" in completed.stdout


def test_engine_independence_runtime_check_blocks_dynamic_import(tmp_path: Path) -> None:
    package = tmp_path / "distributed_sql"
    package.mkdir()
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "bad.py").write_text(
        "import importlib\nimportlib.import_module('duckdb')\n",
        encoding="utf-8",
    )

    completed = _run_scanner(package, mode="runtime")

    assert completed.returncode == 1
    assert "forbidden runtime import: duckdb" in completed.stdout

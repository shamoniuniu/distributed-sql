from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from distributed_sql.common.config import WorkerSettings

ROOT = Path(__file__).parents[1]
KUBERNETES = ROOT / "deploy" / "kubernetes"


def _kubernetes_resources() -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    for path in sorted(KUBERNETES.glob("*.yaml")):
        if path.name == "kustomization.yaml":
            continue
        resources.extend(
            document
            for document in yaml.safe_load_all(path.read_text(encoding="utf-8"))
            if document is not None
        )
    return resources


def _resource(
    resources: list[dict[str, Any]],
    kind: str,
    name: str,
) -> dict[str, Any]:
    return next(
        item
        for item in resources
        if item["kind"] == kind and item["metadata"]["name"] == name
    )


def test_worker_can_bind_all_interfaces_and_advertise_a_routable_address() -> None:
    settings = WorkerSettings(host="0.0.0.0", advertised_host="worker-1", port=8091)
    assert settings.endpoint == "http://worker-1:8091"
    assert WorkerSettings(host="127.0.0.1", port=8092).endpoint == "http://127.0.0.1:8092"


def test_dockerfile_is_pinned_non_root_and_has_role_healthchecks() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "python:3.12.11-slim-bookworm@sha256:" in dockerfile
    assert "uv sync --frozen --no-dev --no-editable" in dockerfile
    assert "ARG BUILD_VERSION=0.1.0" in dockerfile
    assert 'org.opencontainers.image.version="${BUILD_VERSION}"' in dockerfile
    assert "DISTRIBUTED_SQL_BUILD_VERSION=${BUILD_VERSION}" in dockerfile
    assert "USER 10001:10001" in dockerfile
    assert "FROM runtime AS coordinator" in dockerfile
    assert "FROM runtime AS worker" in dockerfile
    assert dockerfile.count("HEALTHCHECK") == 2
    assert 'ENTRYPOINT ["distributed-sql-coordinator"]' in dockerfile
    assert 'ENTRYPOINT ["distributed-sql-worker"]' in dockerfile


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker CLI is unavailable")
def test_compose_configuration_has_cluster_network_and_persistence() -> None:
    completed = subprocess.run(
        ["docker", "compose", "-f", "compose.yaml", "config", "--format", "json"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    compose: dict[str, Any] = json.loads(completed.stdout)
    services = compose["services"]
    assert {"coordinator", "worker-1", "worker-2", "minio"} <= services.keys()
    assert services["coordinator"]["build"]["target"] == "coordinator"
    assert services["worker-1"]["build"]["target"] == "worker"
    assert services["worker-2"]["build"]["target"] == "worker"
    assert services["worker-1"]["environment"]["DISTRIBUTED_SQL_WORKER_ID"] == "worker-1"
    assert services["worker-2"]["environment"]["DISTRIBUTED_SQL_WORKER_ID"] == "worker-2"
    for name in ("coordinator", "worker-1", "worker-2"):
        environment = services[name]["environment"]
        assert any(key.endswith("_OBJECT_STORE_ENDPOINT") for key in environment)
        assert any(key.endswith("_OBJECT_STORE_ACCESS_KEY") for key in environment)
        assert any(key.endswith("_OBJECT_STORE_SECRET_KEY") for key in environment)
        assert any(key.endswith("_OBJECT_STORE_BUCKET") for key in environment)
        assert any(key.endswith("_OBJECT_STORE_REGION") for key in environment)
        assert any(key.endswith("_REMOTE_TASK_AUTH_TOKEN") for key in environment)
    assert "distributed-sql" in compose["networks"]
    assert {"coordinator-data", "minio-data"} <= compose["volumes"].keys()
    assert services["minio"]["healthcheck"]["test"]


def test_kubernetes_resources_cover_workloads_configuration_and_storage() -> None:
    resources = _kubernetes_resources()
    assert _resource(resources, "Namespace", "distributed-sql")
    assert _resource(resources, "ConfigMap", "distributed-sql-config")
    secret = _resource(resources, "Secret", "distributed-sql-object-store")
    assert secret["type"] == "Opaque"
    assert _resource(resources, "PersistentVolumeClaim", "coordinator-catalog")
    assert _resource(resources, "PersistentVolumeClaim", "minio-data")
    for service in ("coordinator", "workers", "minio"):
        assert _resource(resources, "Service", service)

    for name in ("coordinator", "worker", "minio"):
        deployment = _resource(resources, "Deployment", name)
        spec = deployment["spec"]
        assert spec["strategy"]["type"] == "RollingUpdate"
        pod_spec = spec["template"]["spec"]
        assert pod_spec["securityContext"]["runAsNonRoot"] is True
        container = pod_spec["containers"][0]
        assert {"startupProbe", "readinessProbe", "livenessProbe"} <= container.keys()
        assert {"requests", "limits"} <= container["resources"].keys()
        assert container["securityContext"]["allowPrivilegeEscalation"] is False
        assert container["securityContext"]["readOnlyRootFilesystem"] is True

    worker = _resource(resources, "Deployment", "worker")
    assert worker["spec"]["replicas"] >= 2
    worker_env = worker["spec"]["template"]["spec"]["containers"][0]["env"]
    field_paths = {
        item["valueFrom"]["fieldRef"]["fieldPath"]
        for item in worker_env
        if "valueFrom" in item
    }
    assert {"metadata.name", "status.podIP"} <= field_paths
    worker_env_from = worker["spec"]["template"]["spec"]["containers"][0]["envFrom"]
    assert {"secretRef": {"name": "distributed-sql-object-store"}} in worker_env_from

    config = _resource(resources, "ConfigMap", "distributed-sql-config")["data"]
    for role in ("COORDINATOR", "WORKER"):
        assert config[f"DISTRIBUTED_SQL_{role}_OBJECT_STORE_ENDPOINT"] == "http://minio:9000"
        assert config[f"DISTRIBUTED_SQL_{role}_OBJECT_STORE_BUCKET"] == "distributed-sql"
        assert config[f"DISTRIBUTED_SQL_{role}_OBJECT_STORE_REGION"] == "us-east-1"
    secret_data = secret["stringData"]
    for role in ("COORDINATOR", "WORKER"):
        assert f"DISTRIBUTED_SQL_{role}_OBJECT_STORE_ACCESS_KEY" in secret_data
        assert f"DISTRIBUTED_SQL_{role}_OBJECT_STORE_SECRET_KEY" in secret_data
        assert f"DISTRIBUTED_SQL_{role}_REMOTE_TASK_AUTH_TOKEN" in secret_data

    coordinator = _resource(resources, "Deployment", "coordinator")
    coordinator_volumes = coordinator["spec"]["template"]["spec"]["volumes"]
    assert any(
        volume.get("persistentVolumeClaim", {}).get("claimName") == "coordinator-catalog"
        for volume in coordinator_volumes
    )


@pytest.mark.skipif(shutil.which("kubectl") is None, reason="kubectl is unavailable")
def test_kustomize_renders_all_resources_offline() -> None:
    completed = subprocess.run(
        ["kubectl", "kustomize", "deploy/kubernetes"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    rendered = list(yaml.safe_load_all(completed.stdout))
    assert len(rendered) == 11
    assert all(
        item["kind"] == "Namespace"
        or item.get("metadata", {}).get("namespace") == "distributed-sql"
        for item in rendered
    )


def test_kubernetes_smoke_script_performs_upgrade_and_undo() -> None:
    script = (ROOT / "scripts" / "verify-kubernetes.ps1").read_text(encoding="utf-8")
    assert "set image deployment/coordinator" in script
    assert "set image deployment/worker" in script
    assert "rollout undo deployment/coordinator" in script
    assert "rollout undo deployment/worker" in script
    assert script.count("Invoke-DeploymentSmoke") >= 4
    assert "require-existing-catalog" in script


def test_deployment_smoke_imports_partitioned_data() -> None:
    script = (ROOT / "scripts" / "deployment_smoke.py").read_text(encoding="utf-8")
    assert 'f"{table_path}/imports"' in script
    assert '"partition_count": 2' in script
    assert '"partition_key": "id"' in script
    assert "s3://distributed-sql/" in script
    assert "deployment_smoke.shared_numbers" in script


def test_task24_acceptance_records_real_images_catalog_and_cleanup() -> None:
    acceptance = (ROOT / "scripts" / "kubernetes_acceptance.py").read_text(
        encoding="utf-8"
    )
    harness = (ROOT / "scripts" / "verify-task24.ps1").read_text(encoding="utf-8")

    for required in (
        '"schema_version": 1',
        '"image_id": image_id',
        '"revision": deployment["metadata"]["annotations"]',
        '"final_catalog"',
        '"rollout_history"',
        '"exit_codes"',
        'self.result["status"] = "passed"',
        'acceptance.result["status"] = "failed"',
    ):
        assert required in acceptance
    assert "running imageID did not change during upgrade" in acceptance
    assert "rollback did not restore initial imageID" in acceptance
    assert 'phase["smoke"]["catalog"]["table"] != initial_table' in acceptance

    assert "docker exec $container ctr --namespace k8s.io images import" in harness
    assert "--build-arg \"BUILD_VERSION=$initialVersion\"" in harness
    assert "--build-arg \"BUILD_VERSION=$upgradeVersion\"" in harness
    assert "docker rm --force --volumes $container" in harness
    assert "cleanup-v1.json" in harness

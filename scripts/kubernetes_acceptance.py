"""Collect auditable Kubernetes deployment, upgrade, and rollback evidence."""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class Acceptance:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.root = Path(__file__).parents[1]
        self.evidence_path = Path(args.evidence).resolve()
        self.commands: list[dict[str, Any]] = []
        self.result: dict[str, Any] = {
            "schema_version": 1,
            "task": 24,
            "status": "running",
            "started_at": datetime.now(UTC).isoformat(),
            "cluster": {"k3s_runtime_version": args.k3s_version},
            "images": {
                "initial": {
                    "coordinator": args.initial_coordinator_image,
                    "worker": args.initial_worker_image,
                },
                "upgrade": {
                    "coordinator": args.upgrade_coordinator_image,
                    "worker": args.upgrade_worker_image,
                },
            },
            "phases": {},
            "commands": self.commands,
        }

    def write(self) -> None:
        self.evidence_path.parent.mkdir(parents=True, exist_ok=True)
        self.evidence_path.write_text(
            json.dumps(self.result, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def run(
        self,
        command: list[str],
        *,
        phase: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        started = time.monotonic()
        completed = subprocess.run(
            command,
            cwd=self.root,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        entry = {
            "phase": phase,
            "command": command,
            "exit_code": completed.returncode,
            "duration_seconds": round(time.monotonic() - started, 3),
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
        self.commands.append(entry)
        self.write()
        if check and completed.returncode != 0:
            raise RuntimeError(
                f"Command failed ({completed.returncode}): {' '.join(command)}\n"
                f"{completed.stderr.strip()}"
            )
        return completed

    def kubectl(
        self,
        arguments: list[str],
        *,
        phase: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return self.run(["kubectl", *arguments], phase=phase, check=check)

    def kubectl_json(self, arguments: list[str], *, phase: str) -> Any:
        completed = self.kubectl([*arguments, "-o", "json"], phase=phase)
        return json.loads(completed.stdout)

    def image_inspect(self, image: str, *, phase: str) -> dict[str, Any]:
        completed = self.run(["docker", "image", "inspect", image], phase=phase)
        inspected = json.loads(completed.stdout)[0]
        return {
            "id": inspected["Id"],
            "repo_tags": inspected.get("RepoTags", []),
            "version_label": inspected["Config"].get("Labels", {}).get(
                "org.opencontainers.image.version"
            ),
            "build_version_env": next(
                (
                    value.split("=", 1)[1]
                    for value in inspected["Config"].get("Env", [])
                    if value.startswith("DISTRIBUTED_SQL_BUILD_VERSION=")
                ),
                None,
            ),
        }

    def rollout(self, deployment: str, *, phase: str) -> dict[str, Any]:
        completed = self.kubectl(
            [
                "-n",
                self.args.namespace,
                "rollout",
                "status",
                f"deployment/{deployment}",
                "--timeout=240s",
            ],
            phase=phase,
        )
        return {
            "status": "successful",
            "exit_code": completed.returncode,
            "output": completed.stdout.strip(),
        }

    def workload_state(self, deployment_name: str, expected_image: str) -> dict[str, Any]:
        deployment = self.kubectl_json(
            ["-n", self.args.namespace, "get", "deployment", deployment_name],
            phase=f"{self.current_phase}-snapshot",
        )
        component = deployment["metadata"]["labels"]["app.kubernetes.io/component"]
        pods = self.kubectl_json(
            [
                "-n",
                self.args.namespace,
                "get",
                "pods",
                "-l",
                f"app.kubernetes.io/component={component}",
            ],
            phase=f"{self.current_phase}-snapshot",
        )["items"]
        desired = deployment["spec"]["replicas"]
        ready = deployment["status"].get("readyReplicas", 0)
        if ready != desired or deployment["status"].get("availableReplicas", 0) != desired:
            raise AssertionError(f"{deployment_name} is not Ready: {ready}/{desired}")
        deployment_image = deployment["spec"]["template"]["spec"]["containers"][0]["image"]
        if deployment_image != expected_image:
            raise AssertionError(
                f"{deployment_name} expected {expected_image}, got {deployment_image}"
            )
        pod_states = []
        for pod in pods:
            container = pod["spec"]["containers"][0]
            container_status = pod.get("status", {}).get("containerStatuses", [{}])[0]
            if container["image"] != expected_image or not container_status.get("ready"):
                raise AssertionError(f"{deployment_name} Pod image/readiness mismatch")
            image_id = container_status.get("imageID")
            if not image_id:
                raise AssertionError(f"{deployment_name} Pod has no imageID")
            pod_states.append(
                {
                    "name": pod["metadata"]["name"],
                    "phase": pod["status"]["phase"],
                    "ready": container_status["ready"],
                    "image": container["image"],
                    "image_id": image_id,
                }
            )
        if len(pod_states) != desired:
            raise AssertionError(
                f"{deployment_name} expected {desired} Pods, got {len(pod_states)}"
            )
        return {
            "revision": deployment["metadata"]["annotations"][
                "deployment.kubernetes.io/revision"
            ],
            "desired_replicas": desired,
            "ready_replicas": ready,
            "expected_image": expected_image,
            "deployment_image": deployment_image,
            "pods": pod_states,
        }

    def snapshot(
        self,
        phase: str,
        coordinator_image: str,
        worker_image: str,
        rollouts: dict[str, Any],
    ) -> dict[str, Any]:
        self.current_phase = phase
        resources = self.kubectl_json(
            [
                "-n",
                self.args.namespace,
                "get",
                "deployment,pod,service,pvc",
            ],
            phase=f"{phase}-snapshot",
        )
        items = resources["items"]
        pvcs = [
            {
                "name": item["metadata"]["name"],
                "phase": item["status"]["phase"],
                "volume_name": item["spec"].get("volumeName"),
                "storage_class": item["spec"].get("storageClassName"),
            }
            for item in items
            if item["kind"] == "PersistentVolumeClaim"
        ]
        services = [
            {
                "name": item["metadata"]["name"],
                "type": item["spec"]["type"],
                "cluster_ip": item["spec"].get("clusterIP"),
                "ports": item["spec"].get("ports", []),
            }
            for item in items
            if item["kind"] == "Service"
        ]
        if len(pvcs) != 2 or any(pvc["phase"] != "Bound" for pvc in pvcs):
            raise AssertionError(f"Expected two Bound PVCs, got {pvcs}")
        if len(services) != 3:
            raise AssertionError(f"Expected three Services, got {len(services)}")
        return {
            "rollouts": rollouts,
            "deployments": {
                "coordinator": self.workload_state("coordinator", coordinator_image),
                "worker": self.workload_state("worker", worker_image),
                "minio": self.workload_state(
                    "minio",
                    "minio/minio:RELEASE.2025-04-22T22-12-26Z"
                    "@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e",
                ),
            },
            "pvcs": pvcs,
            "services": services,
            "pods": [
                {
                    "name": item["metadata"]["name"],
                    "phase": item["status"]["phase"],
                    "pod_ip": item["status"].get("podIP"),
                }
                for item in items
                if item["kind"] == "Pod"
            ],
        }

    def smoke(self, phase: str, require_existing: bool) -> dict[str, Any]:
        forward_command = [
            "kubectl",
            "-n",
            self.args.namespace,
            "port-forward",
            "service/coordinator",
            "18080:8080",
            "--address",
            "127.0.0.1",
        ]
        forward = subprocess.Popen(
            forward_command,
            cwd=self.root,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        started = time.monotonic()
        try:
            while time.monotonic() - started < 30:
                if forward.poll() is not None:
                    raise RuntimeError(
                        f"kubectl port-forward exited early with {forward.returncode}"
                    )
                try:
                    with socket.create_connection(("127.0.0.1", 18080), timeout=1):
                        break
                except OSError:
                    time.sleep(0.25)
            else:
                raise RuntimeError("kubectl port-forward did not become ready")

            target = self.evidence_path.parent / f"query-{phase}.json"
            command = [
                sys.executable,
                "scripts/deployment_smoke.py",
                "--url",
                "http://127.0.0.1:18080",
                "--timeout",
                "180",
                "--evidence",
                str(target),
            ]
            if require_existing:
                command.append("--require-existing-catalog")
            self.run(command, phase=f"{phase}-remote-query")
            loaded = json.loads(target.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise TypeError(f"{phase} smoke evidence must be a JSON object")
            smoke: dict[str, Any] = loaded
            if smoke["status"] != "passed":
                raise AssertionError(f"{phase} smoke did not pass")
            return smoke
        finally:
            forward.terminate()
            try:
                forward.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forward.kill()
                forward.wait(timeout=10)
            self.commands.append(
                {
                    "phase": f"{phase}-port-forward",
                    "command": forward_command,
                    "exit_code": forward.returncode,
                    "termination": "terminated after smoke query",
                }
            )
            self.write()

    @staticmethod
    def pod_image_ids(snapshot: dict[str, Any], workload: str) -> set[str]:
        return {
            pod["image_id"]
            for pod in snapshot["deployments"][workload]["pods"]
        }

    def execute(self) -> None:
        initial_images = self.result["images"]["initial"]
        upgrade_images = self.result["images"]["upgrade"]
        if initial_images == upgrade_images:
            raise AssertionError("Initial and upgrade image references must differ")

        image_details = {
            "initial": {
                role: self.image_inspect(image, phase="image-verification")
                for role, image in initial_images.items()
            },
            "upgrade": {
                role: self.image_inspect(image, phase="image-verification")
                for role, image in upgrade_images.items()
            },
        }
        for role in ("coordinator", "worker"):
            initial = image_details["initial"][role]
            upgrade = image_details["upgrade"][role]
            if initial["id"] == upgrade["id"]:
                raise AssertionError(f"{role} initial/upgrade Docker image IDs are identical")
            if initial["version_label"] != self.args.initial_build_version:
                raise AssertionError(f"{role} initial image build label is incorrect")
            if upgrade["version_label"] != self.args.upgrade_build_version:
                raise AssertionError(f"{role} upgrade image build label is incorrect")
        self.result["images"]["details"] = image_details

        self.result["cluster"]["kubernetes_version"] = self.kubectl_json(
            ["version"], phase="cluster-version"
        )
        nodes = self.kubectl_json(["get", "nodes"], phase="cluster-version")
        self.result["cluster"]["nodes"] = [
            {
                "name": node["metadata"]["name"],
                "kubelet_version": node["status"]["nodeInfo"]["kubeletVersion"],
                "container_runtime_version": node["status"]["nodeInfo"][
                    "containerRuntimeVersion"
                ],
            }
            for node in nodes["items"]
        ]

        self.kubectl(["apply", "-k", "deploy/kubernetes"], phase="initial")
        for deployment, image in (
            ("coordinator", initial_images["coordinator"]),
            ("worker", initial_images["worker"]),
        ):
            self.kubectl(
                [
                    "-n",
                    self.args.namespace,
                    "set",
                    "image",
                    f"deployment/{deployment}",
                    f"{deployment}={image}",
                ],
                phase="initial",
            )
        initial_rollouts = {
            name: self.rollout(name, phase="initial")
            for name in ("minio", "coordinator", "worker")
        }
        initial = self.snapshot(
            "initial",
            initial_images["coordinator"],
            initial_images["worker"],
            initial_rollouts,
        )
        initial["smoke"] = self.smoke("initial", require_existing=False)
        self.result["phases"]["initial"] = initial
        self.write()

        for deployment, image in (
            ("coordinator", upgrade_images["coordinator"]),
            ("worker", upgrade_images["worker"]),
        ):
            self.kubectl(
                [
                    "-n",
                    self.args.namespace,
                    "set",
                    "image",
                    f"deployment/{deployment}",
                    f"{deployment}={image}",
                ],
                phase="upgrade",
            )
        upgrade_rollouts = {
            name: self.rollout(name, phase="upgrade")
            for name in ("coordinator", "worker")
        }
        upgrade = self.snapshot(
            "upgrade",
            upgrade_images["coordinator"],
            upgrade_images["worker"],
            upgrade_rollouts,
        )
        upgrade["smoke"] = self.smoke("upgrade", require_existing=True)
        self.result["phases"]["upgrade"] = upgrade
        self.write()

        for role in ("coordinator", "worker"):
            if self.pod_image_ids(initial, role) == self.pod_image_ids(upgrade, role):
                raise AssertionError(f"{role} running imageID did not change during upgrade")

        for deployment in ("coordinator", "worker"):
            self.kubectl(
                [
                    "-n",
                    self.args.namespace,
                    "rollout",
                    "undo",
                    f"deployment/{deployment}",
                ],
                phase="rollback",
            )
        rollback_rollouts = {
            name: self.rollout(name, phase="rollback")
            for name in ("coordinator", "worker")
        }
        rollback = self.snapshot(
            "rollback",
            initial_images["coordinator"],
            initial_images["worker"],
            rollback_rollouts,
        )
        rollback["smoke"] = self.smoke("rollback", require_existing=True)
        self.result["phases"]["rollback"] = rollback

        initial_table = initial["smoke"]["catalog"]["table"]
        initial_rows = initial["smoke"]["query_result"]["rows"]
        for phase_name, phase in (("upgrade", upgrade), ("rollback", rollback)):
            if phase["smoke"]["catalog"]["table"] != initial_table:
                raise AssertionError(f"{phase_name} Catalog table metadata changed")
            if phase["smoke"]["query_result"]["rows"] != initial_rows:
                raise AssertionError(f"{phase_name} query results changed")
        for role in ("coordinator", "worker"):
            if self.pod_image_ids(rollback, role) != self.pod_image_ids(initial, role):
                raise AssertionError(f"{role} rollback did not restore initial imageID")

        histories: dict[str, Any] = {}
        for deployment in ("coordinator", "worker"):
            history = self.kubectl(
                [
                    "-n",
                    self.args.namespace,
                    "rollout",
                    "history",
                    f"deployment/{deployment}",
                ],
                phase="final-history",
            )
            histories[deployment] = {
                "text": history.stdout.strip(),
                "exit_code": history.returncode,
            }
        histories["replica_sets"] = self.kubectl_json(
            ["-n", self.args.namespace, "get", "replicasets"],
            phase="final-history",
        )
        self.result["rollout_history"] = histories
        self.result["final_pvcs"] = self.kubectl_json(
            ["-n", self.args.namespace, "get", "pvc"],
            phase="final-state",
        )
        self.result["final_catalog"] = rollback["smoke"]["catalog"]
        self.result["exit_codes"] = {
            "all_recorded_commands_zero": all(
                command["exit_code"] == 0
                for command in self.commands
                if command["phase"] != "rollback-port-forward"
                and command["phase"] != "upgrade-port-forward"
                and command["phase"] != "initial-port-forward"
            ),
            "failed_command_count": sum(
                command["exit_code"] != 0
                for command in self.commands
                if "port-forward" not in command["phase"]
            ),
        }
        if not self.result["exit_codes"]["all_recorded_commands_zero"]:
            raise AssertionError("At least one recorded command failed")
        self.result["status"] = "passed"
        self.result["finished_at"] = datetime.now(UTC).isoformat()
        self.write()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--initial-coordinator-image", required=True)
    parser.add_argument("--initial-worker-image", required=True)
    parser.add_argument("--upgrade-coordinator-image", required=True)
    parser.add_argument("--upgrade-worker-image", required=True)
    parser.add_argument("--initial-build-version", required=True)
    parser.add_argument("--upgrade-build-version", required=True)
    parser.add_argument("--k3s-version", required=True)
    parser.add_argument("--namespace", default="distributed-sql")
    parser.add_argument("--evidence", default="artifacts/task24/k3s-results-v1.json")
    return parser.parse_args()


def main() -> int:
    acceptance = Acceptance(parse_args())
    acceptance.write()
    try:
        acceptance.execute()
    except BaseException as exc:
        acceptance.result["status"] = "failed"
        acceptance.result["finished_at"] = datetime.now(UTC).isoformat()
        acceptance.result["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        acceptance.write()
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# 部署指南

所有命令从仓库根目录执行。镜像使用固定 Python 基础镜像摘要和冻结的
`uv.lock`，Coordinator/Worker 分别构建，均以 UID/GID `10001` 非 root 运行并
提供角色健康检查。

## Docker Compose

前置条件：Docker Engine/Desktop 支持 Compose v2，端口 `8080`、`9000`、
`9001` 可用。可在 `.env` 设置 MinIO 凭据；默认值只适合本机演示。

完整自动验收：

```powershell
docker info
docker compose -f compose.yaml config --quiet
.\scripts\verify-compose.ps1
```

脚本会：

1. 构建 `distributed-sql-coordinator:0.1.0` 和
   `distributed-sql-worker:0.1.0`。
2. 启动 Coordinator、两个 Worker 和 MinIO，并等待健康状态。
3. 通过 Catalog REST API 将镜像内确定性 CSV 导入 MinIO 的两个分区，由远程
   Worker 从 `s3://distributed-sql/...` 扫描并经 MinIO 交换 Shuffle/结果。
4. 重启 Coordinator，要求 Catalog 对象和查询仍可用。
5. 默认停止容器但保留 named volumes；加 `-KeepRunning` 可保留集群。

手工生命周期：

```powershell
docker compose -f compose.yaml build --pull
docker compose -f compose.yaml up --detach --wait --wait-timeout 180
docker compose -f compose.yaml ps
Invoke-RestMethod http://127.0.0.1:8080/health
Invoke-RestMethod http://127.0.0.1:8080/api/v1/nodes
docker compose -f compose.yaml logs --tail 100 coordinator worker-1 worker-2
docker compose -f compose.yaml down
```

`docker compose down` 保留 `coordinator-data` 和 `minio-data`。仅在确定要删除
Catalog 和对象数据时执行：

```powershell
docker compose -f compose.yaml down --volumes
```

## Kubernetes

前置条件：可用 Kubernetes 集群、`kubectl`、默认 StorageClass，以及集群可拉取
的镜像仓库。当前清单创建 namespace、三个 Deployment、三个 Service、ConfigMap、
Secret 和两个 PVC；每个 Pod 均配置 startup/readiness/liveness probe、资源
请求/限制和 RollingUpdate。

### 1. 构建并推送不可变版本

```powershell
docker build --pull --target coordinator `
  -t registry.example/distributed-sql-coordinator:0.1.0 .
docker build --pull --target worker `
  -t registry.example/distributed-sql-worker:0.1.0 .
docker push registry.example/distributed-sql-coordinator:0.1.0
docker push registry.example/distributed-sql-worker:0.1.0
```

生产提交应使用不可变 tag 或 digest。将 `registry.example` 替换为实际仓库。

### 2. 静态检查并部署

```powershell
kubectl kustomize deploy/kubernetes
kubectl apply -k deploy/kubernetes
kubectl -n distributed-sql set image deployment/coordinator `
  coordinator=registry.example/distributed-sql-coordinator:0.1.0
kubectl -n distributed-sql set image deployment/worker `
  worker=registry.example/distributed-sql-worker:0.1.0
```

示例 Secret 必须在存储真实数据前替换，不要提交真实凭据：

```powershell
kubectl create secret generic distributed-sql-object-store `
  --namespace distributed-sql `
  --from-literal=DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_ACCESS_KEY='access-key' `
  --from-literal=DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_SECRET_KEY='secret-key' `
  --from-literal=DISTRIBUTED_SQL_COORDINATOR_REMOTE_TASK_AUTH_TOKEN='random-task-token' `
  --from-literal=DISTRIBUTED_SQL_WORKER_OBJECT_STORE_ACCESS_KEY='access-key' `
  --from-literal=DISTRIBUTED_SQL_WORKER_OBJECT_STORE_SECRET_KEY='secret-key' `
  --from-literal=DISTRIBUTED_SQL_WORKER_REMOTE_TASK_AUTH_TOKEN='random-task-token' `
  --from-literal=MINIO_ROOT_USER='access-key' `
  --from-literal=MINIO_ROOT_PASSWORD='secret-key' `
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n distributed-sql rollout restart deployment/minio deployment/coordinator deployment/worker
```

`ConfigMap` 中为两类服务配置相同 endpoint、bucket 和 region，`Secret` 中配置
相同访问凭据和 Task API token。不要把密钥或 token 放入 Task payload；网络
策略还应限制 Worker `8091` 端口仅允许 Coordinator 访问。

等待并检查：

```powershell
kubectl -n distributed-sql rollout status deployment/minio --timeout=180s
kubectl -n distributed-sql rollout status deployment/coordinator --timeout=180s
kubectl -n distributed-sql rollout status deployment/worker --timeout=180s
kubectl -n distributed-sql get deployment,pod,service,pvc
kubectl -n distributed-sql port-forward service/coordinator 18080:8080
```

端口转发运行时，可在另一个终端访问 `http://127.0.0.1:18080/` 或执行
`uv run -- python -m distributed_sql.cli.main --url http://127.0.0.1:18080 ...`。

### 3. 升级和回滚验收

先构建并推送 `0.1.1`，再运行完整脚本：

```powershell
.\scripts\verify-kubernetes.ps1 `
  -InitialCoordinatorImage registry.example/distributed-sql-coordinator:0.1.0 `
  -InitialWorkerImage registry.example/distributed-sql-worker:0.1.0 `
  -UpgradeCoordinatorImage registry.example/distributed-sql-coordinator:0.1.1 `
  -UpgradeWorkerImage registry.example/distributed-sql-worker:0.1.1
```

脚本依次执行初始查询、升级、升级后查询、两个 Deployment 的 `rollout undo`、
镜像引用核对及回滚后查询。升级和回滚后的冒烟查询都要求使用升级前建立的
Catalog，以验证 PVC 持久性。

手工回滚命令：

```powershell
kubectl -n distributed-sql rollout history deployment/coordinator
kubectl -n distributed-sql rollout history deployment/worker
kubectl -n distributed-sql rollout undo deployment/coordinator
kubectl -n distributed-sql rollout undo deployment/worker
kubectl -n distributed-sql rollout status deployment/coordinator --timeout=180s
kubectl -n distributed-sql rollout status deployment/worker --timeout=180s
```

### 4. Docker 内临时 K3s 可审计验收

宿主无法运行 kind、但 Docker 支持特权容器时，可执行：

```powershell
.\scripts\verify-task24.ps1
```

脚本构建并向 K3s containerd 导入内容不同的 `task24-v1`、`task24-v2`
Coordinator/Worker 镜像，实际应用 `deploy/kubernetes`，依次执行初始查询、
滚动升级查询和 `rollout undo` 后查询。每阶段均核对 Deployment revision、
期望镜像、Pod `image/imageID`、rollout、PVC、Service、Catalog 分区和查询结果。
无论成功或失败，脚本最后都会删除临时 K3s 容器/网络、镜像归档、kubeconfig
和四个临时镜像标签。

本机通过记录：

- [K3s 主验收证据](../artifacts/task24/k3s-results-v1.json)
- [初始查询](../artifacts/task24/query-initial.json)
- [升级后查询](../artifacts/task24/query-upgrade.json)
- [回滚后查询](../artifacts/task24/query-rollback.json)
- [清理证据](../artifacts/task24/cleanup-v1.json)

主证据只有在全部断言完成后才写入 `status: passed`；异常路径写
`status: failed`。

### 5. 清理

以下命令会删除 namespace 及其中的 PVC 和数据：

```powershell
kubectl delete namespace distributed-sql
```

## 不依赖运行集群的检查

```powershell
docker compose -f compose.yaml config --quiet
kubectl kustomize deploy/kubernetes
uv run pytest tests/test_deployment.py -q
```

已完成的 Compose/K3s 实机验收记录见
[功能验证](../docs/verification.md#部署验证记录)。

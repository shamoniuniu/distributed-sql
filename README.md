# Distributed SQL

课程级分布式 SQL 计算系统。系统使用 Python 3.12、FastAPI、SQLGlot 和
PyArrow 实现 Coordinator/Worker 架构，自研逻辑计划、RBO、CBO、Stage/Task
调度、Shuffle、故障重试和算子落盘。DuckDB 只在测试中生成参考结果，不参与
系统查询执行或优化。

## 文档导航

- [系统架构与运行流程](docs/architecture.md)
- [优化、执行与核心代码](docs/implementation.md)
- [功能验证、评分证据与已知限制](docs/verification.md)
- [答辩汇报](docs/defense.md)
- [Compose/Kubernetes 部署](deploy/README.md)
- [Task16 机器可读结果](artifacts/task16/task16-results.json)
- [Task16 摘要](artifacts/task16/task16-summary.md)
- [Task18 远程 Worker 机器可读证据](artifacts/task18/task18-results.json)

## 快速开始

前置条件：Python 3.12、[uv](https://docs.astral.sh/uv/)；本机启动无需
Docker、Kubernetes 或外部云服务。

```powershell
uv sync --dev --frozen
Copy-Item .env.example .env
uv run -- python -m distributed_sql.local_cluster --workers 2
```

启动后：

- WebUI：<http://127.0.0.1:8080/>
- OpenAPI：<http://127.0.0.1:8080/docs>
- Coordinator 健康检查：<http://127.0.0.1:8080/health>
- Worker 默认监听端口：`8091`、`8092`

另开终端检查节点并执行查询；表必须先在 Catalog 中注册。

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/v1/nodes
uv run -- python -m distributed_sql.cli.main catalog namespaces
uv run -- python -m distributed_sql.cli.main query `
  "SELECT region, COUNT(*) AS n FROM orders GROUP BY region ORDER BY region"
```

使用 `Ctrl+C` 停止本机集群。快速质量门禁：

```powershell
uv sync --dev --frozen
uv run python scripts/verify_engine_independence.py
uv run ruff check .
uv run -- python -m mypy
.\scripts\verify-docs.ps1
docker compose -f compose.yaml config --quiet
kubectl kustomize deploy/kubernetes | Out-Null
New-Item -ItemType Directory -Force artifacts/ci | Out-Null
uv run pytest -m "not stress" --junitxml artifacts/ci/fast-junit.xml
```

## 持续集成

[`ci.yml`](.github/workflows/ci.yml) 在 push、pull request 和手动触发时从干净
checkout 按 `uv.lock` 安装依赖，执行上述快速门禁并上传 JUnit。完整 Mypy
范围由 `pyproject.toml` 的 `files = ["src", "tests"]` 固定。

[`stress.yml`](.github/workflows/stress.yml) 仅支持每周定时或手动触发，独立运行
1 GiB stress，并上传 JSON、JUnit 和 Markdown 摘要；它不属于每次 push 的快速
门禁。两个工作流均只授予 `contents: read`，固定 action 主版本并设置超时，
uv 缓存依赖 `uv.lock`。

当前状态：配置和本地等价门禁已通过，远端徽章待公开仓库创建。当前目录没有
Git 远端上下文，因此未声称 GitHub Actions 云端 run 已发生。

## 配置

配置由 Pydantic Settings 从环境变量或根目录 `.env` 读取。复制
[`.env.example`](.env.example) 后按需修改；不要提交真实对象存储密钥。

| 环境变量 | 默认值 | 作用 |
|---|---|---|
| `DISTRIBUTED_SQL_COORDINATOR_HOST` | `127.0.0.1` | Coordinator 监听地址 |
| `DISTRIBUTED_SQL_COORDINATOR_PORT` | `8080` | Coordinator 端口 |
| `DISTRIBUTED_SQL_COORDINATOR_CATALOG_PATH` | `data/catalog.db` | SQLite Catalog |
| `DISTRIBUTED_SQL_COORDINATOR_LEASE_TTL_SECONDS` | `6` | Worker 租约超时 |
| `DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_ENDPOINT` | 未设置 | S3/MinIO 端点 |
| `DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_ACCESS_KEY` | 未设置 | 对象存储访问键 |
| `DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_SECRET_KEY` | 未设置 | 对象存储密钥 |
| `DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_BUCKET` | 未设置 | 远程 Scan/Shuffle/结果共享 bucket；未设置时使用本机共享目录 |
| `DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_REGION` | `us-east-1` | 对象存储区域 |
| `DISTRIBUTED_SQL_COORDINATOR_OBJECT_STORE_SECURE` | `true` | 是否使用 HTTPS |
| `DISTRIBUTED_SQL_COORDINATOR_REMOTE_TASK_AUTH_TOKEN` | 未设置 | Coordinator 调用内部 Worker Task API 的 bearer token |
| `DISTRIBUTED_SQL_COORDINATOR_CANCELLATION_TIMEOUT_SECONDS` | `10` | 等待查询取消确认的上限 |
| `DISTRIBUTED_SQL_WORKER_HOST` | `127.0.0.1` | Worker 监听地址 |
| `DISTRIBUTED_SQL_WORKER_ADVERTISED_HOST` | 未设置 | Worker 对外注册地址 |
| `DISTRIBUTED_SQL_WORKER_PORT` | `8091` | Worker 端口 |
| `DISTRIBUTED_SQL_WORKER_COORDINATOR_URL` | `http://127.0.0.1:8080` | Coordinator 地址 |
| `DISTRIBUTED_SQL_WORKER_HEARTBEAT_INTERVAL_SECONDS` | `2` | 心跳间隔 |
| `DISTRIBUTED_SQL_WORKER_REGISTRATION_RETRY_SECONDS` | `1` | 注册重试间隔 |
| `DISTRIBUTED_SQL_WORKER_SLOTS` | `1` | Worker 并发槽位 |
| `DISTRIBUTED_SQL_WORKER_MEMORY_LIMIT_BYTES` | `67108864` | 执行账户预算 |
| `DISTRIBUTED_SQL_WORKER_TEMP_DIRECTORY` | `data/tmp` | Spill 临时目录 |
| `DISTRIBUTED_SQL_WORKER_OBJECT_STORE_*` | 未设置 | 与 Coordinator 相同的 endpoint/access key/secret/bucket/region/secure 配置 |
| `DISTRIBUTED_SQL_WORKER_REMOTE_TASK_AUTH_TOKEN` | 未设置 | 与 Coordinator 相同的内部 Task API token |
| `DISTRIBUTED_SQL_WORKER_CANCELLATION_TIMEOUT_SECONDS` | `5` | 等待执行线程响应取消的上限 |
| `DISTRIBUTED_SQL_LOCAL_WORKER_COUNT` | `2` | 本机启动 Worker 数 |
| `DISTRIBUTED_SQL_LOCAL_WORKER_START_PORT` | `8091` | 本机 Worker 起始端口 |
| `DISTRIBUTED_SQL_URL` | `http://127.0.0.1:8080` | CLI 使用的 Coordinator |

## Catalog 与数据导入

以下示例注册 CSV 表并按 `id` Hash 分成两个不可变分区。路径可以是本地路径、
`file://` 或已配置凭据的 `s3://`。

```powershell
uv run -- python -m distributed_sql.cli.main catalog create-namespace default
uv run -- python -m distributed_sql.cli.main catalog create-table default orders `
  --format csv `
  --location data/imported/orders `
  --schema '{"fields":[{"name":"id","data_type":"int64","nullable":false},{"name":"region","data_type":"string"},{"name":"amount","data_type":"float64"}]}'
uv run -- python -m distributed_sql.cli.main import default orders .\orders.csv `
  --source-format csv --partitions 2 --key id
uv run -- python -m distributed_sql.cli.main catalog tables default
```

不指定 `--key` 时按轮询方式分区。导入流程先写 partition 文件，最后原子发布
manifest，再把 partition、行数、字节数、NULL/NDV/min/max 统计写入 SQLite
Catalog。REST 等价入口为
`POST /api/v1/catalog/namespaces/{namespace}/tables/{table}/imports`。

支持注册的格式是 CSV、Parquet、Avro、ORC 和 Iceberg。Iceberg 表通过
PyIceberg 读取当前快照及 manifest；MinIO/S3 参数使用上表中的对象存储配置。

跨容器或跨主机部署必须为 Coordinator 和所有 Worker 配置同一 S3/MinIO
endpoint、bucket、region 和凭据。凭据只通过环境变量或 Kubernetes Secret
注入，不进入 Task 协议或 manifest；Task 数据位置使用 `s3://bucket/key` URI。
仅本机多进程模式可省略 bucket，并使用所有进程都可访问的共享目录。

远程计划片段当前采用显式 `python-pickle-v5`、version 1 封装。由于 pickle
只能处理可信输入，包含计划的 Worker Task API 在未配置认证 token 时拒绝执行，
部署时必须使用随机 Secret，并限制 Worker API 只允许 Coordinator 所在网络访问。

## SQL 支持矩阵

| 类别 | 支持 | 边界 |
|---|---|---|
| 查询 | `SELECT`、`FROM`、别名、`WHERE`、`ORDER BY`、`LIMIT` | 单条查询，必须有基础表 |
| Join | `INNER`、`LEFT`、`RIGHT`、`FULL OUTER JOIN ... ON` | 不支持 `CROSS/NATURAL/USING` |
| 分组 | `GROUP BY`、`HAVING`、`GROUPING SETS` | 不支持 `ROLLUP/CUBE` |
| 聚合 | `COUNT`、`SUM`、`AVG`、`MIN`、`MAX`、`COUNT(DISTINCT x)` | 单参数聚合 |
| 窗口 | `ROW_NUMBER`、`RANK`、`DENSE_RANK`、五种聚合 `OVER` | 支持 `PARTITION BY`、`ORDER BY`、整数 `ROWS` frame |
| 表达式 | 算术、比较、`AND/OR/NOT`、`IS NULL`、`IN`、`BETWEEN`、`LIKE`、searched `CASE` | SQL NULL 三值逻辑 |
| 标量函数 | `LOWER`、`UPPER`、`LENGTH`、`ABS`、`COALESCE`、`CONCAT`、`SUBSTRING`、`ROUND` | 固定参数规则 |
| 类型 | boolean、int32/int64、float32/float64、decimal、string、binary、date、timestamp、null | 支持必要的数值隐式转换和显式 `CAST` |
| 不支持 | DML、事务、CTE、子查询、`UNION`、`SELECT DISTINCT`、UDF、递归查询 | 返回明确语法或绑定错误 |

示例：

```sql
SELECT
  region,
  COUNT(DISTINCT id) AS customers,
  SUM(amount) AS total,
  RANK() OVER (ORDER BY SUM(amount) DESC) AS revenue_rank
FROM orders
WHERE amount BETWEEN 10 AND 1000
GROUP BY region
HAVING SUM(amount) > 100
ORDER BY total DESC
LIMIT 20;
```

## 查询与诊断

```powershell
uv run -- python -m distributed_sql.cli.main explain `
  "SELECT region, SUM(amount) AS total FROM orders GROUP BY region"
uv run -- python -m distributed_sql.cli.main query `
  "SELECT id, amount FROM orders ORDER BY id LIMIT 10" --no-wait
uv run -- python -m distributed_sql.cli.main status <query-id>
uv run -- python -m distributed_sql.cli.main cancel <query-id>
```

查询 REST API 提供提交、状态、取消、分页结果、计划、指标和顾问：
`/api/v1/queries`、`/api/v1/queries/{id}`、`/results`、`/plan`、`/metrics`
和 `/advisor`。EXPLAIN 同时显示原逻辑计划、RBO 轨迹、CBO 基数/代价、Join
决策和物理计划。

## 故障注入

自动化、确定性的单 Worker 租约丢失测试：

```powershell
uv run pytest tests/test_worker_recovery.py -q
uv run pytest tests/test_task18_remote_workers.py tests/test_worker_task_api.py -q
```

手工验证时先启动本机集群并提交足够长的查询，再结束一个 Worker 进程。默认
租约 TTL 为 `6` 秒；Coordinator 将相关 attempt 标记为 LOST，并在健康 Worker
上重试。可通过查询 metrics 的 retry events 和 Stage/Task 状态核对恢复过程。
不要终止 Coordinator：本项目不包含 Coordinator 主备和跨 Coordinator 查询恢复。

磁盘不足、取消和执行失败后的 Spill 清理可用以下定向测试复现：

```powershell
uv run pytest tests/test_memory_spill.py -q
```

## 部署

Compose 启动 Coordinator、两个 Worker 和 MinIO：

```powershell
docker compose -f compose.yaml config --quiet
docker compose -f compose.yaml up --build -d
.\scripts\verify-compose.ps1
docker compose -f compose.yaml down
```

Kubernetes 清单包含 Deployment、Service、ConfigMap、Secret、PVC、资源约束、
三类探针和 RollingUpdate。完整部署、升级与 `rollout undo` 见
[部署指南](deploy/README.md)。

```powershell
kubectl kustomize deploy/kubernetes
kubectl apply -k deploy/kubernetes
kubectl -n distributed-sql rollout status deployment/coordinator --timeout=180s
kubectl -n distributed-sql rollout status deployment/worker --timeout=180s
```

## 1 GiB 验收数据

压力数据不作为仓库交付物，`artifacts/task16/data/**/*.parquet` 已被
`.gitignore` 排除。保留的 manifest 和 Task16 报告用于审计；缺失数据时以下
命令会按 manifest 参数确定性重建并执行验收：

```powershell
.\scripts\verify-task16.ps1
```

该命令会运行快速测试和独立 stress 测试，通常不应放入每次开发循环。
当前可引用结果、测量口径和已知限制见[验证报告](docs/verification.md)。

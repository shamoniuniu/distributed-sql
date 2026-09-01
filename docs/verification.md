# 功能验证、评分证据与已知限制

本文只记录可由代码、测试或验收产物追溯的结论。Task16 数字来自
[`task16-results.json`](../artifacts/task16/task16-results.json) 和两份 JUnit，
不是估算值。

## 验证命令

```powershell
# 文档本地链接、PowerShell 代码块和引用命令静态检查
.\scripts\verify-docs.ps1

# 锁定安装、引擎独立性、格式、类型和全部快速测试
uv sync --dev --frozen
uv run python scripts/verify_engine_independence.py
uv run ruff check .
uv run -- python -m mypy
uv run pytest -m "not stress" --junitxml artifacts/ci/fast-junit.xml

# 独立 1 GiB 验收；会重建被忽略的压力数据
.\scripts\verify-task16.ps1

# 部署静态检查
docker compose -f compose.yaml config --quiet
kubectl kustomize deploy/kubernetes
uv run pytest tests/test_deployment.py -q
```

## Task16 压力验收

产物时间为 2026-08-31。快速 JUnit 记录 `161 passed`、`0 failed`、`0 skipped`，
耗时 `71.054 s`；独立 stress JUnit 记录 `1 passed`，耗时 `43.277 s`。压力
脚本自身记录的三项工作负载总耗时为 `38.570127 s`。

数据由确定性生成器创建：32 个 Parquet 文件、16,384 行、1,074,448,160 字节，
即 1.000658 GiB。两个逻辑 Worker 的执行账户预算均为 67,108,864 字节
（64 MiB）。结果摘要与 DuckDB 参考结果一致；DuckDB 仅用于独立结果比较。

| 工作负载 | 引擎耗时(s) | 结果行 | 执行账户峰值(B) | 进程 RSS 峰值(B) | Spill(B) | Shuffle 写(B) |
|---|---:|---:|---:|---:|---:|---:|
| Sort | 11.275247 | 16 | 67,090,212 | 2,135,519,232 | 898,065 | 11,849,941 |
| Join | 13.680702 | 16 | 67,074,882 | 2,308,820,992 | 9,651,243 | 3,471,590 |
| Aggregate | 10.921861 | 64 | 67,074,318 | 2,107,895,808 | 855,101 | 1,899,955 |

Sort 产生 5 个 external run；Join 产生 16 个 hash partition；Aggregate 产生
8 个 sort aggregate run。三个账户峰值都没有超过配置预算。

注意：执行账户只计入算子显式 charge 的行对象。进程 RSS 还包括 Python、
PyArrow、扫描输入、结果表和分配器保留内存，因此不能声称进程总内存限制在
64 MiB；表中同时保留两套口径。

## Task17 交付验证

2026-08-31 本轮实际执行结果：文档静态检查通过 6 个 Markdown 文件；Ruff
通过；Mypy 检查 78 个源码文件且无问题；全量非 stress 测试为 `161 passed,
1 deselected`，耗时 `55.43 s`；`docker compose config --quiet` 和
`kubectl kustomize deploy/kubernetes` 均通过。未重复运行 1 GiB stress，
压力数字沿用上述 Task16 留存产物。

## Task18 远程 Worker 验证

2026-08-31 定向验收命令：

```powershell
$env:DISTRIBUTED_SQL_TASK18_EVIDENCE='artifacts/task18/task18-results.json'
uv run pytest tests/test_task18_remote_workers.py tests/test_worker_task_api.py -q
uv run ruff check src/distributed_sql tests/test_task18_remote_workers.py
uv run -- python -m mypy
```

最终结果为全量非 stress `166 passed, 1 deselected`；Ruff 全仓项目代码检查
通过；Mypy 检查 83 个源码/测试文件且无问题。机器证据见
[`task18-results.json`](../artifacts/task18/task18-results.json)：Coordinator
PID 与两个 Worker PID 不同，两个 Worker 都收到 Task；`worker-1` 的首 attempt
为 `lost`，`worker-2` 的 retry 为 `succeeded`；远程查询写入和读取 Shuffle
各 4 行，结果为 1、2、3、4。证据明确记录 `logical_worker_used=false`。

Compose 实机证据见
[`compose-results.json`](../artifacts/task18/compose-results.json)：Catalog 表位于
`s3://distributed-sql/deployment-smoke/imported-numbers`，远程运行根为
`s3://distributed-sql/runtime`；两个没有共享文件卷的 Worker 完成 Scan、
Shuffle 和结果物化，Coordinator 重启后再次查询成功。Compose 容器和网络已
清理；Kubernetes 清单完成 `kubectl kustomize` 静态渲染及配置闭环测试。

## 部署验证记录

Task15 在 2026-08-31 的实机记录：

- `verify-compose.ps1` 验证 MinIO、Coordinator、两个 Worker 健康，Catalog
  创建、CSV 导入、查询及 Coordinator 重启后的 Catalog 持久化查询通过。
- 临时单节点 K3s 使用 `rancher/k3s:v1.33.5-k3s1`；三个 Deployment Ready，
  两个 PVC Bound。
- `verify-kubernetes.ps1` 验证导入、查询、镜像升级和 `rollout undo`；回滚至
  `0.1.0` 后 Catalog 和查询仍可用。
- 验收后的容器、网络、卷、临时镜像包和 kubeconfig 已清理。

这些是已完成验收的历史证据，不表示任意新机器无需准备 Docker/Kubernetes
环境即可直接通过。

### Task24 K3s 可审计验收

2026-08-31 使用 Docker 内临时 `rancher/k3s:v1.33.5-k3s1` 重新完成实机验收。
[主证据](../artifacts/task24/k3s-results-v1.json)采用 schema version 1，记录
Kubernetes/K3s 版本、51 条关键命令及退出码、Deployment/Pod/Service/PVC
快照、rollout history 和完整 Catalog。

- 三个 Deployment Ready，两个 PVC Bound，三个 Service 存在。
- 初始、升级、回滚 revision 分别为 `2`、`3`、`4`。
- Coordinator/Worker 的 `task24-v1` 与 `task24-v2` 镜像具有不同 OCI version
  label、Docker image ID 和运行中 Pod imageID；回滚后 Deployment 与 Pod
  imageID 均恢复 v1。
- [初始](../artifacts/task24/query-initial.json)、
  [升级后](../artifacts/task24/query-upgrade.json)和
  [回滚后](../artifacts/task24/query-rollback.json)三次远程查询均返回
  `1/10、2/20、3/30`，后两次复用初始 Catalog；表始终保持 3 行、2 个分区及
  相同分区元数据。
- 主证据状态为 `passed`、记录命令失败数为 0；
  [清理证据](../artifacts/task24/cleanup-v1.json)确认临时 K3s 容器/网络、
  kubeconfig、镜像归档及四个临时镜像标签已清理。
- 最终门禁：部署定向测试 `8 passed`；全量非 stress pytest
  `176 passed, 1 deselected`；Ruff、Mypy（86 个源码/测试文件）及文档链接
  检查通过。

## 原评分项映射

表中命令都从仓库根目录运行。“实现”列给出主要入口，不代表唯一文件。

| 类别/权重 | 原评分项 | 实现 | 自动化证据 | 复现命令 |
|---|---|---|---|---|
| 基础 20% | Scan、Project、Filter、Aggregate、四类 Join、Limit | `execution/operators.py`、`engine.py` | `test_execution.py` | `uv run pytest tests/test_execution.py -q` |
| 基础 2% | Predicate 下推 Project | `optimizer/rules.py::PredicatePushdownProject` | `test_optimizer.py::test_predicate_pushdown_project_shape_and_equivalence` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | Predicate 下推 Aggregate | `PredicatePushdownAggregate` | `test_predicate_pushdown_aggregate_*` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | Predicate 下推 Join | `PredicatePushdownJoin` | `test_predicate_pushdown_join_respects_outer_join_preserved_rows` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | Limit 下推 Project | `LimitPushdownProject` | `test_limit_pushdown_project_is_exact_and_equivalent` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | Limit 下推 Join | `LimitJoinInputHint` | `test_limit_join_hint_keeps_final_limit_and_is_not_executed` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | 列裁剪 | `ColumnPruning` | `test_column_pruning_*` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | 常量折叠 | `ConstantFolding` | `test_constant_folding_preserves_null_three_valued_logic` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | 谓词合并 | `PredicateMerge` | `test_predicate_merge_normalizes_and_deduplicates` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | 传递闭包 | `EqualityInference` | `test_equality_inference_crosses_inner_but_not_outer_join` | `uv run pytest tests/test_optimizer.py -q` |
| 基础 2% | Kubernetes 升级回滚 | `Dockerfile`、`compose.yaml`、`deploy/kubernetes/` | `test_deployment.py`、Task15 实机记录 | `uv run pytest tests/test_deployment.py -q` |
| 进阶 5% | Iceberg、Parquet、Avro、ORC | `data_source/files.py`、`iceberg.py` | `test_data_sources.py` | `uv run pytest tests/test_data_sources.py -q` |
| 进阶 5% | Repartition Shuffle | `execution/physical.py`、`shuffle.py` | `test_distributed_execution.py` | `uv run pytest tests/test_distributed_execution.py -q` |
| 进阶 5% | CBO Hash Join 构建侧 | `optimizer/cost.py` | `test_cbo.py::test_partition_strategy_selection` | `uv run pytest tests/test_cbo.py -q` |
| 进阶 5% | CBO Join Reorder | `optimizer/cbo.py::_reorder_region` | `test_dynamic_programming_reorders_inner_joins_but_not_outer_boundary` | `uv run pytest tests/test_cbo.py -q` |
| 进阶 5% | Runtime Filter | `execution/runtime_filter.py` | `test_runtime_filter.py` | `uv run pytest tests/test_runtime_filter.py -q` |
| 进阶 5% | Order/Having/Window/Grouping Sets | `planner/binder.py`、`execution/operators.py` | `test_advanced_execution.py` | `uv run pytest tests/test_advanced_execution.py -q` |
| 探索 7% | AI4DB | `advisor.py`、`observability.py` | `test_advisor_observability.py` | `uv run pytest tests/test_advisor_observability.py -q` |
| 探索 6% | Worker 故障恢复 | `execution/scheduler.py`、`coordinator/registry.py` | `test_worker_recovery.py` | `uv run pytest tests/test_worker_recovery.py -q` |
| 探索 7% | 小内存处理大数据 | `execution/memory.py`、`operators.py` | Task16 JSON/JUnit、`test_memory_spill.py` | `.\scripts\verify-task16.ps1` |
| 工程 5% | 整体架构设计 | Coordinator/Worker、Stage/Task/Attempt、统一协议 | `test_protocol.py`、`test_distributed_execution.py` | `uv run pytest tests/test_protocol.py tests/test_distributed_execution.py -q` |
| 工程 5% | 模块、单测、注释、集成测试 | `src/distributed_sql/`、`tests/` | 快速 JUnit、Ruff、Mypy | `uv run pytest -m "not stress"` |

## 功能面补充证据

| 功能 | 测试 |
|---|---|
| Catalog CRUD、导入、重启持久化 | `tests/test_catalog.py` |
| 本机两个 Worker、注册和心跳 | `tests/test_local_cluster.py`、`test_service_lifecycle.py` |
| REST、CLI、WebUI 查询工作流 | `tests/test_interfaces.py` |
| SQL 边界、绑定、类型、NULL | `tests/test_sql_parser.py`、`test_sql_binding.py`、`test_expressions.py` |
| Shuffle attempt 隔离、摘要校验 | `tests/test_distributed_execution.py` |
| 取消、磁盘不足及临时文件清理 | `tests/test_memory_spill.py` |
| 多格式确定性生成和小规模验收 | `tests/test_task16_acceptance.py` |

## 已知限制

- 这是课程系统，不是生产数据库。用户查询/Catalog API 无认证、授权、租户
  隔离、资源队列和审计；内部 Worker Task API 仅提供共享 bearer token。
- Coordinator 为单点；不支持主备、Raft 或跨 Coordinator 查询状态恢复。
- 查询状态和最终结果保存在 Coordinator 进程内存，重启不会恢复运行中查询。
- SQL 是显式子集：无 DML、事务、CTE、子查询、UNION、UDF、递归查询和通用
  ANSI SQL 兼容；`SELECT DISTINCT` 不支持。
- Iceberg 只读取当前快照，不负责写入、提交、时间旅行或并发事务。
- 查询服务已使用注册 Worker endpoint，不再在 Coordinator 中执行数据算子。
  当前计划片段使用显式 `python-pickle-v5`/version 1 封装，只适用于同版本、
  可信 Worker，尚不是跨语言协议。Worker 在未配置认证 token 时拒绝含计划
  Task；禁止把 pickle Task API 暴露给任意未认证外部输入。
- 本机共享目录和 Compose MinIO 数据面均已验证；Kubernetes 本轮只完成清单
  静态验证，实机升级/回滚沿用 Task15 记录。
- 仅验证单 Worker 故障；多 Worker 同时故障、极端倾斜和无限重试不在保证范围。
- 执行账户并非进程级硬内存上限；Task16 的 RSS 峰值明显高于 64 MiB。
- Shuffle/Spill 采用文件与对象存储，没有复制、纠删码或跨区域容灾。
- 1 GiB 结果来自记录所示环境的一次确定性验收，不代表吞吐量基准或性能承诺。
- Kubernetes 实机验证使用临时单节点 K3s；不同云厂商的 StorageClass、网络和
  镜像仓库仍需环境适配，Task24 证据不代表生产多节点集群验证。

## Task25 CI 与引擎独立性

GitHub Actions 快速工作流
[`ci.yml`](../.github/workflows/ci.yml) 在 push、pull request 和手动触发时执行
锁定安装、生产代码独立性扫描、Ruff、完整 Mypy、文档检查、Compose 配置检查、
Kustomize 渲染及全部非 stress pytest，并始终尝试上传快速 JUnit。

独立工作流 [`stress.yml`](../.github/workflows/stress.yml) 仅由每周计划或手动
触发，运行 1 GiB stress，上传 JUnit、JSON 和 Markdown 摘要，不阻塞每次 push
的快速门禁。两个 job 均有最小只读权限、明确超时、固定 action 主版本，uv 缓存
依赖 `uv.lock`。

[`verify_engine_independence.py`](../scripts/verify_engine_independence.py) 对
`src/distributed_sql` 执行 AST 导入/调用扫描，并在禁止加载 DuckDB/Calcite
的导入器下逐个导入生产模块。结构化 YAML 和违规样例测试位于
[`test_ci.py`](../tests/test_ci.py)。本机没有 `actionlint`，因此采用 PyYAML
结构化测试验证工作流语法与关键契约。

2026-08-31 状态：配置和本地等价门禁已通过，远端徽章待公开仓库创建。当前
工作目录不是 Git checkout，未执行或声称 GitHub Actions 云端 run。

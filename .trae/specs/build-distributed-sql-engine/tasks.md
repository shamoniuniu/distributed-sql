# 任务清单

- [x] Task 1：建立工程骨架与共享协议，形成可启动的 Coordinator/Worker 最小集群。
  - [x] 创建 Python 3.12 项目、依赖锁定、质量工具、配置模型和统一异常模型。
  - [x] 定义 Query、Stage、Task、Attempt、Worker、Schema、Partition、统计信息和计划节点协议。
  - [x] 实现 Coordinator/Worker 健康检查、注册、租约心跳及本机多进程启动脚本。
  - [x] 添加协议序列化和服务生命周期单元测试。

- [x] Task 2：实现持久化 Catalog、对象存储抽象和数据导入。
  - [x] 实现 namespace、表、列、位置、分区和统计信息的 SQLite 存储及 REST CRUD。
  - [x] 实现本地文件系统与 MinIO/S3 兼容对象存储抽象。
  - [x] 实现按键 Hash 或轮询分区的数据导入、原子清单发布和统计记录。
  - [x] 验证 Coordinator 重启后的 Catalog 持久化与导入数据可读性。

- [x] Task 3：实现统一数据源接口与 CSV、Parquet、Avro、ORC、Iceberg 适配器。
  - [x] 定义投影、谓词、批大小和文件任务可下推的扫描接口。
  - [x] 用 PyArrow/fastavro 实现四种文件格式读取并输出统一 RecordBatch。
  - [x] 用 PyIceberg 解析 MinIO 上表的当前快照、schema、manifest 和数据文件。
  - [x] 用同构数据差分验证四格式及 Iceberg 查询输入一致。

- [x] Task 4：实现 SQL 绑定、类型系统、表达式和逻辑计划。
  - [x] 用 SQLGlot 解析受支持 SELECT 方言并拒绝超范围语法。
  - [x] 实现名称绑定、别名/歧义检测、基础类型推导、隐式转换和 NULL 三值逻辑。
  - [x] 构建 Scan、Project、Filter、Aggregate、Join、Limit、Order、Window、Grouping Sets 逻辑节点。
  - [x] 添加解析、绑定、表达式求值和错误定位测试。

- [x] Task 5：实现单 Worker 批处理算子和基础 SQL 正确性。
  - [x] 实现 Scan、Project、Filter、Limit、Hash Aggregate 和 Hash Join。
  - [x] 正确实现 INNER、LEFT、RIGHT、FULL OUTER JOIN 的匹配与 NULL 补齐。
  - [x] 实现算子取消、批次迭代和基础指标采集。
  - [x] 与 DuckDB 差分测试基础算子组合和 NULL 边界。

- [x] Task 6：实现 RBO 规则框架与全部九项规则。
  - [x] 实现规则匹配、计划重写、不动点终止、规则轨迹和 EXPLAIN 输出。
  - [x] 实现 Predicate 对 Project、Aggregate、Join 的语义安全下推。
  - [x] 实现 Limit 对 Project、Join 的安全下推或执行上界提示。
  - [x] 实现列裁剪、常量折叠、谓词合并和等值传递闭包。
  - [x] 为每条规则添加计划形状与优化前后结果等价测试。

- [x] Task 7：实现统计信息、代价模型与 CBO。
  - [x] 收集行数、字节数、NULL、NDV、min/max，并实现缺失统计回退。
  - [x] 实现过滤、聚合、Join 的基数及 CPU/网络/内存/磁盘代价估算。
  - [x] 选择 Hash Join 构建侧和复用、广播、单侧/双侧 Repartition 策略。
  - [x] 用动态规划实现多表 Inner Join Reorder，并保持 Outer Join 边界。
  - [x] 添加估算、策略选择、重排与 EXPLAIN 证据测试。

- [x] Task 8：实现 Stage 切分、Task 调度和 Repartition Shuffle。
  - [x] 根据 Exchange 将物理计划切分为 Stage DAG 和 partition Task。
  - [x] 实现 Worker 并发槽位、依赖就绪、状态机、取消和结果汇总。
  - [x] 实现左侧、右侧、双侧 Repartition、广播及分区复用。
  - [x] 实现 attempt 隔离、原子 Shuffle 清单和 Shuffle 指标。
  - [x] 添加多 Worker 聚合、Join、Limit 和 Shuffle 数据完整性集成测试。

- [x] Task 9：实现高级 SQL 执行。
  - [x] 实现 ORDER BY、HAVING 和外部可替换的排序接口。
  - [x] 实现 ROW_NUMBER、RANK、DENSE_RANK 和聚合窗口函数及 ROWS frame。
  - [x] 实现 GROUPING SETS 和 COUNT DISTINCT。
  - [x] 与 DuckDB 差分测试分区、排序、frame、NULL 和空输入边界。

- [x] Task 10：实现 Runtime Filter。
  - [x] 从 Join 构建侧生成可序列化 Bloom 和 min/max filter。
  - [x] 将 Filter 传递到安全的探测侧 Scan，并记录过滤指标。
  - [x] 禁止对会改变 Outer Join 保留侧语义的输入应用 Filter。
  - [x] 添加误判容忍、无假阴性、分布式传递和结果等价测试。

- [x] Task 11：实现内存管理和算子落盘。
  - [x] 实现每查询/Task 的内存账户、64 MB 可配置预算和临时文件管理。
  - [x] 实现外部归并排序。
  - [x] 实现分区 Hash Join，并在适用时回退 Sort-Merge Join。
  - [x] 实现 Sort Aggregate 和溢写指标。
  - [x] 验证成功、失败、取消、磁盘不足后的临时文件清理。

- [x] Task 12：实现 Worker 故障恢复。
  - [x] 根据心跳租约标记失联节点并定位受影响 attempt。
  - [x] 在健康 Worker 上重试 Task，保证仅消费原子发布的 attempt 输出。
  - [x] 实现重试上限、退避、超时和带上下文的最终错误。
  - [x] 添加查询中终止 Worker 的无重复、无丢失故障集成测试。

- [x] Task 13：实现 AI4DB 查询顾问和可观测性。
  - [x] 汇总优化轨迹、统计质量、物理计划、Stage/Task 指标和重试事件。
  - [x] 实现缺失统计、高 Shuffle、数据倾斜、落盘和 Join 策略建议规则。
  - [x] 输出严重度、证据、原因、动作、预期影响及无建议状态。
  - [x] 提供结构化日志、EXPLAIN ANALYZE 和查询诊断时间线。

- [x] Task 14：实现 REST API、CLI 和 WebUI 完整工作流。
  - [x] 实现提交、状态、取消、结果分页、计划、指标、节点和顾问 API。
  - [x] 实现 CLI 的 Catalog、导入、查询、EXPLAIN、状态和取消命令。
  - [x] 实现查询编辑、结果、计划、指标、节点状态和 Catalog 管理 WebUI。
  - [x] 添加 API 合同、CLI 冒烟和关键 WebUI 工作流测试。

- [x] Task 15：完成 Docker Compose 与 Kubernetes 部署。
  - [x] 构建可重复、非 root、带健康检查的 Coordinator/Worker 镜像。
  - [x] 编排 Coordinator、两个以上 Worker、MinIO、网络和持久化卷。
  - [x] 提供 Kubernetes 工作负载、服务、配置、Secret、PVC、探针和滚动策略。
  - [x] 验证 Compose 端到端查询及 Kubernetes 部署、升级、回滚冒烟流程。
  - 验证状态（2026-08-31）：Docker Desktop 29.6.2 经 CLI 重启后恢复，
    `docker info` 通过；Coordinator/Worker 镜像实构建通过。
  - `.\scripts\verify-compose.ps1` 实机通过：MinIO、Coordinator、两个 Worker
    均健康；Catalog 创建、CSV 导入（2 个分区）、查询及 Coordinator 重启后的
    Catalog 持久化查询均通过。容器、网络和卷随后已清理。
  - 使用 `rancher/k3s:v1.33.5-k3s1` 特权容器创建临时单节点 Kubernetes，
    将本地 Coordinator/Worker 0.1.0 与 0.1.1 标签及固定版本 MinIO 镜像导入
    K3s containerd；`deploy/kubernetes` 清单应用成功，3 个 Deployment Ready，
    2 个 PVC Bound。
  - `.\scripts\verify-kubernetes.ps1` 实机通过：Catalog 创建、CSV 导入（3 行、
    2 个分区）和查询成功；镜像升级后查询与 Catalog 持久化通过；执行
    `kubectl rollout undo` 后恢复 0.1.0 镜像，Catalog 持久化及查询再次通过。
  - `docker compose config --quiet`、`kubectl kustomize deploy/kubernetes`、
    Ruff、Mypy、全量 pytest（158 passed）通过；部署定向测试 7 passed。
    K3s 容器、网络、临时镜像包和 kubeconfig 在验收后清理。

- [x] Task 16：建立完整自动化验收与 1 GB 压力测试。
  - [x] 建立快速单测、属性/差分测试、小规模分布式集成测试和故障测试分层。
  - [x] 实现确定性多格式示例数据和至少 1 GB 压力数据生成器。
  - [x] 在每 Worker 64 MB 执行预算下验证排序、Join、聚合落盘及 DuckDB 结果一致性。
  - [x] 输出 JUnit/JSON 测试结果、资源峰值、Shuffle/Spill 指标和可引用摘要。
  - 验证状态（2026-08-31）：pytest markers 分为 unit、property、
    differential、integration、fault、stress；快速全量 161 passed，
    独立 1 GiB stress 1 passed，Ruff 与 Mypy 通过。
  - 确定性压力数据为 32 个 Parquet 文件、16,384 行、1,074,448,160 字节
    （1.000658 GiB），并生成 CSV、Parquet、Avro、ORC 同构示例。
  - 两个逻辑 Worker 的执行账户预算均配置为 67,108,864 字节。排序、Join、
    聚合账户峰值分别为 67,090,212、67,074,882、67,074,318 字节；
    三者结果摘要均与 DuckDB 参考结果一致。
  - 排序产生 5 个 external sort run、898,065 spill 字节；Join 产生 16 个
    hash partition、9,651,243 spill 字节；聚合产生 8 个 sort aggregate run、
    855,101 spill 字节。进程 RSS 峰值分别为 2,135,519,232、
    2,308,820,992、2,107,895,808 字节，已与执行账户预算分开记录。
  - 机器可读结果、资源指标与答辩摘要位于 `artifacts/task16/`。

- [x] Task 17：完成课程交付文档。
  - [x] 编写快速开始、配置、SQL 支持矩阵、Catalog/导入、故障注入和部署指南。
  - [x] 编写系统架构图、模块交互图、查询运行流程和分布式思想说明。
  - [x] 编写 RBO、CBO、Shuffle、容错、落盘和核心代码实现说明。
  - [x] 编写功能验证、测试结果、评分项证据映射和已知限制。
  - [x] 生成答辩汇报文档，保留项目成员分工和公开仓库地址待填字段。

# 任务依赖

- Task 2、Task 4 依赖 Task 1；二者可并行。
- Task 3 依赖 Task 2。
- Task 5 依赖 Task 3、Task 4。
- Task 6 依赖 Task 4、Task 5。
- Task 7 依赖 Task 2、Task 4、Task 6。
- Task 8 依赖 Task 1、Task 5、Task 7。
- Task 9 依赖 Task 4、Task 5、Task 8。
- Task 10 依赖 Task 3、Task 7、Task 8。
- Task 11 依赖 Task 5、Task 8、Task 9。
- Task 12 依赖 Task 8。
- Task 13 依赖 Task 6、Task 7、Task 8、Task 10、Task 11、Task 12。
- Task 14 依赖 Task 2、Task 8、Task 9、Task 13。
- Task 15 依赖 Task 1、Task 2、Task 14。
- Task 16 依赖 Task 3 至 Task 15。
- Task 17 依赖 Task 16 的最终证据；可在实现期间持续维护非结果章节。

# Seventh 系统性验证修复任务

- [x] Task 18：实现真实的远程 Worker 数据面并恢复 Coordinator/Worker 职责边界。
  - [x] 在 Worker 服务提供版本化、可序列化的 Task 提交、状态、取消和结果/Shuffle
    manifest 协议与 API，由 Worker 进程执行 Scan、算子、Shuffle 和结果物化。
  - [x] 将 Coordinator 的调度从进程内 `LogicalWorker.execute()` 改为调用已注册
    Worker endpoint；Coordinator 只保留规划、Stage/Task 调度、重试和结果汇总。
  - [x] 让取消、租约失效和 attempt 重试通过真实远程 Worker 生效，并验证 Worker
    临时文件及未发布 attempt 输出被清理且不被下游消费。
  - [x] 新增真实多进程集成测试：证明任务在 Worker PID 内执行，两个 Worker 均接收
    Task，终止一个 Worker 后由另一 Worker 重试成功；不得以 `LogicalWorker` 模拟替代。
  - [x] 更新架构、实现、验证和答辩文档，删除“查询数据面使用进程内 LogicalWorker”
    的限制声明，并提供可复现命令及机器可读证据。
  - 验证状态（2026-08-31）：真实 Coordinator 与两个 Worker PID 测试通过，
    两个 Worker 均执行 Task；终止 `worker-1` 后首 attempt 为 LOST，
    `worker-2` 重试成功，证据见 `artifacts/task18/task18-results.json`。
  - Compose 使用 MinIO `s3://` URI 完成分区导入、远程 Scan、Shuffle、结果物化
    及 Coordinator 重启后的持久化查询，证据见
    `artifacts/task18/compose-results.json`；容器和网络随后已清理。
  - Task/状态/结果和 Shuffle manifest 使用显式 version 1；Task payload 按
    operation 做 Pydantic 类型校验。可信 Python pickle 计划片段声明
    `python-pickle-v5`/version 1，仅允许 bearer token 认证的内部 Task API 输入。
  - Coordinator/Worker 的 MinIO endpoint、access key、secret、bucket、region
    已通过 Compose 环境变量和 Kubernetes ConfigMap/Secret 同步注入；本机仍可
    使用共享文件目录。`kubectl kustomize` 和部署静态测试通过。
  - 最终门禁：全量非 stress pytest `166 passed, 1 deselected`，Ruff、完整
    Mypy（83 个源码/测试文件）、文档检查及 7 项部署测试全部通过。

- [x] Task 19：修复远程 Worker 错误协议，保证查询 API 可区分资源错误与任务错误。
  - [x] 为 Worker Task 失败状态增加稳定的错误码和结构化上下文；捕获
    `DistributedSQLError` 时保留 `RESOURCE_EXHAUSTED` 等领域错误码，不得只返回
    异常类名与消息。
  - [x] Coordinator/RemoteWorker 解析远程错误并保留领域错误；仅将无领域分类的
    Worker 执行、网络或重试耗尽错误归类为 `TASK_FAILED`。
  - [x] 增加真实 Worker 查询 API 合同测试，分别触发语法、绑定、资源耗尽、任务
    失败和内部错误，断言五类响应码可区分且响应中不包含 traceback/内部堆栈。
  - 验证状态（2026-08-31）：真实 Worker 查询 API 五类错误合同测试通过；
    全量非 stress pytest `169 passed, 1 deselected`，Ruff 与完整 Mypy
    （84 个源码/测试文件）通过。

- [x] Task 20：补全真实远程执行的 Stage/Task 可观测指标，满足验收清单第 42 项。
  - [x] Worker 为每个真实 Task 采集并返回输入/输出行数、输入/输出字节、执行耗时、
    Shuffle 读写和 Spill 指标，不得仅返回 `spill_bytes` 或查询级汇总。
  - [x] Coordinator 从 `RemoteTaskResult.artifact` 和 Worker 指标构建逐 Task 指标，
    并按 Stage 聚合输入/输出、耗时、Shuffle 和 Spill；真实远程结果不得显示
    `output_rows`/`output_bytes` 为 `unknown`。
  - [x] 扩展 `TaskMetrics`、`StageMetrics`、指标 API 和 EXPLAIN ANALYZE 输出，明确
    暴露上述字段并保持字段语义一致。
  - [x] 新增真实 Coordinator/Worker 集成测试，通过查询指标 API 断言每个 Stage/Task
    均具有可审计的输入输出行数、字节、耗时、Shuffle 和 Spill 数据；不得以手工构造
    `ScheduleResult` 或进程内执行器替代。
  - 验证状态（2026-08-31）：真实 Coordinator + 两 Worker 指标 API 集成测试及
    Task 18 远程回归通过；全量非 stress pytest `170 passed, 1 deselected`，
    全仓 Ruff 与完整 Mypy（85 个源码/测试文件）通过。

- [x] Task 21：补全真实远程查询取消的终止与清理语义，满足验收清单第 43 项。
  - [x] Worker Task 取消接口不得在后台执行线程仍运行时提前暴露终态；应提供可等待的
    终止语义，确认算子已响应取消且不会继续写入结果、Shuffle 或 Spill 文件。
  - [x] Coordinator 取消运行中查询后必须停止派发尚未开始的 Stage/Task，并等待所有
    已派发远程 attempt 完成取消或进入有界的明确失败状态后再完成查询取消。
  - [x] 取消路径应清理 Worker 上该查询的未发布结果、Shuffle attempt 输出和 Spill
    临时目录，且不得删除其他查询或其他 attempt 的已发布数据。
  - [x] 新增真实 Coordinator 与至少两个独立 Worker 进程的故障集成测试：在第一 Stage
    运行中取消查询，断言后续 Stage/Task 从未提交、所有 Worker attempt 不再运行、
    查询最终为 CANCELED，并轮询确认结果、Shuffle 与 Spill 临时数据全部清空。
  - 验证状态（2026-08-31）：Worker 有界取消、超时不伪装终态及跨 query/attempt
    清理隔离测试通过；真实 Coordinator + 两个独立 Worker 进程在第一 Stage 运行中
    取消测试通过，后续 Stage 零提交、全部 attempt 为 CANCELED，结果、Shuffle 与
    Spill 临时数据为空。最终门禁：全量非 stress pytest `173 passed, 1 deselected`，
    Ruff 与完整 Mypy（86 个源码/测试文件）通过。

- [x] Task 22：修复重试超时与退避时序验证，完成验收清单第 47 项。
  - [x] 明确 attempt 超时从 Worker 执行实际开始计时，避免健康检查协程调度延迟吞掉
    `attempt_timeout_seconds`，并保证下一 attempt 在超时结束及配置退避后才启动。
  - [x] 修复 `test_retry_timeout_backoff_and_final_error_include_safe_context` 的稳定性，
    可靠断言超时与退避均已发生，同时保留重试耗尽错误中的 query、stage、task、
    attempt、worker、attempt_count、failure_kind 和安全根因语义。
  - [x] 复跑 `tests/test_worker_recovery.py` 及全量非 stress pytest、Ruff、完整 Mypy。
  - 失败证据（2026-08-31）：`tests/test_worker_recovery.py` 为 `1 failed, 1 passed`；
    两次 attempt 实际启动间隔约 `0.031s`，低于测试要求的 `0.045s`。
  - 验证状态（2026-08-31）：Windows 事件循环曾提前唤醒超时等待，使 attempt
    实际执行约 `0.01487s`，低于 `0.02s` 配置；改用单调时钟截止点重复等待后，
    `tests/test_worker_recovery.py` 在全量测试并发负载下连续 10 轮均为
    `2 passed`。最终门禁：全量非 stress pytest `173 passed, 1 deselected`
    （`121.95s`），Ruff 全仓通过，完整 Mypy 检查 86 个源码/测试文件无问题，
    且无残留 Coordinator、Worker 或本地集群服务进程。

- [x] Task 23：补全 Docker Compose Worker 故障恢复端到端验收，完成验收清单第 56 项。
  - [x] 修复 Compose 远程查询中 Worker 失联后的恢复路径，避免取消确认失败以
    `TASK_FAILED` 覆盖 LOST attempt 的健康 Worker 重试。
  - [x] 在 `verify-compose.ps1` 中运行足够长的远程 Shuffle 查询并于执行中终止一个
    Worker，断言失联 attempt 为 LOST、另一 Worker 的 retry 成功且结果正确。
  - [x] 同一验收覆盖 Worker 注册、CSV 导入、远程查询、Shuffle 读写、故障恢复和
    Catalog 持久化，并输出包含 attempt、Worker、Shuffle 和结果摘要的机器可读证据。
  - 失败证据（2026-08-31）：Compose 四服务启动及常规持久化查询通过；随后执行
    `docker kill distributed-sql-worker-1-1` 并立即查询，查询
    `981180d0-74a0-4a04-95ea-fad6ee3a4990` 失败，错误为
    `TASK_FAILED`，`failure_kind=cancellation_confirmation`，未完成 Worker 故障恢复。
  - 验证状态（2026-08-31）：`verify-compose.ps1` 实机通过；先确认
    `worker-1` 的远程 Shuffle write attempt 已为 RUNNING，再执行 `docker kill`，
    同一 attempt 随后记录为 LOST，`worker-2` 的 attempt-001 重试成功。查询返回
    3 行正确结果，Shuffle 写入/读取均为 3 行、1506 字节；Coordinator 重启后的
    Catalog 与查询结果保持不变。机器可读证据位于
    `artifacts/task23/compose-results.json`，容器、网络和卷均已清理。
  - 最终门禁：定向恢复及取消回归 `6 passed`；全量非 stress pytest
    `175 passed, 1 deselected`；Ruff 全仓通过；完整 Mypy 检查 86 个源码/测试
    文件无问题。

- [x] Task 24：补全 K3s 部署、升级和回滚的可审计实机证据，完成验收清单第 58 项。
  - [x] 在临时 K3s 集群实际应用 `deploy/kubernetes`，记录 Kubernetes/K3s 版本、
    三个 Deployment Ready、两个 PVC Bound、Service 和 Pod 状态。
  - [x] 使用不同的不可变 Coordinator/Worker 镜像版本完成初始部署与滚动升级，
    分别记录 Deployment revision、期望镜像、实际 Pod image/imageID 和 rollout
    成功状态，证明升级确实替换了运行镜像。
  - [x] 在初始部署、升级后和 `kubectl rollout undo` 回滚后各执行一次远程查询；
    后两次必须复用初始部署创建的 Catalog 表，并断言分区元数据和查询结果保持。
  - [x] 回滚后断言 Coordinator/Worker 的 Deployment 与实际 Pod 均恢复初始镜像，
    保存 rollout history、PVC、Catalog、查询结果及命令退出状态。
  - [x] 将上述结果输出到版本化、机器可读的 `artifacts/task24/` 产物，并在
    `tasks.md`、部署指南、验证文档中链接该产物；失败时不得写入 `passed` 状态。
  - 实机证据：
    [`k3s-results-v1.json`](../../../artifacts/task24/k3s-results-v1.json)、
    [`cleanup-v1.json`](../../../artifacts/task24/cleanup-v1.json)；部署与验证说明见
    [`deploy/README.md`](../../../deploy/README.md#4-docker-内临时-k3s-可审计验收)和
    [`docs/verification.md`](../../../docs/verification.md#task24-k3s-可审计验收)。
  - 验证状态（2026-08-31）：K3s/Kubernetes `v1.33.5+k3s1`；初始、升级、
    回滚 revision 为 `2/3/4`，三轮远程查询均为 3 行、2 分区；51 条记录命令
    无失败，清理通过。部署定向测试 `8 passed`，非 stress pytest
    `176 passed, 1 deselected`，Ruff、Mypy 和文档检查通过。

- [x] Task 25：补全实际 CI 配置并证明自研引擎不存在 DuckDB/Calcite 执行路径，
  完成验收清单第 61 项。
  - [x] 新增仓库实际使用的 CI 工作流，使用锁定依赖安装，并执行 Ruff、完整 Mypy、
    非 stress pytest 和文档检查；配置文件中的命令必须能在干净环境直接运行。
  - [x] 在 CI 中明确验证快速测试与独立 1 GiB stress 测试分层，stress 不进入每次
    快速门禁，并提供可审计的 JUnit/机器可读产物上传配置。
  - [x] 增加静态扫描和运行时测试，证明 `src/distributed_sql` 生产代码不导入或调用
    DuckDB/Calcite；DuckDB 仅允许出现在测试参考结果路径。
  - 失败证据（2026-08-31）：仓库不存在 `.github/workflows` 或其他 CI 平台配置；
    当前只有本地验证命令和历史产物，不能证明“代码格式、静态检查和测试在 CI 中
    通过”。验收清单第 61 项保持未勾选，并按遇错即停规则停止后续核验。
  - 完成证据（2026-08-31）：新增 GitHub Actions 快速与独立 stress 工作流；
    PyYAML 结构化 workflow/边界测试 `10 passed`；本地等价快速门禁为
    `186 passed, 1 deselected`，Ruff、完整 Mypy（87 个文件）、文档检查、
    Compose config、Kustomize 和生产代码独立性检查均通过。当前未发生或声称
    GitHub Actions 云端 run，远端徽章待公开仓库创建。

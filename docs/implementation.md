# 优化、执行与核心代码

## 1. RBO

[`RuleOptimizer`](../src/distributed_sql/optimizer/optimizer.py) 对每条规则递归访问
计划树，按固定顺序重复执行至不动点。每次有效改写保存 `before/after` 轨迹；
计划指纹重复时以 `cycle_detected` 终止，默认最多 32 轮。

九项规则集中在
[`optimizer/rules.py`](../src/distributed_sql/optimizer/rules.py)：

| 规则 | 实现要点 |
|---|---|
| Predicate / Project | 按投影表达式替换列后下推 |
| Predicate / Aggregate | 只下推仅引用分组键的安全合取项 |
| Predicate / Join | 按列来源下推；保留 Outer Join 被保留侧语义 |
| Limit / Project | 精确下推，保留等价计划 |
| Limit / Join | 仅写入输入上界提示，最终 Limit 保留 |
| Column Pruning | 从根向 Scan 传播需求，保留过滤、连接、排序隐藏列 |
| Constant Folding | 只折叠常量和确定性表达式，保持 NULL 三值逻辑 |
| Predicate Merge | 拆分、规范化、去重并重组 AND 条件 |
| Equality Inference | 对等值条件求传递闭包，不跨不安全 Outer Join 边界 |

`EXPLAIN` 同时输出原计划、优化后计划、命中规则、终止原因和迭代次数。规则形状
及优化前后等价性由
[`tests/test_optimizer.py`](../tests/test_optimizer.py) 覆盖。

## 2. 统计与 CBO

导入阶段由
[`collect_table_statistics`](../src/distributed_sql/catalog/importer.py)
记录表/分区行数、字节数及列级 NULL、NDV、min/max。缺失统计时
[`CostModel`](../src/distributed_sql/optimizer/cost.py) 使用保守默认值并把来源
写入估算理由。

CBO 在 RBO 之后执行：

1. 估算 Scan、Filter、Aggregate、Join 的基数和行宽。
2. 分别累计 CPU、网络、内存、磁盘代价。
3. 比较已有分区复用、Broadcast、左侧 Repartition、右侧 Repartition和双侧
   Repartition，选取满足连接键分布要求的最低代价方案。
4. Hash Join 选择估算较小且可构建的一侧。
5. 将连续、确定性的 Inner Join 展平；用子集动态规划枚举连接顺序。Outer Join
   和非确定性条件是不可跨越边界。

核心入口是
[`CostBasedOptimizer.optimize`](../src/distributed_sql/optimizer/cbo.py)，决策通过
`JoinDecision` 交给物理规划器。估算、缺失统计、策略和重排测试位于
[`tests/test_cbo.py`](../tests/test_cbo.py)。

## 3. Shuffle 与分布式执行

[`materialize_exchanges`](../src/distributed_sql/execution/physical.py) 把 CBO 决策
变成显式 Exchange：

- `REUSE`：两侧分区已兼容，不新增 Exchange。
- `BROADCAST`：构建侧复制到所有目标 partition。
- `REPARTITION_LEFT` / `REPARTITION_RIGHT`：只重分区不满足要求的一侧。
- `REPARTITION_BOTH`：两侧按相同连接键和 partition 数稳定 Hash。
- 全局排序、无分组聚合和全局 Limit 使用 Single Exchange。

`StagePlanner` 在每个 Exchange 处分割 Stage DAG，并为每个 partition 创建 Task。
[`TaskScheduler`](../src/distributed_sql/execution/scheduler.py) 在依赖就绪后按 Worker
槽位调度，支持取消、attempt 超时、指数退避和重试。

生产查询入口使用
[`RemoteDistributedExecutor`](../src/distributed_sql/coordinator/remote_execution.py)。
它把 Scan、单输入算子、Join、Shuffle write/read 分别编码为版本 1 JSON Task，
调用注册 Worker endpoint；Coordinator 不调用 `LogicalWorker.execute`。
[`WorkerTaskManager`](../src/distributed_sql/worker/tasks.py) 在 Worker 进程中反序列化
计划、执行算子并发布带 SHA-256 的 Parquet 结果。`LogicalWorker` 仅保留给原有
隔离单元测试使用，不属于查询服务执行路径。

Task 外壳、结果和状态都只接受协议 version 1；payload 按 operation 使用独立
Pydantic 模型校验并拒绝未知字段。计划片段声明 `python-pickle-v5` 和 version 1。
该格式不是通用反序列化协议：Worker 只在 bearer token 校验通过后接收含计划的
内部 Task，禁止把 Task API 暴露给任意未认证输入。token 与对象存储凭据通过
环境变量或 Kubernetes Secret 注入，不出现在 Task payload。

[`ShuffleStore`](../src/distributed_sql/execution/shuffle.py) 将路径隔离为
`query/stage/task/attempt/partition`。每个 Parquet 文件记录行数、字节数和
SHA-256；全部文件写完后才原子发布 manifest。读取端只接受指定成功 attempt
的 manifest，并再次校验大小、摘要和行数。指标记录读写记录数、字节、分区数
和耗时。

本机多进程可使用共享文件根；Compose/Kubernetes 将 Coordinator 和 Worker
配置为同一 MinIO endpoint、bucket、region 和凭据，Scan partition、Shuffle
文件/manifest 及 Task 结果均使用 `s3://` URI。Worker 和 Coordinator 都通过
`ObjectStoreRouter` 读取 Parquet 字节，不依赖容器私有路径。

可拆分聚合在
[`DistributedExecutor`](../src/distributed_sql/execution/distributed.py) 中执行
partial aggregate、按分组键 Shuffle 和 final aggregate；全局 Limit 通过
Single Exchange 汇总后裁切。

## 4. Runtime Filter

Hash Join 构建侧生成 Bloom Filter 及可用的 min/max filter，通过
`RuntimeFilterChannel` 传递到探测侧 Scan。Bloom Filter 允许假阳性但不能有
假阴性。`runtime_filter_is_safe` 根据 Join 类型和构建侧禁止过滤 Outer Join
的保留侧。扫描指标同时记录过滤前后行数，结果等价测试位于
[`tests/test_runtime_filter.py`](../tests/test_runtime_filter.py)。

## 5. 容错

Worker 注册后周期性续租；Coordinator 的
[`WorkerRegistry`](../src/distributed_sql/coordinator/registry.py) 根据 TTL 标记
失联节点。Scheduler 在执行期间同时等待 attempt 和租约状态：

1. Worker 失联、超时或执行异常时，将 attempt 标记为 LOST/FAILED。
2. 在健康 Worker 上创建新的唯一 attempt，优先避开刚失败的 Worker。
3. 下游只消费调度器选中的成功 attempt manifest，因此旧输出不会造成重复。
4. 超过重试上限后返回包含 query、stage、task、attempt、worker 和根因的错误。

[`tests/test_worker_recovery.py`](../tests/test_worker_recovery.py) 会在首个 attempt
已发布文件但未返回时停止心跳，验证第二个 Worker 重试后结果无重复、无丢失。
真实进程证据由
[`tests/test_task18_remote_workers.py`](../tests/test_task18_remote_workers.py)
生成：两个 Worker PID 都执行 Task，终止首个 Worker 后 attempt 在第二个 Worker
成功重试。调度器对失败 attempt 调用远程 `discard`，Worker 删除结果或 Shuffle
文件和 manifest。

## 6. 内存预算与落盘

[`MemoryAccount`](../src/distributed_sql/execution/memory.py) 提供 query/task 层级
账户，预留操作是线程安全且原子的。默认执行账户预算为 64 MiB。这里的峰值只
覆盖算子显式 charge 的 Python 行对象，并不等于包含 Python、PyArrow、扫描输入、
结果表和分配器保留内存的进程 RSS。

主要外部算法位于
[`execution/operators.py`](../src/distributed_sql/execution/operators.py)：

- **外部归并排序**：内存 run 达阈值后写 Parquet，最终多路归并。
- **分区 Hash Join**：构建侧超预算时按连接键分区落盘；单分区仍过大时可回退
  Sort-Merge Join。
- **Sort Aggregate**：高基数组合在内存达到阈值时写有序 run，再归并聚合。

`TempFileManager` 为每个 attempt 创建目录，并在成功、异常、取消和磁盘不足时
清理。`ENOSPC` 转换为带上下文的 `RESOURCE_EXHAUSTED`。Task16 对 1 GiB 数据的
实际落盘指标见[验证报告](verification.md#task16-压力验收)。

## 7. Catalog 与扫描

Catalog 由 SQLite 持久化 namespace、表、schema、格式、位置、分区和统计。
[`DataImporter`](../src/distributed_sql/catalog/importer.py) 支持按键 Hash 或
轮询分区，先写不可变文件和 manifest，再更新 Catalog。

[`DataSource`](../src/distributed_sql/data_source/base.py) 是统一扫描接口，
`ScanRequest` 可携带投影、谓词、批大小和文件任务。CSV、Parquet、Avro、ORC
适配器输出 PyArrow `RecordBatch`；Iceberg 适配器通过 PyIceberg 读取当前快照、
schema、manifest 和数据文件。Planner 与上层算子只依赖统一接口。

## 8. 可观测性与查询顾问

查询完成后汇总逻辑/物理计划、RBO 轨迹、估算质量、Stage/Task 状态、Shuffle、
Spill、Runtime Filter、重试事件和结构化日志。AI4DB 顾问是确定性规则系统，
仅在证据满足阈值时输出严重度、量化证据、原因、动作和预期影响；没有证据时
明确返回“无高置信度建议”，不调用外部模型。

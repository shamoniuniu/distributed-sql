# 分布式 SQL 计算系统规格

## Why

从零实现一个课程级、可部署、可验证的分布式 SQL 计算系统，完整覆盖原始作业中的基础、进阶和探索性评分项。系统以正确性、架构可解释性和可复现实验为优先目标，不以生产级吞吐量或完整 SQL 标准兼容为目标。

## What Changes

- 使用 Python 3.12、FastAPI、SQLGlot、PyArrow 构建 Coordinator/Worker 分布式架构。
- 提供 WebUI、CLI、REST 查询接口及 REST Catalog 管理接口。
- 支持 CSV、Parquet、Avro、ORC 文件表和 PyIceberg + MinIO 的 Iceberg 表。
- 支持课程型 SELECT SQL 子集、四类 Join、高级聚合、窗口函数和 GROUPING SETS。
- 自研逻辑计划、RBO、统计信息、CBO、物理计划和分布式 Stage/Task 调度，不使用 Apache Calcite 优化器。
- 支持分区数据导入、三种 Repartition Shuffle、Runtime Filter、Worker 故障重试。
- 支持 64 MB Worker 内存预算下对 1 GB 数据执行外部排序、分区 Hash Join 和 Sort Aggregate。
- 提供规则化 AI4DB 查询顾问，不依赖外部大模型或 API 密钥。
- 提供单元测试、集成测试、故障测试、数据生成器、Docker Compose、Kubernetes 清单、使用文档、架构图和答辩材料。

## Impact

- Affected specs: SQL 解析、Catalog、逻辑计划、RBO、CBO、执行引擎、Shuffle、调度容错、数据源、WebUI、部署、测试和课程文档。
- Affected code: 新建完整 Python 工程、容器和 Kubernetes 配置；当前仓库没有既有代码。
- External services: Docker Compose/Kubernetes 环境中的 MinIO；SQLite 持久化系统 Catalog 和 Iceberg 元数据。

## Constraints

- 主体运行时为 Python 3.12。
- 数据批次统一使用 PyArrow `RecordBatch`；控制面使用 HTTP/JSON RPC；大数据面通过不可变文件和清单交换。
- SQL 解析使用 SQLGlot，但关系代数、优化规则、代价模型和执行计划必须自行实现。
- 本机多进程、Docker Compose 和通用 Kubernetes 集群必须采用同一套服务代码。
- 默认开发环境为 Windows；容器内为 Linux。
- 所有功能必须有确定性测试；1 GB 压力验收可标记为独立慢速测试，不进入每次快速单测。

## Out of Scope

- Coordinator 主备、Raft 共识和跨 Coordinator 查询状态恢复。
- 生产级安全认证、租户隔离、资源队列、审计和加密密钥管理。
- MySQL/PostgreSQL Wire Protocol、DML、事务和并发写入。
- 完整 ANSI SQL、CTE、递归查询、UNION、任意相关子查询和用户自定义函数。
- 对任意规模、任意数据倾斜或多节点同时故障作无条件成功保证。
- 外部大模型调用、自然语言转 SQL 和模型训练算子。

## ADDED Requirements

### Requirement: 工程与服务架构

系统 SHALL 提供单仓库 Python 工程，至少划分 `common`、`coordinator`、`worker`、`catalog`、`planner`、`optimizer`、`execution`、`cli` 和 `web` 模块。Coordinator SHALL 负责 SQL 生命周期、元数据、优化、Stage 切分和任务调度；Worker SHALL 负责扫描、算子执行、Shuffle 和结果物化。

#### Scenario: 本机分布式启动
- **WHEN** 用户执行一键本机启动命令
- **THEN** 系统启动一个 Coordinator 和至少两个 Worker
- **THEN** Worker 通过注册和心跳进入可调度状态

#### Scenario: 服务健康检查
- **WHEN** 用户或编排系统访问 Coordinator、Worker 健康端点
- **THEN** 端点返回进程状态、版本和关键依赖状态

### Requirement: 用户入口

系统 SHALL 提供 REST API、CLI 和可用的 WebUI。WebUI SHALL 直接呈现查询编辑、执行、取消、结果、执行计划、运行指标、节点状态和 Catalog 管理，不提供营销式落地页。

#### Scenario: 提交查询
- **WHEN** 用户通过 WebUI、CLI 或 REST 提交合法 SQL
- **THEN** 系统返回查询 ID，并允许查看状态、结果、逻辑计划、物理计划和运行指标

#### Scenario: 取消查询
- **WHEN** 用户取消正在运行的查询
- **THEN** Coordinator 停止派发新任务并通知 Worker 终止相关任务

#### Scenario: 非法查询
- **WHEN** SQL 语法错误、引用不存在对象或超出支持范围
- **THEN** 系统返回带错误类别和位置/对象信息的明确错误，不泄漏内部堆栈

### Requirement: Catalog 与数据导入

系统 SHALL 提供持久化 Catalog 接口和 REST CRUD，记录 namespace、表、列、类型、格式、位置、分区方式和统计信息。系统 SHALL 支持将源文件按指定键或轮询方式划分为多个不可变 partition，并写入本地共享目录或 MinIO。

#### Scenario: 注册文件表
- **WHEN** 用户提供 CSV、Parquet、Avro 或 ORC 的 schema、位置和格式参数
- **THEN** Catalog 持久化表定义并可被后续 SQL 查询

#### Scenario: 导入并分区
- **WHEN** 用户导入数据并指定分区数和可选分区键
- **THEN** 系统生成分区文件、清单和行数统计，并将位置登记到 Catalog

#### Scenario: 重启保持元数据
- **WHEN** Coordinator 重启并复用 Catalog 存储卷
- **THEN** 已注册表和统计信息仍可读取

### Requirement: 多格式与 Iceberg 数据源

系统 SHALL 读取 CSV、Parquet、Avro 和 ORC；SHALL 通过 PyIceberg 读取 MinIO 上 Iceberg 表的当前快照、schema 和数据文件。格式适配器 SHALL 通过统一扫描接口向执行引擎输出 PyArrow 批次。

#### Scenario: 同构查询
- **WHEN** 四种文件格式保存相同 schema 和数据
- **THEN** 同一 SQL 在四张表上返回等价结果

#### Scenario: Iceberg 快照查询
- **WHEN** Catalog 注册有效 Iceberg 表
- **THEN** Scan 根据当前快照计划文件任务，并返回正确结果

#### Scenario: 格式扩展
- **WHEN** 新增实现统一扫描接口的适配器
- **THEN** Planner 和上层算子无需修改即可查询新格式

### Requirement: SQL 语义和逻辑计划

系统 SHALL 支持 `SELECT`、`FROM`、别名、`INNER/LEFT/RIGHT/FULL OUTER JOIN`、`WHERE`、`GROUP BY`、`HAVING`、`ORDER BY`、`LIMIT` 和 `GROUPING SETS`。表达式 SHALL 支持列引用、字面量、算术、比较、布尔、`IS NULL`、`IN`、`BETWEEN`、`LIKE`、`CASE`、常用标量函数及 SQL NULL 三值逻辑。聚合 SHALL 至少支持 `COUNT`、`SUM`、`AVG`、`MIN`、`MAX` 和 `COUNT DISTINCT`。窗口 SHALL 至少支持 `ROW_NUMBER`、`RANK`、`DENSE_RANK` 及 `SUM/AVG/MIN/MAX/COUNT OVER` 的 `PARTITION BY`、`ORDER BY` 和常用 ROWS frame。

#### Scenario: 基础算子组合
- **WHEN** 查询组合 Scan、Filter、Project、Aggregate、Join、Order 和 Limit
- **THEN** 逻辑计划包含对应节点且结果符合 SQL 语义

#### Scenario: 四类 Join
- **WHEN** 输入包含匹配行、不匹配行和 NULL Join Key
- **THEN** 四类 Join 的输出与参考引擎 DuckDB 一致

#### Scenario: 窗口与分组集
- **WHEN** 查询使用受支持窗口函数或 GROUPING SETS
- **THEN** 系统按分区、排序、frame 和分组集合返回正确结果

### Requirement: RBO

系统 SHALL 使用可重复执行至不动点的规则框架，实现：

- Predicate 下推穿过 Project。
- Predicate 下推穿过 Aggregate，仅下推只引用分组键且语义安全的部分。
- Predicate 按列归属和 Outer Join 语义安全地下推到 Join 输入。
- Limit 下推穿过 Project。
- Limit 作为 Join 输入执行上界提示下推，且最终 Limit 保留以保证语义；仅在不会改变结果时做精确下推。
- 列裁剪覆盖 Scan 和中间节点，并保留 Join、Filter、Aggregate、Order 所需隐藏列。
- 常量折叠遵循 NULL 和确定性函数语义。
- 谓词合并与规范化。
- 由等值条件推导安全传递闭包，并避免跨 Outer Join 错误传播。

#### Scenario: 优化等价
- **WHEN** 任一 RBO 规则改写计划
- **THEN** 优化前后结果在覆盖正常值和 NULL 的测试数据上等价

#### Scenario: 查看规则效果
- **WHEN** 用户请求 EXPLAIN
- **THEN** 系统展示优化前后计划及每条命中规则

### Requirement: 统计信息与 CBO

系统 SHALL 收集或估算表/分区行数、字节数、列 NULL 数、NDV、最小值和最大值。CBO SHALL 基于统计信息估算过滤、聚合和 Join 基数及 CPU、网络、内存、磁盘代价；SHALL 选择 Hash Join 构建侧、Shuffle 策略和多表 Inner Join 顺序。

#### Scenario: Hash Join 构建侧
- **WHEN** 两侧统计规模明显不同且均可构建
- **THEN** 物理计划选择估算较小的一侧构建 Hash Table

#### Scenario: Shuffle 策略
- **WHEN** 输入分区属性和规模分别适合复用、单侧 Repartition、双侧 Repartition 或广播
- **THEN** CBO 选择最低估算代价且满足 Join 分区要求的策略

#### Scenario: Join Reorder
- **WHEN** 查询包含三个及以上可交换的 Inner Join
- **THEN** 系统使用动态规划枚举合法连接子集并选择最低代价顺序
- **THEN** Outer Join 和非确定性条件构成不可跨越的重排边界

#### Scenario: 缺失统计
- **WHEN** 某些统计缺失
- **THEN** CBO 使用有记录的保守默认值并在 EXPLAIN 中标明估算来源

### Requirement: 分布式计划与 Shuffle

系统 SHALL 在 Exchange 处分割 Stage，将 Stage 切分为可重试 Task，并以 partition 为最小调度单位。系统 SHALL 支持 Join 左侧 Repartition、右侧 Repartition、双侧 Repartition，以及广播和已有分区复用。Shuffle 文件 SHALL 按 query/stage/task/attempt/partition 隔离，并通过完成清单原子发布。

#### Scenario: 双侧 Repartition Join
- **WHEN** 两侧均未按 Join Key 分区且无法广播
- **THEN** 两侧按兼容哈希和相同 partition 数重分区，相同 Key 被发送到同一目标 partition

#### Scenario: 单侧 Repartition
- **WHEN** 一侧已满足目标分区属性
- **THEN** 系统只重分区另一侧并复用已有分区

#### Scenario: Shuffle 可观测
- **WHEN** 查询完成
- **THEN** 指标包含读写字节、记录数、分区数、耗时和溢写量

### Requirement: 分布式执行算子

Worker SHALL 以 PyArrow 批次执行 Scan、Project、Filter、Hash/Sort Aggregate、Hash/Sort-Merge Join、Limit、Order、Window 和 Grouping Sets。所有算子 SHALL 接受取消信号并遵守查询资源预算。

#### Scenario: 分布式聚合
- **WHEN** Aggregate 可拆分
- **THEN** 系统执行 partial aggregate、按分组键 Shuffle 和 final aggregate

#### Scenario: 全局 Limit
- **WHEN** 多个 partition 并行产生结果
- **THEN** Coordinator 返回不超过 Limit 的确定数量记录并取消多余工作

#### Scenario: Outer Join
- **WHEN** 分布式 Join 存在未匹配行
- **THEN** 系统按 Join 类型补齐 NULL 并仅输出一次未匹配行

### Requirement: Runtime Filter

系统 SHALL 从 Join 构建侧生成 Bloom Filter 和可用的 min/max filter，将其分发至探测侧 Scan；Runtime Filter SHALL 只用于排除确定不匹配数据，不得改变 Outer Join 语义。

#### Scenario: Filter 生效
- **WHEN** Inner/Semi 等安全 Join 的构建侧完成
- **THEN** 探测侧 Scan 指标显示过滤前后记录数且查询结果不变

#### Scenario: Filter 不安全
- **WHEN** Join 类型或保留侧语义不允许过滤
- **THEN** Planner 禁止向该输入应用 Runtime Filter

### Requirement: 内存预算与落盘

每个 Worker SHALL 提供可配置内存预算，默认课程压力验收值为 64 MB。执行引擎 SHALL 在内存阈值前触发外部归并排序、分区 Hash Join/Sort-Merge Join 和 Sort Aggregate，临时文件 SHALL 在成功、失败或取消后清理。

#### Scenario: 1 GB 有限内存查询
- **WHEN** 至少 1 GB 生成数据在每 Worker 64 MB 执行预算下运行排序、Join 和聚合查询
- **THEN** 查询成功且结果与 DuckDB 参考结果一致
- **THEN** 指标证明发生落盘且受控内存峰值不超过预算加明确记录的运行时余量

#### Scenario: 磁盘空间不足
- **WHEN** 临时目录无法继续写入
- **THEN** 任务以资源耗尽错误失败，Coordinator 按策略重试或终止，且已创建临时文件被清理

### Requirement: Worker 故障恢复

Coordinator SHALL 使用租约心跳识别失联 Worker。Task SHALL 具有唯一 attempt；失败或失联 Task SHALL 在健康 Worker 上重试。输入和已发布 Shuffle 输出不可变，未原子发布的 attempt 输出不得被消费。只要存在足够健康节点且未超过重试/超时上限，单 Worker 故障 SHALL 不导致查询失败。

#### Scenario: 查询中 Worker 被终止
- **WHEN** 多 Worker 查询期间终止一个 Worker
- **THEN** Coordinator 在租约超时后重调度受影响 Task
- **THEN** 查询成功且结果无重复、无丢失

#### Scenario: 重试耗尽
- **WHEN** Task 连续失败超过可配置上限
- **THEN** 查询以包含 Stage、Task、attempt 和根因的错误结束

### Requirement: AI4DB 查询顾问

系统 SHALL 基于 SQL、优化轨迹、统计质量、物理计划和运行指标生成确定性的查询建议。建议 SHALL 包含严重度、证据、原因、可执行动作和预期影响，不调用外部模型。

#### Scenario: 缺失统计建议
- **WHEN** 低质量估算影响 Join 决策
- **THEN** 顾问指出具体表/列缺失的统计并建议执行分析

#### Scenario: Shuffle 或 Spill 建议
- **WHEN** 指标显示高 Shuffle、数据倾斜或频繁落盘
- **THEN** 顾问引用量化指标并给出分区、过滤、内存或 Join 策略建议

#### Scenario: 无充分证据
- **WHEN** 查询计划与指标没有命中可靠规则
- **THEN** 顾问返回“暂无高置信度建议”，不捏造结论

### Requirement: 可观测性

系统 SHALL 提供结构化日志、查询/Stage/Task 状态、关键运行指标和 EXPLAIN/EXPLAIN ANALYZE。日志和指标 SHALL 关联 query、stage、task、attempt 和 worker ID。

#### Scenario: 查询诊断
- **WHEN** 用户打开已完成或失败查询
- **THEN** 可查看时间线、计划、各阶段指标、重试记录和错误根因

### Requirement: 部署与升级回滚

系统 SHALL 提供可重复构建的 Coordinator/Worker 镜像、Docker Compose 本机集群和通用 Kubernetes 清单。Kubernetes 部署 SHALL 使用 Deployment/StatefulSet、Service、ConfigMap/Secret、PVC、startup/readiness/liveness probe 和滚动更新策略；文档 SHALL 给出镜像版本升级与 `rollout undo` 回滚验证。

#### Scenario: Compose 一键启动
- **WHEN** 用户执行文档中的 Compose 命令
- **THEN** Coordinator、两个以上 Worker、MinIO 和持久化 Catalog 达到健康状态并可运行示例查询

#### Scenario: Kubernetes 部署
- **WHEN** 用户将清单应用到可用 Kubernetes 集群
- **THEN** 系统完成服务发现、持久化挂载和健康探测，并通过冒烟查询

#### Scenario: 升级回滚
- **WHEN** 用户更新镜像版本后执行 Kubernetes 回滚
- **THEN** Deployment 恢复上一可用版本，持久化 Catalog 数据不丢失

### Requirement: 测试与课程交付材料

系统 SHALL 提供单元、属性/差分、集成、故障、容器、Kubernetes 冒烟和慢速压力测试。SQL 正确性 SHALL 以 DuckDB 作为测试参考，不作为系统查询执行或优化实现。交付材料 SHALL 包含快速开始、SQL 支持矩阵、架构和运行流程图、模块说明、分布式思想、核心代码说明、测试结果、项目分工占位和仓库地址占位。

#### Scenario: 快速验证
- **WHEN** 开发者在安装依赖后运行快速测试命令
- **THEN** 单元和小规模集成测试自动完成且不依赖外部云服务

#### Scenario: 完整验收
- **WHEN** 执行完整验收脚本
- **THEN** 生成机器可读测试结果、关键性能/资源指标和答辩可引用摘要

## MODIFIED Requirements

无。当前仓库没有既有系统规格。

## REMOVED Requirements

无。

## Acceptance Definition

只有在以下条件全部满足时，项目才视为完成：

1. 原作业评分表中的每个基础、进阶和探索项均能映射到实现、自动化测试和文档证据。
2. 支持范围内的 SQL 正确性测试与 DuckDB 参考结果一致。
3. 本机多进程和 Docker Compose 均能完成端到端查询及单 Worker 故障恢复。
4. Kubernetes 清单通过静态校验，并在可用集群中完成部署、查询、升级和回滚冒烟测试。
5. 独立慢速测试证明 1 GB 数据、每 Worker 64 MB 执行预算下的落盘查询成功。
6. 不存在使用 DuckDB、Calcite 或其他完整查询引擎替代自研规划、优化或执行的实现。
7. 使用文档和答辩材料能够从代码、测试产物和指标中复现所有声明。

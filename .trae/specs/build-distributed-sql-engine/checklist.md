# 验收清单

## 架构与入口

- [x] Python 3.12 工程可从干净环境安装，依赖版本锁定，快速测试命令可执行。
- [x] 本机一键启动一个 Coordinator 和至少两个 Worker，注册、心跳、健康检查正常。
- [x] Coordinator/Worker 职责分离，计划与协议可序列化，模块边界符合规格。
- [x] REST、CLI、WebUI 均可完成提交、查看、取消查询及查看结果。
- [x] WebUI 可查看逻辑/物理计划、运行指标、节点状态并管理 Catalog。
- [x] 错误响应能区分语法、绑定、资源、任务和内部错误，且不暴露内部堆栈。

## Catalog 与数据源

- [x] Catalog 支持 namespace/table CRUD、schema、格式、位置、分区和统计信息持久化。
- [x] Coordinator 重启后 Catalog 元数据保持完整。
- [x] 数据导入支持按键 Hash 和轮询分区，并原子发布 partition 清单。
- [x] CSV、Parquet、Avro、ORC 同构数据查询结果一致。
- [x] PyIceberg 能读取 MinIO 上 Iceberg 当前快照并正确计划数据文件。
- [x] 新格式可通过统一扫描接口扩展，不要求修改 Planner 和上层算子。

## SQL 与基础算子

- [x] 支持 SELECT/FROM/WHERE/GROUP BY/HAVING/ORDER BY/LIMIT 和别名绑定。
- [x] 支持 Project、Filter、Aggregate、Limit 及常用表达式和聚合函数。
- [x] INNER、LEFT、RIGHT、FULL OUTER JOIN 在匹配、不匹配、重复键和 NULL 键下与 DuckDB 一致。
- [x] 支持 SQL NULL 三值逻辑、类型推导、必要隐式转换和明确的歧义列错误。
- [x] 支持 ROW_NUMBER、RANK、DENSE_RANK 和聚合窗口函数的指定 partition/order/frame。
- [x] 支持 GROUPING SETS 和 COUNT DISTINCT，结果与 DuckDB 一致。
- [x] 超出规格的 SQL 被明确拒绝，不发生静默错误解释。

## RBO 基础评分项

- [x] Predicate 可安全下推到 Project 下方，且结果等价。
- [x] Predicate 中仅引用分组键的安全部分可下推到 Aggregate 下方。
- [x] Predicate 按输入列归属和 Join 类型安全下推，不破坏 Outer Join 语义。
- [x] Limit 可下推到 Project 下方。
- [x] Limit 对 Join 实现安全的精确下推或执行上界提示，并保留最终 Limit。
- [x] 列裁剪覆盖 Scan 和中间节点，同时保留算子所需隐藏列。
- [x] 常量折叠遵循 NULL、异常和确定性函数语义。
- [x] 谓词合并可规范化重复/分离条件且保持结果等价。
- [x] 等值传递闭包不跨越不安全 Outer Join 边界。
- [x] EXPLAIN 展示优化前后计划、命中规则和不动点终止，规则无无限循环。

## CBO 与进阶评分项

- [x] 统计信息包含行数、字节数、NULL、NDV、min/max，并能标识估算或默认来源。
- [x] Hash Join 在统计充分时选择估算较小侧构建。
- [x] CBO 可在已有分区复用、广播、左侧、右侧、双侧 Repartition 中选择合法最低代价方案。
- [x] 三表及以上 Inner Join 使用动态规划重排，Outer Join 保持语义边界。
- [x] 代价和基数估算可通过 EXPLAIN 检查并有确定性测试。
- [x] Runtime Bloom/min-max Filter 无假阴性、可下推到安全 Scan，并禁止不安全 Outer Join 过滤。
- [x] ORDER BY、HAVING、窗口函数和 GROUPING SETS 均有自动化正确性证据。

## 分布式执行与 Shuffle

- [x] Exchange 正确切分 Stage DAG，Stage 依赖和 Task partition 状态可观察。
- [x] 支持 Join 左侧 Repartition、右侧 Repartition、双侧 Repartition、广播和已有分区复用。
- [x] 相同 Join Key 使用兼容 Hash 到达同一目标 partition。
- [x] Shuffle 输出按 query/stage/task/attempt/partition 隔离并通过原子清单发布。
- [x] 分布式 partial/final Aggregate、Join 和全局 Limit 结果正确。
- [x] 指标包含各 Stage/Task 的输入输出行数、字节、耗时、Shuffle 和 Spill。
- [x] 运行中取消查询会停止新调度并清理 Worker 工作和临时数据。

## 容错、内存与探索项

- [x] Coordinator 能通过租约发现失联 Worker 并重调度受影响 Task。
- [x] 查询中终止一个 Worker 后，在有健康容量且未耗尽重试时查询成功。
- [x] 故障恢复结果无重复、无丢失，未发布 attempt 输出不会被消费。
- [x] 重试耗尽后错误包含 query/stage/task/attempt 和根因。
- [x] Worker 执行内存预算可配置为 64 MB，并有可审计的内存账户。
- [x] 外部归并排序、分区 Hash/Sort-Merge Join、Sort Aggregate 均能触发 Spill。
- [x] 成功、失败、取消和磁盘不足后临时文件均被清理。
- [x] 至少 1 GB 数据在每 Worker 64 MB 执行预算下完成排序、Join、聚合，结果与 DuckDB 一致。
- [x] AI4DB 顾问能基于量化证据输出缺失统计、Shuffle、倾斜、Spill 或 Join 策略建议。
- [x] AI4DB 顾问在无充分证据时明确返回无高置信度建议，不捏造结论。

## 部署与工程质量

- [x] Coordinator/Worker 镜像可重复构建、以非 root 运行并包含健康检查。
- [x] Docker Compose 可一键启动 Coordinator、至少两个 Worker、MinIO 和持久化 Catalog。
- [x] Compose 集群通过注册表、导入、查询、Shuffle、故障恢复端到端冒烟测试。
- [x] Kubernetes 清单包含工作负载、Service、配置、Secret、PVC、资源约束和三类探针。
- [x] Kubernetes 环境通过部署、查询、滚动升级和回滚冒烟测试，Catalog 数据保持。
- [x] 快速单元测试、小规模差分测试、分布式集成测试和故障测试可分层执行。
- [x] 慢速 1 GB 验收输出机器可读结果、资源峰值、Shuffle/Spill 指标和摘要。
- [x] CI 工作流配置真实可执行且本地等价门禁通过；尚无远端 GitHub run；无
  DuckDB/Calcite 替代自研引擎的路径。

## 文档与评分证据

- [x] README 包含快速开始、配置、SQL 支持矩阵、示例查询和已知限制。
- [x] 文档包含架构图、模块交互图、查询流程图和各图文字说明。
- [x] 文档说明 RBO、CBO、Shuffle、分布式调度、容错、内存落盘和核心代码。
- [x] 每个原作业评分项均映射到实现位置、测试用例和可复现命令。
- [x] 答辩文档包含项目介绍、系统设计、功能验证、测试结果、分布式思想、项目分工占位和仓库地址占位。
- [x] 所有性能、正确性和高可用声明均可追溯到实际测试产物，不使用未经验证的数字。

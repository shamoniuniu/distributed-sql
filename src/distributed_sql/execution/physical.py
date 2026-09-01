"""Physical exchanges and deterministic Stage DAG construction."""

from __future__ import annotations

from dataclasses import dataclass, replace

from distributed_sql.common.protocol import (
    Partition,
    PartitionStrategy,
    PlanNode,
    PlanNodeType,
    Schema,
    Stage,
    Task,
)
from distributed_sql.optimizer import JoinDecision, JoinStrategy
from distributed_sql.planner.expressions import Column, Expression
from distributed_sql.planner.logical import (
    Aggregate,
    Filter,
    GroupingSets,
    Join,
    Limit,
    LogicalPlan,
    Order,
    Project,
    Scan,
    Window,
)
from distributed_sql.planner.types import TypeInfo


@dataclass(frozen=True, slots=True)
class Exchange:
    """A materialization boundary with an explicit target distribution."""

    node_id: str
    input: PhysicalPlan
    strategy: PartitionStrategy
    keys: tuple[Expression, ...]
    partition_count: int
    output_schema: Schema

    def __post_init__(self) -> None:
        if self.partition_count < 1:
            raise ValueError("exchange partition_count must be positive")
        if self.strategy is PartitionStrategy.HASH and not self.keys:
            raise ValueError("hash exchange requires partition keys")

    @property
    def children(self) -> tuple[PhysicalPlan, ...]:
        return (self.input,)

    def to_protocol(self) -> PlanNode:
        return PlanNode(
            node_id=self.node_id,
            node_type=PlanNodeType.EXCHANGE,
            output_schema=self.output_schema,
            children=[self.input.to_protocol()],
            properties={
                "strategy": self.strategy.value,
                "keys": [key.sql() for key in self.keys],
                "partition_count": self.partition_count,
            },
        )


type PhysicalPlan = LogicalPlan | Exchange


@dataclass(frozen=True, slots=True)
class StageGraph:
    root_stage_id: str
    stages: tuple[Stage, ...]
    tasks: tuple[Task, ...]

    def stage(self, stage_id: str) -> Stage:
        return next(stage for stage in self.stages if stage.stage_id == stage_id)


def materialize_exchanges(
    plan: LogicalPlan,
    decisions: tuple[JoinDecision, ...],
    *,
    partition_count: int,
) -> PhysicalPlan:
    """Apply CBO Join decisions and mandatory aggregate/limit exchanges."""

    if partition_count < 1:
        raise ValueError("partition_count must be positive")
    by_node = {decision.node_id: decision for decision in decisions}

    def visit(node: LogicalPlan) -> PhysicalPlan:
        if isinstance(node, Scan):
            return node
        children = tuple(visit(child) for child in node.children)
        rewritten = _replace_children(node, children)
        if isinstance(rewritten, Join):
            decision = by_node.get(rewritten.node_id)
            if decision is None:
                raise ValueError(f"Missing JoinDecision for {rewritten.node_id!r}")
            left: PhysicalPlan = rewritten.left
            right: PhysicalPlan = rewritten.right
            left_keys = _columns(decision.left_keys)
            right_keys = _columns(decision.right_keys)
            if decision.strategy in {
                JoinStrategy.REPARTITION_LEFT,
                JoinStrategy.REPARTITION_BOTH,
            }:
                left = _exchange(left, left_keys, PartitionStrategy.HASH, partition_count, "left")
            if decision.strategy in {
                JoinStrategy.REPARTITION_RIGHT,
                JoinStrategy.REPARTITION_BOTH,
            }:
                right = _exchange(
                    right, right_keys, PartitionStrategy.HASH, partition_count, "right"
                )
            if decision.strategy is JoinStrategy.BROADCAST:
                if decision.build_side == "left":
                    left = _exchange(
                        left, (), PartitionStrategy.BROADCAST, partition_count, "left"
                    )
                else:
                    right = _exchange(
                        right, (), PartitionStrategy.BROADCAST, partition_count, "right"
                    )
            return replace(
                rewritten,
                left=left,
                right=right,
                build_side=decision.build_side,
            )
        if isinstance(rewritten, Aggregate):
            strategy = PartitionStrategy.HASH if rewritten.group_by else PartitionStrategy.SINGLE
            count = partition_count if rewritten.group_by else 1
            return replace(
                rewritten,
                input=_exchange(rewritten.input, rewritten.group_by, strategy, count, "aggregate"),
            )
        if isinstance(rewritten, GroupingSets | Order):
            return replace(
                rewritten,
                input=_exchange(rewritten.input, (), PartitionStrategy.SINGLE, 1, "global"),
            )
        if isinstance(rewritten, Window):
            keys = _window_partition_keys(rewritten)
            strategy = PartitionStrategy.HASH if keys else PartitionStrategy.SINGLE
            count = partition_count if keys else 1
            return replace(
                rewritten,
                input=_exchange(rewritten.input, keys, strategy, count, "window"),
            )
        if isinstance(rewritten, Limit):
            return replace(
                rewritten,
                input=_exchange(rewritten.input, (), PartitionStrategy.SINGLE, 1, "limit"),
            )
        return rewritten

    return visit(plan)


def _window_partition_keys(plan: Window) -> tuple[Expression, ...]:
    if not plan.expressions:
        return ()
    first = plan.expressions[0].expression.partition_by
    signature = tuple(expression.sql() for expression in first)
    if not signature:
        return ()
    if all(
        tuple(expression.sql() for expression in item.expression.partition_by) == signature
        for item in plan.expressions[1:]
    ):
        return first
    return ()


class StagePlanner:
    """Split a physical tree at every Exchange and create partition Tasks."""

    def __init__(self, query_id: str) -> None:
        self.query_id = query_id
        self._stages: list[Stage] = []
        self._tasks: list[Task] = []
        self._next_stage = 0

    def plan(self, plan: PhysicalPlan, *, root_partition_count: int = 1) -> StageGraph:
        self._stages.clear()
        self._tasks.clear()
        self._next_stage = 0
        root_stage_id = self._split(plan, root_partition_count)
        return StageGraph(root_stage_id, tuple(self._stages), tuple(self._tasks))

    def _split(self, plan: PhysicalPlan, partition_count: int) -> str:
        stage_id = f"{self.query_id}-stage-{self._next_stage:03d}"
        self._next_stage += 1
        dependencies: list[str] = []

        def fragment(node: PhysicalPlan) -> PlanNode:
            if isinstance(node, Exchange):
                source_stage = self._split(node.input, node.partition_count)
                dependencies.append(source_stage)
                protocol = node.to_protocol().model_copy(deep=True)
                protocol.children = []
                protocol.properties["source_stage_id"] = source_stage
                return protocol
            protocol = node.to_protocol().model_copy(deep=True)
            protocol.children = [fragment(child) for child in node.children]
            return protocol

        stage_plan = fragment(plan)
        stage = Stage(
            stage_id=stage_id,
            query_id=self.query_id,
            plan=stage_plan,
            dependency_stage_ids=dependencies,
            partition_count=partition_count,
        )
        self._stages.append(stage)
        for ordinal in range(partition_count):
            partition = Partition(
                partition_id=f"{stage_id}-partition-{ordinal:05d}",
                ordinal=ordinal,
                location="",
                strategy=PartitionStrategy.UNKNOWN,
            )
            self._tasks.append(
                Task(
                    task_id=f"{stage_id}-task-{ordinal:05d}",
                    query_id=self.query_id,
                    stage_id=stage_id,
                    partition=partition,
                )
            )
        return stage_id


def _exchange(
    plan: PhysicalPlan,
    keys: tuple[Expression, ...],
    strategy: PartitionStrategy,
    partition_count: int,
    suffix: str,
) -> Exchange:
    return Exchange(
        node_id=f"exchange_{plan.node_id}_{suffix}",
        input=plan,
        strategy=strategy,
        keys=keys,
        partition_count=partition_count,
        output_schema=plan.output_schema,
    )


def _columns(names: tuple[str, ...]) -> tuple[Expression, ...]:
    result: list[Expression] = []
    for name in names:
        source, _, column = name.partition(".")
        result.append(Column(column or source, source if column else "", _unknown_type()))
    return tuple(result)


def _unknown_type() -> TypeInfo:
    from distributed_sql.common.protocol import DataType

    return TypeInfo(DataType.NULL)


def _replace_children(
    plan: LogicalPlan,
    children: tuple[PhysicalPlan, ...],
) -> LogicalPlan:
    if isinstance(plan, Project | Filter | Aggregate | Limit | Order | Window | GroupingSets):
        return replace(plan, input=children[0])
    if isinstance(plan, Join):
        return replace(plan, left=children[0], right=children[1])
    return plan

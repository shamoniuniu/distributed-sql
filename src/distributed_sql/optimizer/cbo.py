"""Cost-based optimization layered on top of the fixed-point rule optimizer."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from itertools import combinations

from distributed_sql.catalog.models import CatalogTable
from distributed_sql.common.protocol import Schema, SchemaField
from distributed_sql.planner.expressions import Expression, ScalarFunction
from distributed_sql.planner.logical import Join, LogicalPlan, Scan

from .cost import CostModel, JoinDecision, PlanEstimate
from .optimizer import OptimizationResult, RuleOptimizer
from .utils import (
    combine_conjuncts,
    expression_sources,
    plan_sources,
    plan_text,
    replace_children,
    split_conjuncts,
    walk_expression,
)

_NONDETERMINISTIC_FUNCTIONS = {
    "current_date",
    "current_timestamp",
    "now",
    "random",
    "rand",
    "uuid",
}


@dataclass(frozen=True, slots=True)
class CostBasedOptimizationResult:
    original_plan: LogicalPlan
    rbo_result: OptimizationResult
    optimized_plan: LogicalPlan
    estimate: PlanEstimate
    node_estimates: dict[str, PlanEstimate]
    join_decisions: tuple[JoinDecision, ...]
    reordered_regions: int

    def explain(self) -> str:
        lines = [
            self.rbo_result.explain(),
            "",
            "== Cost-Based Optimized Plan ==",
            plan_text(self.optimized_plan),
            "",
            "== Cardinality and Cost Estimates ==",
        ]
        for node_id in _preorder_ids(self.optimized_plan):
            estimate = self.node_estimates[node_id]
            cost = estimate.cost
            sources = ", ".join(sorted(estimate.sources))
            lines.append(
                f"{node_id}: rows={estimate.row_count:.2f}, bytes={estimate.size_bytes:.2f}, "
                f"cost(cpu={cost.cpu:.2f}, network={cost.network:.2f}, "
                f"memory={cost.memory:.2f}, disk={cost.disk:.2f}, total={cost.total:.2f})"
            )
            lines.append(f"  sources: {sources}")
        lines.extend(["", "== Join Decisions =="])
        if not self.join_decisions:
            lines.append("(no joins)")
        for decision in self.join_decisions:
            lines.append(
                f"{decision.node_id}: build={decision.build_side}, "
                f"strategy={decision.strategy.value}, rows={decision.estimated_rows:.2f}, "
                f"network={decision.cost.network:.2f}; {decision.reason}"
            )
        lines.append(f"Reordered inner-join regions: {self.reordered_regions}")
        return "\n".join(lines)


class CostBasedOptimizer:
    """Run RBO, dynamic-programming join reorder, and physical join selection."""

    def __init__(
        self,
        catalog: Mapping[str, CatalogTable],
        *,
        rule_optimizer: RuleOptimizer | None = None,
        worker_count: int = 2,
        memory_budget_bytes: int = 64 * 1024 * 1024,
        broadcast_threshold_bytes: int = 10 * 1024 * 1024,
    ) -> None:
        self.rule_optimizer = rule_optimizer or RuleOptimizer()
        self.cost_model = CostModel(
            catalog,
            worker_count=worker_count,
            memory_budget_bytes=memory_budget_bytes,
            broadcast_threshold_bytes=broadcast_threshold_bytes,
        )

    def optimize(self, plan: LogicalPlan) -> CostBasedOptimizationResult:
        rbo_result = self.rule_optimizer.optimize(plan)
        reordered, region_count = self._reorder_recursive(rbo_result.optimized_plan)
        node_estimates: dict[str, PlanEstimate] = {}
        decisions: list[JoinDecision] = []
        self._collect_estimates(reordered, node_estimates, decisions)
        return CostBasedOptimizationResult(
            plan,
            rbo_result,
            reordered,
            node_estimates[reordered.node_id],
            node_estimates,
            tuple(decisions),
            region_count,
        )

    def _reorder_recursive(
        self,
        plan: LogicalPlan,
        *,
        inside_region: bool = False,
    ) -> tuple[LogicalPlan, int]:
        eligible = (
            isinstance(plan, Join)
            and plan.join_type == "inner"
            and is_deterministic(plan.condition)
        )
        rewritten_children: list[LogicalPlan] = []
        regions = 0
        for child in plan.children:
            rewritten, child_regions = self._reorder_recursive(
                child,
                inside_region=eligible,
            )
            rewritten_children.append(rewritten)
            regions += child_regions
        candidate = (
            replace_children(plan, tuple(rewritten_children))
            if tuple(rewritten_children) != plan.children
            else plan
        )
        reordered = None if inside_region else self._reorder_region(candidate)
        if reordered is None:
            return candidate, regions
        return reordered, regions + 1

    def _reorder_region(self, root: LogicalPlan) -> LogicalPlan | None:
        if not isinstance(root, Join) or root.join_type != "inner":
            return None
        leaves: list[LogicalPlan] = []
        predicates: list[Expression] = []

        def flatten(node: LogicalPlan) -> None:
            if (
                isinstance(node, Join)
                and node.join_type == "inner"
                and is_deterministic(node.condition)
            ):
                flatten(node.left)
                flatten(node.right)
                predicates.extend(split_conjuncts(node.condition))
            else:
                leaves.append(node)

        flatten(root)
        if len(leaves) < 3:
            return None
        leaf_sources = [plan_sources(leaf) for leaf in leaves]
        if any(not sources for sources in leaf_sources):
            return None
        if len(frozenset().union(*leaf_sources)) != sum(map(len, leaf_sources)):
            return None
        if any(
            not is_deterministic(predicate) or len(expression_sources(predicate)) < 2
            for predicate in predicates
        ):
            return None

        best: dict[int, tuple[LogicalPlan, PlanEstimate]] = {}
        for index, leaf in enumerate(leaves):
            best[1 << index] = (leaf, self.cost_model.estimate(leaf))
        full_mask = (1 << len(leaves)) - 1
        for size in range(2, len(leaves) + 1):
            for indexes in combinations(range(len(leaves)), size):
                mask = sum(1 << index for index in indexes)
                candidates: list[tuple[LogicalPlan, PlanEstimate]] = []
                # Requiring the lowest set bit on the left removes mirrored duplicates.
                anchor = mask & -mask
                left_mask = (mask - 1) & mask
                while left_mask:
                    right_mask = mask ^ left_mask
                    if (
                        right_mask
                        and left_mask & anchor
                        and left_mask in best
                        and right_mask in best
                    ):
                        left_plan, _ = best[left_mask]
                        right_plan, _ = best[right_mask]
                        crossing = _crossing_predicates(predicates, left_plan, right_plan)
                        if crossing:
                            join = Join(
                                "cbo_join_"
                                + "_".join(
                                    sorted(plan_sources(left_plan) | plan_sources(right_plan))
                                ),
                                left_plan,
                                right_plan,
                                "inner",
                                combine_conjuncts(crossing),
                                _join_schema(left_plan, right_plan),
                            )
                            candidates.append((join, self.cost_model.estimate(join)))
                    left_mask = (left_mask - 1) & mask
                if candidates:
                    best[mask] = min(
                        candidates,
                        key=lambda item: (item[1].cost.total, plan_text(item[0])),
                    )
        winner = best.get(full_mask)
        if winner is None:
            return None
        winner_plan = winner[0]
        assert isinstance(winner_plan, Join)
        return replace(winner_plan, node_id=root.node_id, output_schema=root.output_schema)

    def _collect_estimates(
        self,
        plan: LogicalPlan,
        estimates: dict[str, PlanEstimate],
        decisions: list[JoinDecision],
    ) -> None:
        for child in plan.children:
            self._collect_estimates(child, estimates, decisions)
        if isinstance(plan, Join):
            estimate, decision = self.cost_model.estimate_join(plan)
            estimates[plan.node_id] = estimate
            decisions.append(decision)
        else:
            estimates[plan.node_id] = self.cost_model.estimate(plan)


def is_deterministic(expression: Expression) -> bool:
    return not any(
        isinstance(item, ScalarFunction)
        and item.name.casefold() in _NONDETERMINISTIC_FUNCTIONS
        for item in walk_expression(expression)
    )


def _crossing_predicates(
    predicates: list[Expression],
    left: LogicalPlan,
    right: LogicalPlan,
) -> tuple[Expression, ...]:
    left_sources = plan_sources(left)
    right_sources = plan_sources(right)
    available = left_sources | right_sources
    return tuple(
        predicate
        for predicate in predicates
        if expression_sources(predicate) <= available
        and bool(expression_sources(predicate) & left_sources)
        and bool(expression_sources(predicate) & right_sources)
    )


def _join_schema(left: LogicalPlan, right: LogicalPlan) -> Schema:
    return Schema(
        fields=[*_qualified_fields(left), *_qualified_fields(right)],
        metadata=left.output_schema.metadata,
    )


def _qualified_fields(plan: LogicalPlan) -> list[SchemaField]:
    if not isinstance(plan, Scan):
        return list(plan.output_schema.fields)
    alias = plan.alias
    return [
        field.model_copy(update={"name": f"{alias}.{field.name}"})
        if "." not in field.name
        else field
        for field in plan.output_schema.fields
    ]


def _preorder_ids(plan: LogicalPlan) -> tuple[str, ...]:
    result = [plan.node_id]
    for child in plan.children:
        result.extend(_preorder_ids(child))
    return tuple(result)

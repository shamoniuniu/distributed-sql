"""Rule-based and cost-based optimization."""

from distributed_sql.optimizer.cbo import (
    CostBasedOptimizationResult,
    CostBasedOptimizer,
    is_deterministic,
)
from distributed_sql.optimizer.cost import (
    ColumnEstimate,
    Cost,
    CostModel,
    Distribution,
    JoinDecision,
    JoinStrategy,
    PlanEstimate,
)
from distributed_sql.optimizer.optimizer import (
    OptimizationResult,
    RuleOptimizer,
    RuleTrace,
    explain_optimization,
    optimize_plan,
)
from distributed_sql.optimizer.rules import (
    DEFAULT_RULES,
    ColumnPruning,
    ConstantFolding,
    EqualityInference,
    LimitJoinInputHint,
    LimitPushdownProject,
    PredicateMerge,
    PredicatePushdownAggregate,
    PredicatePushdownJoin,
    PredicatePushdownProject,
    Rule,
)

__all__ = [
    "DEFAULT_RULES",
    "ColumnEstimate",
    "ColumnPruning",
    "ConstantFolding",
    "Cost",
    "CostBasedOptimizationResult",
    "CostBasedOptimizer",
    "CostModel",
    "Distribution",
    "EqualityInference",
    "JoinDecision",
    "JoinStrategy",
    "LimitJoinInputHint",
    "LimitPushdownProject",
    "OptimizationResult",
    "PlanEstimate",
    "PredicateMerge",
    "PredicatePushdownAggregate",
    "PredicatePushdownJoin",
    "PredicatePushdownProject",
    "Rule",
    "RuleOptimizer",
    "RuleTrace",
    "explain_optimization",
    "is_deterministic",
    "optimize_plan",
]

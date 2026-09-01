"""Fixed-point rule optimizer with deterministic tracing and EXPLAIN output."""

from __future__ import annotations

from dataclasses import dataclass

from distributed_sql.planner.logical import LogicalPlan

from .rules import DEFAULT_RULES, Rule
from .utils import plan_text, replace_children


@dataclass(frozen=True, slots=True)
class RuleTrace:
    iteration: int
    rule: str
    before: str
    after: str


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    original_plan: LogicalPlan
    optimized_plan: LogicalPlan
    trace: tuple[RuleTrace, ...]
    iterations: int
    termination: str

    @property
    def converged(self) -> bool:
        return self.termination == "fixed_point"

    def explain(self) -> str:
        lines = [
            "== Original Logical Plan ==",
            plan_text(self.original_plan),
            "",
            "== Optimized Logical Plan ==",
            plan_text(self.optimized_plan),
            "",
            "== Rule Trace ==",
        ]
        if self.trace:
            lines.extend(
                f"{entry.iteration}. {entry.rule}\n"
                f"  before: {entry.before.splitlines()[0]}\n"
                f"  after:  {entry.after.splitlines()[0]}"
                for entry in self.trace
            )
        else:
            lines.append("(no rules matched)")
        lines.extend(
            [
                "",
                f"Termination: {self.termination}",
                f"Iterations: {self.iterations}",
            ]
        )
        return "\n".join(lines)


class RuleOptimizer:
    def __init__(
        self,
        rules: tuple[Rule, ...] = DEFAULT_RULES,
        *,
        max_iterations: int = 32,
    ) -> None:
        if max_iterations <= 0:
            raise ValueError("max_iterations must be positive")
        self.rules = rules
        self.max_iterations = max_iterations

    def optimize(self, plan: LogicalPlan) -> OptimizationResult:
        current = plan
        trace: list[RuleTrace] = []
        seen = {repr(plan)}
        for iteration in range(1, self.max_iterations + 1):
            changed = False
            for rule in self.rules:
                changes: tuple[tuple[LogicalPlan, LogicalPlan], ...]
                if rule.whole_plan:
                    rewritten = rule.apply(current)
                    changes = () if rewritten is None else ((current, rewritten),)
                else:
                    rewritten, changes = self._apply_recursive(current, rule)
                if rewritten is None or rewritten == current:
                    continue
                changed = True
                current = rewritten
                trace.extend(
                    RuleTrace(iteration, rule.name, plan_text(before), plan_text(after))
                    for before, after in changes
                    if before != after
                )
            if not changed:
                return OptimizationResult(plan, current, tuple(trace), iteration, "fixed_point")
            fingerprint = repr(current)
            if fingerprint in seen:
                return OptimizationResult(plan, current, tuple(trace), iteration, "cycle_detected")
            seen.add(fingerprint)
        return OptimizationResult(
            plan,
            current,
            tuple(trace),
            self.max_iterations,
            "max_iterations",
        )

    def _apply_recursive(
        self,
        plan: LogicalPlan,
        rule: Rule,
    ) -> tuple[LogicalPlan | None, tuple[tuple[LogicalPlan, LogicalPlan], ...]]:
        changes: list[tuple[LogicalPlan, LogicalPlan]] = []
        rewritten_children: list[LogicalPlan] = []
        child_changed = False
        for child in plan.children:
            rewritten, child_changes = self._apply_recursive(child, rule)
            actual = rewritten if rewritten is not None else child
            rewritten_children.append(actual)
            child_changed = child_changed or actual != child
            changes.extend(child_changes)
        candidate = replace_children(plan, tuple(rewritten_children)) if child_changed else plan
        rewritten = rule.apply(candidate)
        if rewritten is not None and rewritten != candidate:
            changes.append((candidate, rewritten))
            candidate = rewritten
        if candidate == plan:
            return None, tuple(changes)
        return candidate, tuple(changes)


def optimize_plan(
    plan: LogicalPlan,
    *,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
    max_iterations: int = 32,
) -> LogicalPlan:
    return RuleOptimizer(rules, max_iterations=max_iterations).optimize(plan).optimized_plan


def explain_optimization(
    plan: LogicalPlan,
    *,
    rules: tuple[Rule, ...] = DEFAULT_RULES,
    max_iterations: int = 32,
) -> str:
    return RuleOptimizer(rules, max_iterations=max_iterations).optimize(plan).explain()

"""Deterministic AI4DB advice backed only by collected query evidence."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from statistics import median
from typing import cast

from pydantic import Field, JsonValue

from distributed_sql.common.protocol import ProtocolModel
from distributed_sql.observability import JoinPlanEvidence, QueryDiagnostics
from distributed_sql.optimizer import JoinStrategy


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AdviceStatus(StrEnum):
    RECOMMENDATIONS = "recommendations"
    NO_RECOMMENDATIONS = "no_recommendations"


class AdviceEvidence(ProtocolModel):
    metric: str
    value: JsonValue
    unit: str | None = None
    comparison: str | None = None
    threshold: JsonValue | None = None
    query_id: str
    stage_id: str | None = None
    task_id: str | None = None
    plan_node_id: str | None = None


class Recommendation(ProtocolModel):
    code: str
    severity: Severity
    title: str
    evidence: list[AdviceEvidence]
    cause: str
    action: str
    expected_impact: str


class AdvisorReport(ProtocolModel):
    query_id: str
    status: AdviceStatus
    message: str
    recommendations: list[Recommendation] = Field(default_factory=list)


@dataclass(frozen=True, slots=True)
class AdvisorThresholds:
    high_shuffle_bytes: int = 64 * 1024 * 1024
    skew_ratio: float = 3.0
    skew_min_partition_bytes: int = 1 * 1024 * 1024
    frequent_spill_count: int = 3
    critical_spill_bytes: int = 64 * 1024 * 1024
    broadcast_candidate_bytes: int = 10 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.high_shuffle_bytes < 1:
            raise ValueError("high_shuffle_bytes must be positive")
        if self.skew_ratio <= 1:
            raise ValueError("skew_ratio must be greater than one")
        if self.skew_min_partition_bytes < 1:
            raise ValueError("skew_min_partition_bytes must be positive")
        if self.frequent_spill_count < 1:
            raise ValueError("frequent_spill_count must be positive")
        if self.critical_spill_bytes < 1:
            raise ValueError("critical_spill_bytes must be positive")
        if self.broadcast_candidate_bytes < 1:
            raise ValueError("broadcast_candidate_bytes must be positive")


class QueryAdvisor:
    """Apply conservative rules; absence of evidence never becomes a finding."""

    def __init__(self, thresholds: AdvisorThresholds | None = None) -> None:
        self.thresholds = thresholds or AdvisorThresholds()

    def analyze(self, diagnostics: QueryDiagnostics) -> AdvisorReport:
        recommendations = [
            *self._missing_statistics(diagnostics),
            *self._high_shuffle(diagnostics),
            *self._data_skew(diagnostics),
            *self._spill(diagnostics),
            *self._join_strategy(diagnostics),
        ]
        recommendations.sort(key=lambda item: (_severity_rank(item.severity), item.code))
        if not recommendations:
            return AdvisorReport(
                query_id=diagnostics.query_id,
                status=AdviceStatus.NO_RECOMMENDATIONS,
                message="暂无高置信度建议",
            )
        return AdvisorReport(
            query_id=diagnostics.query_id,
            status=AdviceStatus.RECOMMENDATIONS,
            message=f"发现 {len(recommendations)} 条基于量化证据的建议",
            recommendations=recommendations,
        )

    def _missing_statistics(
        self,
        diagnostics: QueryDiagnostics,
    ) -> list[Recommendation]:
        findings: list[Recommendation] = []
        for join in diagnostics.optimization.joins:
            if not join.statistics_fallbacks:
                continue
            references = sorted(_statistics_references(join.statistics_fallbacks))
            findings.append(
                Recommendation(
                    code=f"MISSING_STATISTICS:{join.node_id}",
                    severity=Severity.WARNING,
                    title="Join 决策使用了回退统计",
                    evidence=[
                        AdviceEvidence(
                            metric="statistics_fallback_count",
                            value=len(join.statistics_fallbacks),
                            comparison=">",
                            threshold=0,
                            query_id=diagnostics.query_id,
                            plan_node_id=join.node_id,
                        ),
                        AdviceEvidence(
                            metric="statistics_fallbacks",
                            value=cast(JsonValue, join.statistics_fallbacks),
                            query_id=diagnostics.query_id,
                            plan_node_id=join.node_id,
                        ),
                    ],
                    cause=(
                        f"Join {join.node_id} 的基数或宽度估算缺少 Catalog 统计, "
                        "优化器使用了显式默认值。"
                    ),
                    action=(
                        "对以下表或列执行 ANALYZE 后重新规划查询: "
                        + (", ".join(references) if references else "证据中列出的对象")
                        + "。"
                    ),
                    expected_impact="提高 Join 基数、构建侧和数据分布策略选择的可靠性。",
                )
            )
        return findings

    def _high_shuffle(
        self,
        diagnostics: QueryDiagnostics,
    ) -> list[Recommendation]:
        actual = diagnostics.runtime.shuffle_bytes_written
        if actual < self.thresholds.high_shuffle_bytes:
            return []
        return [
            Recommendation(
                code="HIGH_SHUFFLE",
                severity=Severity.WARNING,
                title="Shuffle 写入量较高",
                evidence=[
                    AdviceEvidence(
                        metric="shuffle_bytes_written",
                        value=actual,
                        unit="bytes",
                        comparison=">=",
                        threshold=self.thresholds.high_shuffle_bytes,
                        query_id=diagnostics.query_id,
                    ),
                    AdviceEvidence(
                        metric="shuffle_records_written",
                        value=diagnostics.runtime.shuffle_records_written,
                        unit="rows",
                        query_id=diagnostics.query_id,
                    ),
                ],
                cause="Exchange 实际写入字节数达到高 Shuffle 阈值。",
                action="优先在 Exchange 前增加可下推过滤和列裁剪, 并检查 Join 键分区复用。",
                expected_impact="减少网络传输、Shuffle 文件 I/O 和下游 Task 输入量。",
            )
        ]

    def _data_skew(
        self,
        diagnostics: QueryDiagnostics,
    ) -> list[Recommendation]:
        sizes = diagnostics.shuffle_partition_bytes
        if len(sizes) < 2 or max(sizes) < self.thresholds.skew_min_partition_bytes:
            return []
        ordered = sorted(sizes)
        baseline = float(median(ordered[:-1]))
        ratio = max(sizes) / max(baseline, 1.0)
        if ratio < self.thresholds.skew_ratio:
            return []
        return [
            Recommendation(
                code="SHUFFLE_SKEW",
                severity=Severity.WARNING,
                title="Shuffle 分区存在数据倾斜",
                evidence=[
                    AdviceEvidence(
                        metric="max_partition_bytes",
                        value=max(sizes),
                        unit="bytes",
                        comparison=">=",
                        threshold=self.thresholds.skew_min_partition_bytes,
                        query_id=diagnostics.query_id,
                    ),
                    AdviceEvidence(
                        metric="max_to_typical_partition_ratio",
                        value=round(ratio, 3),
                        comparison=">=",
                        threshold=self.thresholds.skew_ratio,
                        query_id=diagnostics.query_id,
                    ),
                    AdviceEvidence(
                        metric="shuffle_partition_bytes",
                        value=cast(JsonValue, sizes),
                        unit="bytes",
                        query_id=diagnostics.query_id,
                    ),
                ],
                cause="最大 Shuffle 分区显著大于分区中位数, 单个 Task 可能成为长尾。",
                action="检查高频 Join/聚合键, 考虑增加分区数、复合分区键或对热点键加盐。",
                expected_impact="降低长尾 Task 的运行时间和峰值内存压力。",
            )
        ]

    def _spill(self, diagnostics: QueryDiagnostics) -> list[Recommendation]:
        runtime = diagnostics.runtime
        if runtime.spill_bytes == 0 and runtime.spill_count == 0:
            return []
        critical = runtime.spill_bytes >= self.thresholds.critical_spill_bytes
        frequent = runtime.spill_count >= self.thresholds.frequent_spill_count
        severity = Severity.CRITICAL if critical or frequent else Severity.WARNING
        return [
            Recommendation(
                code="OPERATOR_SPILL",
                severity=severity,
                title="执行算子发生落盘",
                evidence=[
                    AdviceEvidence(
                        metric="spill_bytes",
                        value=runtime.spill_bytes,
                        unit="bytes",
                        comparison=">=",
                        threshold=(
                            self.thresholds.critical_spill_bytes if critical else 1
                        ),
                        query_id=diagnostics.query_id,
                    ),
                    AdviceEvidence(
                        metric="spill_count",
                        value=runtime.spill_count,
                        comparison=">=",
                        threshold=(
                            self.thresholds.frequent_spill_count if frequent else 1
                        ),
                        query_id=diagnostics.query_id,
                    ),
                    AdviceEvidence(
                        metric="peak_memory_bytes",
                        value=runtime.peak_memory_bytes,
                        unit="bytes",
                        query_id=diagnostics.query_id,
                    ),
                ],
                cause="排序、聚合或 Join 的实际工作集超过了可用内存并写入临时存储。",
                action="先减少输入行和列; 再评估提高查询内存预算或调整 Join 构建侧与策略。",
                expected_impact="减少临时磁盘 I/O; 内存预算不变时可降低落盘次数和执行时延。",
            )
        ]

    def _join_strategy(
        self,
        diagnostics: QueryDiagnostics,
    ) -> list[Recommendation]:
        findings: list[Recommendation] = []
        for join in diagnostics.optimization.joins:
            if join.statistics_fallbacks:
                continue
            if (
                join.strategy
                in {
                    "repartition_left",
                    "repartition_right",
                    "repartition_both",
                }
                and join.build_bytes <= self.thresholds.broadcast_candidate_bytes
            ):
                findings.append(self._broadcast_candidate(diagnostics.query_id, join))
            elif (
                join.strategy == JoinStrategy.BROADCAST.value
                and join.build_bytes > self.thresholds.broadcast_candidate_bytes
            ):
                findings.append(self._oversized_broadcast(diagnostics.query_id, join))
        return findings

    def _broadcast_candidate(
        self,
        query_id: str,
        join: JoinPlanEvidence,
    ) -> Recommendation:
        return Recommendation(
            code=f"JOIN_BROADCAST_CANDIDATE:{join.node_id}",
            severity=Severity.INFO,
            title="Join 构建侧可评估广播",
            evidence=[
                AdviceEvidence(
                    metric="join_strategy",
                    value=join.strategy,
                    query_id=query_id,
                    plan_node_id=join.node_id,
                ),
                AdviceEvidence(
                    metric="estimated_build_bytes",
                    value=join.build_bytes,
                    unit="bytes",
                    comparison="<=",
                    threshold=self.thresholds.broadcast_candidate_bytes,
                    query_id=query_id,
                    plan_node_id=join.node_id,
                ),
            ],
            cause="当前 Join 需要重分区, 但有可靠统计的构建侧估算低于广播候选阈值。",
            action=(
                "结合 Worker 数量和内存预算评估 BROADCAST, "
                "并用 EXPLAIN ANALYZE 对比实际 Shuffle。"
            ),
            expected_impact="在构建侧可安全驻留内存时, 可能消除一侧或双侧 Repartition。",
        )

    def _oversized_broadcast(
        self,
        query_id: str,
        join: JoinPlanEvidence,
    ) -> Recommendation:
        return Recommendation(
            code=f"JOIN_BROADCAST_SIZE:{join.node_id}",
            severity=Severity.WARNING,
            title="广播 Join 构建侧较大",
            evidence=[
                AdviceEvidence(
                    metric="estimated_build_bytes",
                    value=join.build_bytes,
                    unit="bytes",
                    comparison=">",
                    threshold=self.thresholds.broadcast_candidate_bytes,
                    query_id=query_id,
                    plan_node_id=join.node_id,
                )
            ],
            cause="广播会在每个 Worker 复制构建侧, 可靠估算已超过配置的广播建议阈值。",
            action="评估改用单侧或双侧 Repartition, 并确认 Join 键已有分区是否可复用。",
            expected_impact="降低每个 Worker 的构建侧内存占用和广播网络放大。",
        )


def _statistics_references(fallbacks: list[str]) -> set[str]:
    references: set[str] = set()
    for source in fallbacks:
        payload = source.removeprefix("default:").split("=", 1)[0]
        references.add(payload.rsplit(".", 1)[0])
    return references


def _severity_rank(severity: Severity) -> int:
    return {
        Severity.CRITICAL: 0,
        Severity.WARNING: 1,
        Severity.INFO: 2,
    }[severity]

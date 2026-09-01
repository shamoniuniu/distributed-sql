"""Asynchronous query lifecycle backed by the engine's real planning stack."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, NoReturn

from pydantic import Field

from distributed_sql.advisor import AdvisorReport, QueryAdvisor
from distributed_sql.catalog.models import CatalogTable
from distributed_sql.catalog.repository import SQLiteCatalog
from distributed_sql.catalog.storage import ObjectStoreRouter
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import PlanNode, ProtocolModel, Query, QueryState
from distributed_sql.coordinator.registry import WorkerRegistry
from distributed_sql.coordinator.remote import RemoteWorker
from distributed_sql.coordinator.remote_execution import RemoteDistributedExecutor
from distributed_sql.data_source import create_data_source_registry
from distributed_sql.execution import PhysicalPlan, materialize_exchanges
from distributed_sql.execution.operators import ExecutionCancelled
from distributed_sql.execution.scheduler import CancellationConfirmationError
from distributed_sql.observability import QueryDiagnostics, build_query_diagnostics
from distributed_sql.optimizer import CostBasedOptimizationResult, CostBasedOptimizer
from distributed_sql.planner import Binder


class QuerySubmitRequest(ProtocolModel):
    sql: str = Field(min_length=1)


class QueryListResponse(ProtocolModel):
    queries: list[Query]


class QueryPlanResponse(ProtocolModel):
    query_id: str
    original_logical_plan: PlanNode
    optimized_logical_plan: PlanNode
    physical_plan: PlanNode
    explain: str


class QueryResultPage(ProtocolModel):
    query_id: str
    columns: list[str]
    rows: list[dict[str, Any]]
    offset: int = Field(ge=0)
    limit: int = Field(ge=1)
    returned: int = Field(ge=0)
    total_rows: int = Field(ge=0)
    next_offset: int | None = Field(default=None, ge=0)


class QueryMetricsResponse(ProtocolModel):
    query_id: str
    diagnostics: QueryDiagnostics
    explain_analyze: str


@dataclass(slots=True)
class _QueryRecord:
    query: Query
    optimization: CostBasedOptimizationResult | None = None
    physical_plan: PhysicalPlan | None = None
    rows: list[dict[str, Any]] | None = None
    columns: list[str] | None = None
    diagnostics: QueryDiagnostics | None = None
    advisor: AdvisorReport | None = None
    executor: RemoteDistributedExecutor | None = None
    task: asyncio.Task[None] | None = None
    cancel_requested: bool = False


class QueryService:
    """Own query state and connect HTTP clients to planner, optimizer and executor."""

    def __init__(
        self,
        catalog: SQLiteCatalog,
        object_stores: ObjectStoreRouter,
        worker_registry: WorkerRegistry,
        *,
        runtime_root: str | Path,
        remote_task_auth_token: str | None = None,
        cancellation_timeout_seconds: float = 10.0,
    ) -> None:
        self._catalog = catalog
        self._object_stores = object_stores
        self._worker_registry = worker_registry
        self._runtime_root = runtime_root
        self._remote_task_auth_token = remote_task_auth_token
        self._cancellation_timeout_seconds = cancellation_timeout_seconds
        self._records: dict[str, _QueryRecord] = {}

    def submit(self, sql: str) -> Query:
        query = Query(sql=sql.strip())
        record = _QueryRecord(query=query)
        self._records[query.query_id] = record
        record.task = asyncio.create_task(
            self._run(record),
            name=f"query-{query.query_id}",
        )
        return query.model_copy(deep=True)

    async def explain(self, sql: str) -> QueryPlanResponse:
        query_id = Query(sql=sql.strip()).query_id
        tables = self._catalog_snapshot()
        workers = await self._worker_registry.list_workers()
        optimization, physical = await asyncio.to_thread(
            self._plan,
            sql,
            tables,
            max(len(workers), 1),
        )
        return self._plan_response(query_id, optimization, physical)

    def list_queries(self) -> list[Query]:
        return [
            record.query.model_copy(deep=True)
            for record in sorted(
                self._records.values(),
                key=lambda item: item.query.created_at,
                reverse=True,
            )
        ]

    def get_query(self, query_id: str) -> Query:
        return self._get(query_id).query.model_copy(deep=True)

    def get_plan(self, query_id: str) -> QueryPlanResponse:
        record = self._get(query_id)
        optimization = record.optimization
        physical_plan = record.physical_plan
        if optimization is None or physical_plan is None:
            self._not_ready(record, "plan")
        return self._plan_response(
            query_id,
            optimization,
            physical_plan,
        )

    def get_results(self, query_id: str, offset: int, limit: int) -> QueryResultPage:
        record = self._get(query_id)
        all_rows = record.rows
        if record.query.state is not QueryState.SUCCEEDED or all_rows is None:
            self._not_ready(record, "results")
        rows = all_rows[offset : offset + limit]
        total = len(all_rows)
        next_offset = offset + len(rows) if offset + len(rows) < total else None
        return QueryResultPage(
            query_id=query_id,
            columns=record.columns or [],
            rows=rows,
            offset=offset,
            limit=limit,
            returned=len(rows),
            total_rows=total,
            next_offset=next_offset,
        )

    def get_metrics(self, query_id: str) -> QueryMetricsResponse:
        record = self._get(query_id)
        diagnostics = record.diagnostics
        if diagnostics is None:
            self._not_ready(record, "metrics")
        return QueryMetricsResponse(
            query_id=query_id,
            diagnostics=diagnostics,
            explain_analyze=diagnostics.explain_analyze(),
        )

    def get_advisor(self, query_id: str) -> AdvisorReport:
        record = self._get(query_id)
        advisor = record.advisor
        if advisor is None:
            self._not_ready(record, "advisor report")
        return advisor

    async def cancel(self, query_id: str) -> Query:
        record = self._get(query_id)
        if record.query.state in {
            QueryState.SUCCEEDED,
            QueryState.FAILED,
            QueryState.CANCELED,
        }:
            return record.query.model_copy(deep=True)
        record.cancel_requested = True
        if record.executor is not None:
            record.executor.cancel(query_id)
        if record.task is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(record.task),
                    timeout=self._cancellation_timeout_seconds,
                )
            except TimeoutError:
                record.query.error = {
                    "code": ErrorCode.TASK_FAILED.value,
                    "message": "Query cancellation could not be confirmed before the timeout.",
                    "context": {
                        "query_id": query_id,
                        "failure_kind": "cancellation_timeout",
                    },
                }
                self._set_state(record, QueryState.FAILED)
        return record.query.model_copy(deep=True)

    async def close(self) -> None:
        pending: list[asyncio.Task[Query]] = []
        for record in self._records.values():
            if record.task is None or record.task.done():
                continue
            pending.append(asyncio.create_task(self.cancel(record.query.query_id)))
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _run(self, record: _QueryRecord) -> None:
        query = record.query
        try:
            self._set_state(record, QueryState.PLANNING)
            tables = self._catalog_snapshot()
            registered = await self._worker_registry.list_workers()
            active = [worker for worker in registered if worker.state.value == "active"]
            if not active:
                raise DistributedSQLError(
                    ErrorCode.SERVICE_UNAVAILABLE,
                    "No active Worker is available.",
                    status_code=503,
                )
            optimization, physical = await asyncio.to_thread(
                self._plan,
                query.sql,
                tables,
                len(active),
            )
            record.optimization = optimization
            record.physical_plan = physical
            query.logical_plan = optimization.optimized_plan.to_protocol()
            query.physical_plan = physical.to_protocol()
            if record.cancel_requested:
                raise asyncio.CancelledError
            workers = [
                RemoteWorker(
                    item.worker_id,
                    item.slots,
                    item.endpoint,
                    stores=self._object_stores,
                    auth_token=self._remote_task_auth_token,
                    cancellation_timeout_seconds=self._cancellation_timeout_seconds,
                )
                for item in active
            ]
            executor = RemoteDistributedExecutor(
                tables,
                create_data_source_registry(self._object_stores),
                workers,
                self._object_stores,
                self._runtime_root,
                registry=self._worker_registry,
            )
            record.executor = executor
            self._set_state(record, QueryState.RUNNING)
            result = await executor.execute(query.query_id, physical)
            if record.cancel_requested:
                raise asyncio.CancelledError
            record.columns = result.table.column_names
            record.rows = result.table.to_pylist()
            record.diagnostics = build_query_diagnostics(
                query.query_id,
                optimization,
                physical,
                result,
            )
            record.advisor = QueryAdvisor().analyze(record.diagnostics)
            self._set_state(record, QueryState.SUCCEEDED)
        except (asyncio.CancelledError, ExecutionCancelled):
            if record.query.state is not QueryState.FAILED:
                self._set_state(record, QueryState.CANCELED)
        except CancellationConfirmationError:
            query.error = {
                "code": ErrorCode.TASK_FAILED.value,
                "message": "Query cancellation could not be confirmed for every remote attempt.",
                "context": {
                    "query_id": query.query_id,
                    "failure_kind": "cancellation_confirmation",
                },
            }
            self._set_state(record, QueryState.FAILED)
        except DistributedSQLError as exc:
            if record.cancel_requested:
                self._set_state(record, QueryState.CANCELED)
            else:
                query.error = exc.as_response().error.model_dump(mode="json")
                self._set_state(record, QueryState.FAILED)
        except Exception as exc:
            if record.cancel_requested:
                self._set_state(record, QueryState.CANCELED)
            else:
                query.error = {
                    "code": ErrorCode.INTERNAL_ERROR.value,
                    "message": "Query execution failed.",
                    "context": {"type": type(exc).__name__},
                }
                self._set_state(record, QueryState.FAILED)
        finally:
            record.executor = None

    def _catalog_snapshot(self) -> dict[str, CatalogTable]:
        return {
            f"{namespace.name}.{table.name}": table
            for namespace in self._catalog.list_namespaces()
            for table in self._catalog.list_tables(namespace.name)
        }

    @staticmethod
    def _plan(
        sql: str,
        tables: dict[str, CatalogTable],
        worker_count: int,
    ) -> tuple[CostBasedOptimizationResult, PhysicalPlan]:
        logical = Binder(tables).bind(sql)
        optimization = CostBasedOptimizer(
            tables,
            worker_count=worker_count,
        ).optimize(logical)
        physical = materialize_exchanges(
            optimization.optimized_plan,
            optimization.join_decisions,
            partition_count=worker_count,
        )
        return optimization, physical

    @staticmethod
    def _plan_response(
        query_id: str,
        optimization: CostBasedOptimizationResult,
        physical: PhysicalPlan,
    ) -> QueryPlanResponse:
        return QueryPlanResponse(
            query_id=query_id,
            original_logical_plan=optimization.original_plan.to_protocol(),
            optimized_logical_plan=optimization.optimized_plan.to_protocol(),
            physical_plan=physical.to_protocol(),
            explain=optimization.explain(),
        )

    def _get(self, query_id: str) -> _QueryRecord:
        record = self._records.get(query_id)
        if record is None:
            raise DistributedSQLError(
                ErrorCode.NOT_FOUND,
                f"Query {query_id!r} does not exist.",
                status_code=404,
                context={"query_id": query_id},
            )
        return record

    @staticmethod
    def _not_ready(record: _QueryRecord, resource: str) -> NoReturn:
        raise DistributedSQLError(
            ErrorCode.CONFLICT,
            f"Query {resource} are not available while state is {record.query.state.value!r}.",
            status_code=409,
            context={
                "query_id": record.query.query_id,
                "state": record.query.state.value,
            },
        )

    @staticmethod
    def _set_state(record: _QueryRecord, state: QueryState) -> None:
        record.query.state = state
        record.query.updated_at = datetime.now(UTC)

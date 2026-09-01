"""REST contracts for asynchronous query operations."""

from fastapi import APIRouter, status
from fastapi import Query as QueryParameter

from distributed_sql.advisor import AdvisorReport
from distributed_sql.common.protocol import Query, WorkerListResponse
from distributed_sql.coordinator.queries import (
    QueryListResponse,
    QueryMetricsResponse,
    QueryPlanResponse,
    QueryResultPage,
    QueryService,
    QuerySubmitRequest,
)
from distributed_sql.coordinator.registry import WorkerRegistry


def create_query_router(
    service: QueryService,
    worker_registry: WorkerRegistry,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["queries"])

    @router.post(
        "/queries",
        response_model=Query,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def submit_query(request: QuerySubmitRequest) -> Query:
        return service.submit(request.sql)

    @router.post("/queries/explain", response_model=QueryPlanResponse)
    async def explain_query(request: QuerySubmitRequest) -> QueryPlanResponse:
        return await service.explain(request.sql)

    @router.get("/queries", response_model=QueryListResponse)
    async def list_queries() -> QueryListResponse:
        return QueryListResponse(queries=service.list_queries())

    @router.get("/queries/{query_id}", response_model=Query)
    async def get_query(query_id: str) -> Query:
        return service.get_query(query_id)

    @router.delete("/queries/{query_id}", response_model=Query)
    async def cancel_query(query_id: str) -> Query:
        return await service.cancel(query_id)

    @router.get("/queries/{query_id}/results", response_model=QueryResultPage)
    async def get_query_results(
        query_id: str,
        offset: int = QueryParameter(default=0, ge=0),
        limit: int = QueryParameter(default=100, ge=1, le=1000),
    ) -> QueryResultPage:
        return service.get_results(query_id, offset, limit)

    @router.get("/queries/{query_id}/plan", response_model=QueryPlanResponse)
    async def get_query_plan(query_id: str) -> QueryPlanResponse:
        return service.get_plan(query_id)

    @router.get("/queries/{query_id}/metrics", response_model=QueryMetricsResponse)
    async def get_query_metrics(query_id: str) -> QueryMetricsResponse:
        return service.get_metrics(query_id)

    @router.get("/queries/{query_id}/advisor", response_model=AdvisorReport)
    async def get_query_advisor(query_id: str) -> AdvisorReport:
        return service.get_advisor(query_id)

    @router.get("/nodes", response_model=WorkerListResponse)
    async def list_nodes() -> WorkerListResponse:
        return WorkerListResponse(workers=await worker_registry.list_workers())

    return router

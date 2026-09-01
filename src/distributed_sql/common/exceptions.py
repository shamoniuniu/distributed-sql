"""Stable internal and HTTP-facing error contracts."""

from enum import StrEnum
from typing import Any

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException

_PUBLIC_CONTEXT_KEYS = frozenset(
    {
        "alias",
        "attempt_count",
        "attempt_id",
        "candidates",
        "column",
        "column_name",
        "column_names",
        "column_count",
        "end",
        "expression_type",
        "failure_kind",
        "format",
        "function",
        "limit_bytes",
        "line",
        "namespace",
        "partition",
        "partition_count",
        "query_id",
        "requested_bytes",
        "stage_id",
        "start",
        "statement_count",
        "table",
        "task_id",
        "worker_id",
    }
)


class ErrorCode(StrEnum):
    SYNTAX_ERROR = "SYNTAX_ERROR"
    BINDING_ERROR = "BINDING_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    INVALID_REQUEST = "INVALID_REQUEST"
    LEASE_REJECTED = "LEASE_REJECTED"
    RESOURCE_EXHAUSTED = "RESOURCE_EXHAUSTED"
    TASK_FAILED = "TASK_FAILED"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    INTERNAL_ERROR = "INTERNAL_ERROR"


_PUBLIC_TASK_MESSAGES = {
    ErrorCode.SYNTAX_ERROR: "Worker SQL syntax validation failed.",
    ErrorCode.BINDING_ERROR: "Worker SQL binding failed.",
    ErrorCode.NOT_FOUND: "Worker Task input was not found.",
    ErrorCode.CONFLICT: "Worker Task state conflicts with the request.",
    ErrorCode.INVALID_REQUEST: "Worker Task input is invalid.",
    ErrorCode.LEASE_REJECTED: "Worker lease was rejected.",
    ErrorCode.RESOURCE_EXHAUSTED: "Worker resources were exhausted.",
    ErrorCode.TASK_FAILED: "Worker Task execution failed.",
    ErrorCode.SERVICE_UNAVAILABLE: "Worker service is unavailable.",
    ErrorCode.INTERNAL_ERROR: "Worker encountered an internal error.",
}


class ErrorDetail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: ErrorCode
    message: str
    context: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    error: ErrorDetail


class DistributedSQLError(Exception):
    """Expected domain error that is safe to expose without a traceback."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        status_code: int = 400,
        context: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.context = context or {}

    def as_response(self) -> ErrorResponse:
        return ErrorResponse(
            error=ErrorDetail(code=self.code, message=self.message, context=self.context)
        )


def public_task_error(
    exc: Exception,
    *,
    context: dict[str, Any],
) -> ErrorDetail:
    """Build a stable Worker error without exposing filesystem or credential details."""

    if isinstance(exc, DistributedSQLError):
        code = exc.code
        message = _PUBLIC_TASK_MESSAGES[code]
        source_context = exc.context
    else:
        code = ErrorCode.TASK_FAILED
        message = "Worker Task execution failed."
        source_context = {}
    safe_context = {
        key: value for key, value in source_context.items() if key in _PUBLIC_CONTEXT_KEYS
    }
    safe_context.update(
        {key: value for key, value in context.items() if key in _PUBLIC_CONTEXT_KEYS}
    )
    return ErrorDetail(code=code, message=message, context=safe_context)


def status_code_for_error(code: ErrorCode) -> int:
    """Return the HTTP status associated with a remotely reconstructed domain error."""

    return {
        ErrorCode.NOT_FOUND: 404,
        ErrorCode.CONFLICT: 409,
        ErrorCode.INVALID_REQUEST: 422,
        ErrorCode.LEASE_REJECTED: 403,
        ErrorCode.RESOURCE_EXHAUSTED: 507,
        ErrorCode.TASK_FAILED: 500,
        ErrorCode.SERVICE_UNAVAILABLE: 503,
        ErrorCode.INTERNAL_ERROR: 500,
    }.get(code, 400)


def install_exception_handlers(app: FastAPI) -> None:
    """Install the common error envelope on a FastAPI application."""

    @app.exception_handler(DistributedSQLError)
    async def handle_domain_error(_request: Request, exc: DistributedSQLError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.as_response().model_dump(mode="json"),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        response = ErrorResponse(
            error=ErrorDetail(
                code=ErrorCode.INVALID_REQUEST,
                message="Request validation failed.",
                context={"errors": jsonable_encoder(exc.errors())},
            )
        )
        return JSONResponse(status_code=422, content=response.model_dump(mode="json"))

    @app.exception_handler(HTTPException)
    async def handle_http_error(_request: Request, exc: HTTPException) -> JSONResponse:
        code = ErrorCode.NOT_FOUND if exc.status_code == 404 else ErrorCode.INVALID_REQUEST
        response = ErrorResponse(
            error=ErrorDetail(
                code=code,
                message=str(exc.detail),
                context={"status_code": exc.status_code},
            )
        )
        return JSONResponse(status_code=exc.status_code, content=response.model_dump(mode="json"))

    @app.exception_handler(Exception)
    async def handle_unexpected_error(_request: Request, _exc: Exception) -> JSONResponse:
        response = ErrorResponse(
            error=ErrorDetail(
                code=ErrorCode.INTERNAL_ERROR,
                message="An unexpected internal error occurred.",
            )
        )
        return JSONResponse(status_code=500, content=response.model_dump(mode="json"))

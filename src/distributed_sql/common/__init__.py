"""Configuration, errors, and wire protocols shared by all services."""

from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode

__all__ = ["DistributedSQLError", "ErrorCode"]

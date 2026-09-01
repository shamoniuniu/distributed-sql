"""Persistent SQLite Catalog repository."""

import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from distributed_sql.catalog.models import (
    CatalogTable,
    Namespace,
    NamespaceCreate,
    NamespaceUpdate,
    TableCreate,
    TableFormat,
    TableUpdate,
)
from distributed_sql.common.exceptions import DistributedSQLError, ErrorCode
from distributed_sql.common.protocol import (
    ColumnStatistics,
    Partition,
    PartitionStrategy,
    Schema,
    SchemaField,
    Statistics,
)

_SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS namespaces (
    name TEXT PRIMARY KEY,
    properties_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS catalog_tables (
    namespace_name TEXT NOT NULL,
    name TEXT NOT NULL,
    format TEXT NOT NULL,
    location TEXT NOT NULL,
    partition_strategy TEXT NOT NULL,
    partition_keys_json TEXT NOT NULL,
    properties_json TEXT NOT NULL,
    schema_metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (namespace_name, name),
    FOREIGN KEY (namespace_name) REFERENCES namespaces(name) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS columns (
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    field_json TEXT NOT NULL,
    PRIMARY KEY (namespace_name, table_name, ordinal),
    FOREIGN KEY (namespace_name, table_name)
        REFERENCES catalog_tables(namespace_name, name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS partitions (
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    partition_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    location TEXT NOT NULL,
    strategy TEXT NOT NULL,
    keys_json TEXT NOT NULL,
    size_bytes INTEGER,
    row_count INTEGER,
    checksum TEXT,
    PRIMARY KEY (namespace_name, table_name, partition_id),
    UNIQUE (namespace_name, table_name, ordinal),
    FOREIGN KEY (namespace_name, table_name)
        REFERENCES catalog_tables(namespace_name, name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS table_statistics (
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_count INTEGER,
    size_bytes INTEGER,
    collected_at TEXT,
    source TEXT NOT NULL,
    PRIMARY KEY (namespace_name, table_name),
    FOREIGN KEY (namespace_name, table_name)
        REFERENCES catalog_tables(namespace_name, name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS column_statistics (
    namespace_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    null_count INTEGER,
    distinct_count INTEGER,
    min_value_json TEXT,
    max_value_json TEXT,
    average_size_bytes REAL,
    PRIMARY KEY (namespace_name, table_name, column_name),
    FOREIGN KEY (namespace_name, table_name)
        REFERENCES catalog_tables(namespace_name, name) ON DELETE CASCADE
);
"""


def _now() -> datetime:
    return datetime.now(UTC)


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _decode(value: str) -> Any:
    return json.loads(value)


class SQLiteCatalog:
    def __init__(self, path: Path) -> None:
        self.path = path

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(_SCHEMA)

    def create_namespace(self, request: NamespaceCreate) -> Namespace:
        now = _now().isoformat()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO namespaces(name, properties_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (request.name, _json(request.properties), now, now),
                )
        except sqlite3.IntegrityError as exc:
            raise self._conflict("Namespace", request.name) from exc
        return self.get_namespace(request.name)

    def list_namespaces(self) -> list[Namespace]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM namespaces ORDER BY name").fetchall()
        return [self._namespace_from_row(row) for row in rows]

    def get_namespace(self, name: str) -> Namespace:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM namespaces WHERE name = ?",
                (name,),
            ).fetchone()
        if row is None:
            raise self._not_found("Namespace", name)
        return self._namespace_from_row(row)

    def update_namespace(self, name: str, request: NamespaceUpdate) -> Namespace:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE namespaces SET properties_json = ?, updated_at = ?
                WHERE name = ?
                """,
                (_json(request.properties), _now().isoformat(), name),
            )
        if cursor.rowcount == 0:
            raise self._not_found("Namespace", name)
        return self.get_namespace(name)

    def delete_namespace(self, name: str) -> None:
        try:
            with self._connect() as connection:
                cursor = connection.execute("DELETE FROM namespaces WHERE name = ?", (name,))
        except sqlite3.IntegrityError as exc:
            raise DistributedSQLError(
                ErrorCode.CONFLICT,
                f"Namespace {name!r} is not empty.",
                status_code=409,
                context={"namespace": name},
            ) from exc
        if cursor.rowcount == 0:
            raise self._not_found("Namespace", name)

    def create_table(self, namespace: str, request: TableCreate) -> CatalogTable:
        self.get_namespace(namespace)
        now = _now().isoformat()
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO catalog_tables(
                        namespace_name, name, format, location, partition_strategy,
                        partition_keys_json, properties_json, schema_metadata_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        namespace,
                        request.name,
                        request.format.value,
                        request.location,
                        request.partition_strategy.value,
                        _json(request.partition_keys),
                        _json(request.properties),
                        _json(request.schema_.metadata),
                        now,
                        now,
                    ),
                )
                self._insert_columns(connection, namespace, request.name, request.schema_.fields)
        except sqlite3.IntegrityError as exc:
            raise self._conflict("Table", f"{namespace}.{request.name}") from exc
        return self.get_table(namespace, request.name)

    def list_tables(self, namespace: str) -> list[CatalogTable]:
        self.get_namespace(namespace)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM catalog_tables
                WHERE namespace_name = ? ORDER BY name
                """,
                (namespace,),
            ).fetchall()
            return [self._table_from_row(connection, row) for row in rows]

    def get_table(self, namespace: str, name: str) -> CatalogTable:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM catalog_tables
                WHERE namespace_name = ? AND name = ?
                """,
                (namespace, name),
            ).fetchone()
            if row is None:
                raise self._not_found("Table", f"{namespace}.{name}")
            return self._table_from_row(connection, row)

    def update_table(
        self,
        namespace: str,
        name: str,
        request: TableUpdate,
    ) -> CatalogTable:
        current = self.get_table(namespace, name)
        schema = request.schema_ or current.schema_
        partition_keys = (
            request.partition_keys
            if request.partition_keys is not None
            else current.partition_keys
        )
        validated = TableCreate(
            name=name,
            schema=schema,
            format=request.format or current.format,
            location=request.location or current.location,
            partition_strategy=request.partition_strategy or current.partition_strategy,
            partition_keys=partition_keys,
            properties=request.properties
            if request.properties is not None
            else current.properties,
        )
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE catalog_tables SET
                    format = ?, location = ?, partition_strategy = ?,
                    partition_keys_json = ?, properties_json = ?,
                    schema_metadata_json = ?, updated_at = ?
                WHERE namespace_name = ? AND name = ?
                """,
                (
                    validated.format.value,
                    validated.location,
                    validated.partition_strategy.value,
                    _json(validated.partition_keys),
                    _json(validated.properties),
                    _json(validated.schema_.metadata),
                    _now().isoformat(),
                    namespace,
                    name,
                ),
            )
            connection.execute(
                "DELETE FROM columns WHERE namespace_name = ? AND table_name = ?",
                (namespace, name),
            )
            self._insert_columns(connection, namespace, name, validated.schema_.fields)
        return self.get_table(namespace, name)

    def delete_table(self, namespace: str, name: str) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM catalog_tables WHERE namespace_name = ? AND name = ?",
                (namespace, name),
            )
        if cursor.rowcount == 0:
            raise self._not_found("Table", f"{namespace}.{name}")

    def replace_import_metadata(
        self,
        namespace: str,
        name: str,
        *,
        strategy: PartitionStrategy,
        partition_keys: list[str],
        partitions: list[Partition],
        statistics: Statistics,
    ) -> CatalogTable:
        self.get_table(namespace, name)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE catalog_tables
                SET partition_strategy = ?, partition_keys_json = ?, updated_at = ?
                WHERE namespace_name = ? AND name = ?
                """,
                (
                    strategy.value,
                    _json(partition_keys),
                    _now().isoformat(),
                    namespace,
                    name,
                ),
            )
            connection.execute(
                "DELETE FROM partitions WHERE namespace_name = ? AND table_name = ?",
                (namespace, name),
            )
            connection.executemany(
                """
                INSERT INTO partitions(
                    namespace_name, table_name, partition_id, ordinal, location,
                    strategy, keys_json, size_bytes, row_count, checksum
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        namespace,
                        name,
                        partition.partition_id,
                        partition.ordinal,
                        partition.location,
                        partition.strategy.value,
                        _json(partition.keys),
                        partition.size_bytes,
                        partition.row_count,
                        partition.checksum,
                    )
                    for partition in partitions
                ],
            )
            connection.execute(
                """
                INSERT INTO table_statistics(
                    namespace_name, table_name, row_count, size_bytes, collected_at, source
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(namespace_name, table_name) DO UPDATE SET
                    row_count = excluded.row_count,
                    size_bytes = excluded.size_bytes,
                    collected_at = excluded.collected_at,
                    source = excluded.source
                """,
                (
                    namespace,
                    name,
                    statistics.row_count,
                    statistics.size_bytes,
                    statistics.collected_at.isoformat() if statistics.collected_at else None,
                    statistics.source,
                ),
            )
            connection.execute(
                """
                DELETE FROM column_statistics
                WHERE namespace_name = ? AND table_name = ?
                """,
                (namespace, name),
            )
            connection.executemany(
                """
                INSERT INTO column_statistics(
                    namespace_name, table_name, column_name, null_count, distinct_count,
                    min_value_json, max_value_json, average_size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        namespace,
                        name,
                        column.column_name,
                        column.null_count,
                        column.distinct_count,
                        _json(column.min_value) if column.min_value is not None else None,
                        _json(column.max_value) if column.max_value is not None else None,
                        column.average_size_bytes,
                    )
                    for column in statistics.columns.values()
                ],
            )
        return self.get_table(namespace, name)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    @staticmethod
    def _insert_columns(
        connection: sqlite3.Connection,
        namespace: str,
        table: str,
        fields: Iterable[SchemaField],
    ) -> None:
        connection.executemany(
            """
            INSERT INTO columns(namespace_name, table_name, ordinal, field_json)
            VALUES (?, ?, ?, ?)
            """,
            [
                (
                    namespace,
                    table,
                    ordinal,
                    field.model_dump_json(),
                )
                for ordinal, field in enumerate(fields)
            ],
        )

    @staticmethod
    def _namespace_from_row(row: sqlite3.Row) -> Namespace:
        return Namespace(
            name=row["name"],
            properties=_decode(row["properties_json"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _table_from_row(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> CatalogTable:
        namespace = str(row["namespace_name"])
        name = str(row["name"])
        column_rows = connection.execute(
            """
            SELECT field_json FROM columns
            WHERE namespace_name = ? AND table_name = ? ORDER BY ordinal
            """,
            (namespace, name),
        ).fetchall()
        partition_rows = connection.execute(
            """
            SELECT * FROM partitions
            WHERE namespace_name = ? AND table_name = ? ORDER BY ordinal
            """,
            (namespace, name),
        ).fetchall()
        statistics = self._load_statistics(connection, namespace, name)
        return CatalogTable(
            namespace=namespace,
            name=name,
            schema=Schema(
                fields=[
                    SchemaField.model_validate_json(column["field_json"])
                    for column in column_rows
                ],
                metadata=_decode(row["schema_metadata_json"]),
            ),
            format=TableFormat(row["format"]),
            location=row["location"],
            partition_strategy=PartitionStrategy(row["partition_strategy"]),
            partition_keys=_decode(row["partition_keys_json"]),
            properties=_decode(row["properties_json"]),
            partitions=[
                Partition(
                    partition_id=partition["partition_id"],
                    ordinal=partition["ordinal"],
                    location=partition["location"],
                    strategy=PartitionStrategy(partition["strategy"]),
                    keys=_decode(partition["keys_json"]),
                    size_bytes=partition["size_bytes"],
                    row_count=partition["row_count"],
                    checksum=partition["checksum"],
                )
                for partition in partition_rows
            ],
            statistics=statistics,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _load_statistics(
        connection: sqlite3.Connection,
        namespace: str,
        table: str,
    ) -> Statistics | None:
        row = connection.execute(
            """
            SELECT * FROM table_statistics
            WHERE namespace_name = ? AND table_name = ?
            """,
            (namespace, table),
        ).fetchone()
        if row is None:
            return None
        column_rows = connection.execute(
            """
            SELECT * FROM column_statistics
            WHERE namespace_name = ? AND table_name = ? ORDER BY column_name
            """,
            (namespace, table),
        ).fetchall()
        columns = {
            column["column_name"]: ColumnStatistics(
                column_name=column["column_name"],
                null_count=column["null_count"],
                distinct_count=column["distinct_count"],
                min_value=(
                    _decode(column["min_value_json"])
                    if column["min_value_json"] is not None
                    else None
                ),
                max_value=(
                    _decode(column["max_value_json"])
                    if column["max_value_json"] is not None
                    else None
                ),
                average_size_bytes=column["average_size_bytes"],
            )
            for column in column_rows
        }
        return Statistics(
            row_count=row["row_count"],
            size_bytes=row["size_bytes"],
            columns=columns,
            collected_at=(
                datetime.fromisoformat(row["collected_at"]) if row["collected_at"] else None
            ),
            source=row["source"],
        )

    @staticmethod
    def _not_found(kind: str, name: str) -> DistributedSQLError:
        return DistributedSQLError(
            ErrorCode.NOT_FOUND,
            f"{kind} {name!r} does not exist.",
            status_code=404,
            context={kind.lower(): name},
        )

    @staticmethod
    def _conflict(kind: str, name: str) -> DistributedSQLError:
        return DistributedSQLError(
            ErrorCode.CONFLICT,
            f"{kind} {name!r} already exists.",
            status_code=409,
            context={kind.lower(): name},
        )

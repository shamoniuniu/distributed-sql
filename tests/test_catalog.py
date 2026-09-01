import json
from pathlib import Path
from typing import cast

import httpx
import pyarrow as pa
import pyarrow.parquet as parquet
import pytest

from distributed_sql.common.config import CoordinatorSettings
from distributed_sql.coordinator.app import create_app


def table_request(location: Path) -> dict[str, object]:
    return {
        "name": "orders",
        "schema": {
            "fields": [
                {"name": "order_id", "data_type": "int64", "nullable": False},
                {"name": "region", "data_type": "string"},
                {"name": "amount", "data_type": "float64"},
            ]
        },
        "format": "parquet",
        "location": str(location),
        "properties": {"owner": "course"},
    }


@pytest.mark.asyncio
async def test_catalog_rest_crud_and_sqlite_persistence(tmp_path: Path) -> None:
    database = tmp_path / "catalog.db"
    settings = CoordinatorSettings(catalog_path=database)
    first_app = create_app(settings)

    async with first_app.router.lifespan_context(first_app):
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://coordinator",
        ) as client:
            namespace = await client.post(
                "/api/v1/catalog/namespaces",
                json={"name": "sales", "properties": {"owner": "analytics"}},
            )
            assert namespace.status_code == 201

            duplicate = await client.post(
                "/api/v1/catalog/namespaces",
                json={"name": "sales"},
            )
            assert duplicate.status_code == 409
            assert duplicate.json()["error"]["code"] == "CONFLICT"

            created = await client.post(
                "/api/v1/catalog/namespaces/sales/tables",
                json=table_request(tmp_path / "warehouse" / "orders"),
            )
            assert created.status_code == 201
            assert [field["name"] for field in created.json()["schema"]["fields"]] == [
                "order_id",
                "region",
                "amount",
            ]

            blocked = await client.delete("/api/v1/catalog/namespaces/sales")
            assert blocked.status_code == 409

            updated = await client.put(
                "/api/v1/catalog/namespaces/sales/tables/orders",
                json={"properties": {"owner": "finance"}},
            )
            assert updated.status_code == 200
            assert updated.json()["properties"] == {"owner": "finance"}

    second_app = create_app(settings)
    async with second_app.router.lifespan_context(second_app):
        transport = httpx.ASGITransport(app=second_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://coordinator",
        ) as client:
            persisted = await client.get(
                "/api/v1/catalog/namespaces/sales/tables/orders"
            )
            assert persisted.status_code == 200
            assert persisted.json()["properties"] == {"owner": "finance"}

            assert (
                await client.delete("/api/v1/catalog/namespaces/sales/tables/orders")
            ).status_code == 204
            assert (
                await client.delete("/api/v1/catalog/namespaces/sales")
            ).status_code == 204
            missing = await client.get("/api/v1/catalog/namespaces/sales")
            assert missing.status_code == 404
            assert missing.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("partition_key", "expected_strategy"),
    [("order_id", "hash"), (None, "round_robin")],
)
async def test_import_partitions_publishes_manifest_and_survives_restart(
    tmp_path: Path,
    partition_key: str | None,
    expected_strategy: str,
) -> None:
    database = tmp_path / f"{expected_strategy}.db"
    warehouse = tmp_path / expected_strategy / "orders"
    source = tmp_path / f"{expected_strategy}.csv"
    source.write_text(
        "order_id,region,amount\n"
        "1,north,10.5\n"
        "2,south,20.0\n"
        "3,north,30.5\n"
        "4,,40.0\n"
        "5,west,50.5\n",
        encoding="utf-8",
    )
    settings = CoordinatorSettings(catalog_path=database)
    first_app = create_app(settings)

    async with first_app.router.lifespan_context(first_app):
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://coordinator",
        ) as client:
            assert (
                await client.post(
                    "/api/v1/catalog/namespaces",
                    json={"name": "sales"},
                )
            ).status_code == 201
            assert (
                await client.post(
                    "/api/v1/catalog/namespaces/sales/tables",
                    json=table_request(warehouse),
                )
            ).status_code == 201
            imported = await client.post(
                "/api/v1/catalog/namespaces/sales/tables/orders/imports",
                json={
                    "source_location": str(source),
                    "source_format": "csv",
                    "partition_count": 3,
                    "partition_key": partition_key,
                },
            )

    assert imported.status_code == 201, imported.text
    payload = imported.json()
    table = payload["table"]
    assert table["partition_strategy"] == expected_strategy
    assert table["partition_keys"] == ([partition_key] if partition_key else [])
    assert table["statistics"]["row_count"] == 5
    assert table["statistics"]["columns"]["region"]["null_count"] == 1
    assert len(table["partitions"]) == 3
    assert sum(item["row_count"] for item in table["partitions"]) == 5

    manifest_path = Path(payload["manifest_location"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    assert manifest["table"] == "sales.orders"
    assert manifest["statistics"]["row_count"] == 5

    all_rows: list[dict[str, object]] = []
    for partition in table["partitions"]:
        partition_path = Path(partition["location"])
        assert partition_path.is_file()
        assert partition["checksum"]
        partition_table = parquet.read_table(partition_path)
        all_rows.extend(partition_table.to_pylist())
    assert sorted(cast(int, row["order_id"]) for row in all_rows) == [1, 2, 3, 4, 5]

    restarted_app = create_app(settings)
    async with restarted_app.router.lifespan_context(restarted_app):
        transport = httpx.ASGITransport(app=restarted_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://coordinator",
        ) as client:
            restored = await client.get(
                "/api/v1/catalog/namespaces/sales/tables/orders"
            )

    assert restored.status_code == 200
    restored_table = restored.json()
    assert restored_table["statistics"] == table["statistics"]
    assert restored_table["partitions"] == table["partitions"]
    restored_rows = pa.concat_tables(
        [parquet.read_table(item["location"]) for item in restored_table["partitions"]]
    )
    assert sorted(restored_rows["order_id"].to_pylist()) == [1, 2, 3, 4, 5]

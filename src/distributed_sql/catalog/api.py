"""FastAPI routes for Catalog metadata and imports."""

from fastapi import APIRouter, status

from distributed_sql.catalog.importer import DataImporter
from distributed_sql.catalog.models import (
    CatalogTable,
    ImportRequest,
    ImportResult,
    Namespace,
    NamespaceCreate,
    NamespaceList,
    NamespaceUpdate,
    TableCreate,
    TableList,
    TableUpdate,
)
from distributed_sql.catalog.repository import SQLiteCatalog


def create_catalog_router(
    catalog: SQLiteCatalog,
    importer: DataImporter,
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/catalog", tags=["catalog"])

    @router.post(
        "/namespaces",
        response_model=Namespace,
        status_code=status.HTTP_201_CREATED,
    )
    def create_namespace(request: NamespaceCreate) -> Namespace:
        return catalog.create_namespace(request)

    @router.get("/namespaces", response_model=NamespaceList)
    def list_namespaces() -> NamespaceList:
        return NamespaceList(namespaces=catalog.list_namespaces())

    @router.get("/namespaces/{namespace}", response_model=Namespace)
    def get_namespace(namespace: str) -> Namespace:
        return catalog.get_namespace(namespace)

    @router.put("/namespaces/{namespace}", response_model=Namespace)
    def update_namespace(namespace: str, request: NamespaceUpdate) -> Namespace:
        return catalog.update_namespace(namespace, request)

    @router.delete(
        "/namespaces/{namespace}",
        status_code=status.HTTP_204_NO_CONTENT,
    )
    def delete_namespace(namespace: str) -> None:
        catalog.delete_namespace(namespace)

    @router.post(
        "/namespaces/{namespace}/tables",
        response_model=CatalogTable,
        status_code=status.HTTP_201_CREATED,
    )
    def create_table(namespace: str, request: TableCreate) -> CatalogTable:
        return catalog.create_table(namespace, request)

    @router.get("/namespaces/{namespace}/tables", response_model=TableList)
    def list_tables(namespace: str) -> TableList:
        return TableList(tables=catalog.list_tables(namespace))

    @router.get(
        "/namespaces/{namespace}/tables/{table}",
        response_model=CatalogTable,
    )
    def get_table(namespace: str, table: str) -> CatalogTable:
        return catalog.get_table(namespace, table)

    @router.put(
        "/namespaces/{namespace}/tables/{table}",
        response_model=CatalogTable,
    )
    def update_table(
        namespace: str,
        table: str,
        request: TableUpdate,
    ) -> CatalogTable:
        return catalog.update_table(namespace, table, request)

    @router.delete(
        "/namespaces/{namespace}/tables/{table}",
        status_code=status.HTTP_204_NO_CONTENT,
    )
    def delete_table(namespace: str, table: str) -> None:
        catalog.delete_table(namespace, table)

    @router.post(
        "/namespaces/{namespace}/tables/{table}/imports",
        response_model=ImportResult,
        status_code=status.HTTP_201_CREATED,
    )
    def import_data(
        namespace: str,
        table: str,
        request: ImportRequest,
    ) -> ImportResult:
        return importer.import_table(namespace, table, request)

    return router

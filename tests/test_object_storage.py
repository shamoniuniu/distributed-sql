from pathlib import Path
from types import TracebackType
from urllib.request import Request

import pytest

from distributed_sql.catalog.storage import (
    LocalObjectStore,
    ObjectStoreRouter,
    S3ObjectStore,
)


def test_local_object_store_read_write_and_atomic_publish(tmp_path: Path) -> None:
    store = LocalObjectStore()
    location = str(tmp_path / "nested" / "manifest.json")

    store.write_bytes(location, b"old")
    store.publish_bytes(location, b"new")

    assert store.exists(location)
    assert store.read_bytes(location) == b"new"
    assert list((tmp_path / "nested").iterdir()) == [tmp_path / "nested" / "manifest.json"]
    store.delete(location)
    assert not store.exists(location)


def test_s3_object_store_uses_path_style_sigv4_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[Request] = []

    class Response:
        def __enter__(self) -> "Response":
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: TracebackType | None,
        ) -> None:
            del exc_type, exc_value, traceback

        def read(self) -> bytes:
            return b"payload"

    def fake_urlopen(request: Request, timeout: float) -> Response:
        assert timeout == 30
        requests.append(request)
        return Response()

    monkeypatch.setattr("distributed_sql.catalog.storage.urlopen", fake_urlopen)
    store = S3ObjectStore(
        access_key="access",
        secret_key="secret",
        endpoint="http://minio:9000",
        secure=False,
    )

    store.create_bucket("warehouse")
    store.write_bytes("s3://warehouse/a folder/object.json", b"content")
    assert store.read_bytes("s3://warehouse/a folder/object.json") == b"payload"

    assert requests[0].full_url == "http://minio:9000/warehouse/"
    assert requests[0].method == "PUT"
    assert requests[1].full_url == "http://minio:9000/warehouse/a%20folder/object.json"
    assert requests[1].method == "PUT"
    authorization = requests[1].get_header("Authorization")
    assert authorization is not None
    assert authorization.startswith("AWS4-HMAC-SHA256 Credential=access/")
    assert requests[1].get_header("X-amz-content-sha256")
    assert requests[2].method == "GET"


def test_object_store_router_rejects_unconfigured_s3() -> None:
    router = ObjectStoreRouter(LocalObjectStore())

    with pytest.raises(ValueError, match="credentials"):
        router.for_location("s3://warehouse/object")

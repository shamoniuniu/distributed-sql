"""URI-based object storage for local files and S3-compatible services."""

import hashlib
import hmac
import tempfile
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlsplit
from urllib.request import Request, urlopen
from uuid import uuid4


class ObjectStore(ABC):
    @abstractmethod
    def read_bytes(self, location: str) -> bytes:
        """Read an object in full."""

    @abstractmethod
    def write_bytes(self, location: str, data: bytes) -> None:
        """Write or replace an object."""

    @abstractmethod
    def publish_bytes(self, location: str, data: bytes) -> None:
        """Atomically publish an object at its final location."""

    @abstractmethod
    def delete(self, location: str) -> None:
        """Delete an object when it exists."""

    @abstractmethod
    def exists(self, location: str) -> bool:
        """Return whether an object exists."""


def local_path(location: str) -> Path:
    if _is_windows_path(location):
        return Path(location)
    parsed = urlsplit(location)
    if parsed.scheme not in {"", "file"}:
        raise ValueError(f"Location is not a local file URI: {location}")
    if not parsed.scheme:
        return Path(location)
    path = unquote(parsed.path)
    if parsed.netloc:
        path = f"//{parsed.netloc}{path}"
    if len(path) >= 3 and path[0] == "/" and path[2] == ":":
        path = path[1:]
    return Path(path)


class LocalObjectStore(ObjectStore):
    def read_bytes(self, location: str) -> bytes:
        return local_path(location).read_bytes()

    def write_bytes(self, location: str, data: bytes) -> None:
        path = local_path(location)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def publish_bytes(self, location: str, data: bytes) -> None:
        path = local_path(location)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            temporary.write_bytes(data)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)

    def delete(self, location: str) -> None:
        local_path(location).unlink(missing_ok=True)

    def exists(self, location: str) -> bool:
        return local_path(location).is_file()


class S3ObjectStore(ObjectStore):
    """Small path-style S3 client using AWS Signature Version 4."""

    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        endpoint: str | None = None,
        region: str = "us-east-1",
        secure: bool = True,
    ) -> None:
        scheme = "https" if secure else "http"
        self._endpoint = (endpoint or f"{scheme}://s3.amazonaws.com").rstrip("/")
        if "://" not in self._endpoint:
            self._endpoint = f"{scheme}://{self._endpoint}"
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region

    def read_bytes(self, location: str) -> bytes:
        try:
            return self._request("GET", location)
        except HTTPError as exc:
            if exc.code == 404:
                raise FileNotFoundError(location) from exc
            raise

    def create_bucket(self, bucket: str) -> None:
        if not bucket or "/" in bucket:
            raise ValueError("S3 bucket name must be a non-empty name")
        try:
            self._request("PUT", f"s3://{bucket}")
        except HTTPError as exc:
            if exc.code != 409:
                raise

    def write_bytes(self, location: str, data: bytes) -> None:
        self._request("PUT", location, data)

    def publish_bytes(self, location: str, data: bytes) -> None:
        # A successful S3 PUT makes the complete object visible atomically.
        self.write_bytes(location, data)

    def delete(self, location: str) -> None:
        try:
            self._request("DELETE", location)
        except HTTPError as exc:
            if exc.code != 404:
                raise

    def exists(self, location: str) -> bool:
        try:
            self._request("HEAD", location)
        except HTTPError as exc:
            if exc.code == 404:
                return False
            raise
        return True

    def _request(self, method: str, location: str, data: bytes | None = None) -> bytes:
        bucket, key = self._split_location(location)
        endpoint = urlsplit(self._endpoint)
        canonical_uri = f"/{quote(bucket, safe='')}/{quote(key, safe='/~')}"
        url = f"{self._endpoint}{canonical_uri}"
        now = datetime.now(UTC)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload = data or b""
        payload_hash = hashlib.sha256(payload).hexdigest()
        canonical_headers = (
            f"host:{endpoint.netloc}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{amz_date}\n"
        )
        signed_headers = "host;x-amz-content-sha256;x-amz-date"
        canonical_request = "\n".join(
            [method, canonical_uri, "", canonical_headers, signed_headers, payload_hash]
        )
        scope = f"{date_stamp}/{self._region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            ]
        )
        signing_key = self._signing_key(date_stamp)
        signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
        authorization = (
            f"AWS4-HMAC-SHA256 Credential={self._access_key}/{scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": authorization,
                "Host": endpoint.netloc,
                "x-amz-content-sha256": payload_hash,
                "x-amz-date": amz_date,
            },
        )
        with urlopen(request, timeout=30) as response:
            return cast(bytes, response.read())

    def _signing_key(self, date_stamp: str) -> bytes:
        date_key = hmac.new(
            f"AWS4{self._secret_key}".encode(),
            date_stamp.encode(),
            hashlib.sha256,
        ).digest()
        region_key = hmac.new(date_key, self._region.encode(), hashlib.sha256).digest()
        service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
        return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()

    @staticmethod
    def _split_location(location: str) -> tuple[str, str]:
        parsed = urlsplit(location)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"Invalid S3 object URI: {location}")
        return parsed.netloc, parsed.path.lstrip("/")


class ObjectStoreRouter:
    def __init__(self, local: ObjectStore, s3: ObjectStore | None = None) -> None:
        self._local = local
        self._s3 = s3

    def for_location(self, location: str) -> ObjectStore:
        if _is_windows_path(location):
            return self._local
        scheme = urlsplit(location).scheme
        if scheme in {"", "file"}:
            return self._local
        if scheme == "s3" and self._s3 is not None:
            return self._s3
        if scheme == "s3":
            raise ValueError("S3 credentials are not configured")
        raise ValueError(f"Unsupported object storage scheme: {scheme}")


def temporary_local_file(data: bytes, suffix: str) -> Path:
    handle = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    try:
        handle.write(data)
        return Path(handle.name)
    finally:
        handle.close()


def _is_windows_path(location: str) -> bool:
    return len(location) >= 3 and location[0].isalpha() and location[1:3] in {":\\", ":/"}

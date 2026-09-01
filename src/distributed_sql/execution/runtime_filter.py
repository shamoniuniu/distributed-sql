"""Serializable runtime filters produced by hash-join build inputs."""

from __future__ import annotations

import base64
import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import cast

import pyarrow as pa

from distributed_sql.planner.expressions import Expression, SQLValue


def _encode_value(value: SQLValue) -> object:
    if value is None:
        return {"type": "null"}
    if isinstance(value, bool):
        return {"type": "bool", "value": value}
    if isinstance(value, int):
        return {"type": "int", "value": str(value)}
    if isinstance(value, float):
        return {"type": "float", "value": value.hex()}
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": str(value)}
    if isinstance(value, bytes):
        return {"type": "bytes", "value": base64.b64encode(value).decode("ascii")}
    if isinstance(value, datetime):
        return {"type": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"type": "date", "value": value.isoformat()}
    return {"type": "string", "value": value}


def _decode_value(payload: object) -> SQLValue:
    item = cast(dict[str, object], payload)
    kind = cast(str, item["type"])
    value = item.get("value")
    if kind == "null":
        return None
    if kind == "bool":
        return cast(bool, value)
    if kind == "int":
        return int(cast(str, value))
    if kind == "float":
        return float.fromhex(cast(str, value))
    if kind == "decimal":
        return Decimal(cast(str, value))
    if kind == "bytes":
        return base64.b64decode(cast(str, value))
    if kind == "datetime":
        return datetime.fromisoformat(cast(str, value))
    if kind == "date":
        return date.fromisoformat(cast(str, value))
    if kind == "string":
        return cast(str, value)
    raise ValueError(f"Unsupported runtime-filter value type: {kind!r}")


def _key_bytes(values: Sequence[SQLValue]) -> bytes:
    payload = [_encode_value(value) for value in values]
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


@dataclass(slots=True)
class BloomFilter:
    bit_count: int
    hash_count: int
    bits: bytearray
    item_count: int = 0

    @classmethod
    def create(
        cls, expected_items: int, false_positive_rate: float = 0.01
    ) -> BloomFilter:
        if expected_items < 1:
            raise ValueError("expected_items must be positive")
        if not 0.0 < false_positive_rate < 1.0:
            raise ValueError("false_positive_rate must be between zero and one")
        bit_count = max(
            64,
            math.ceil(-expected_items * math.log(false_positive_rate) / math.log(2) ** 2),
        )
        hash_count = max(1, round(bit_count / expected_items * math.log(2)))
        return cls(bit_count, hash_count, bytearray(math.ceil(bit_count / 8)))

    def add(self, values: Sequence[SQLValue]) -> None:
        for bit in self._positions(values):
            self.bits[bit // 8] |= 1 << (bit % 8)
        self.item_count += 1

    def might_contain(self, values: Sequence[SQLValue]) -> bool:
        return all(self.bits[bit // 8] & (1 << (bit % 8)) for bit in self._positions(values))

    def merge(self, other: BloomFilter) -> None:
        if (self.bit_count, self.hash_count) != (other.bit_count, other.hash_count):
            raise ValueError("Bloom filters must have matching dimensions")
        for index, value in enumerate(other.bits):
            self.bits[index] |= value
        self.item_count += other.item_count

    def _positions(self, values: Sequence[SQLValue]) -> Iterable[int]:
        digest = hashlib.sha256(_key_bytes(values)).digest()
        first = int.from_bytes(digest[:16], "big")
        second = int.from_bytes(digest[16:], "big") or 1
        for index in range(self.hash_count):
            yield (first + index * second) % self.bit_count

    def to_dict(self) -> dict[str, object]:
        return {
            "bit_count": self.bit_count,
            "hash_count": self.hash_count,
            "bits": base64.b64encode(self.bits).decode("ascii"),
            "item_count": self.item_count,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> BloomFilter:
        bit_count = cast(int, payload["bit_count"])
        bits = bytearray(base64.b64decode(cast(str, payload["bits"])))
        if len(bits) != math.ceil(bit_count / 8):
            raise ValueError("Bloom filter payload has an invalid bit array size")
        return cls(
            bit_count,
            cast(int, payload["hash_count"]),
            bits,
            cast(int, payload["item_count"]),
        )


@dataclass(slots=True)
class MinMaxFilter:
    minimum: SQLValue = None
    maximum: SQLValue = None

    def add(self, value: SQLValue) -> None:
        if value is None:
            return
        if self.minimum is None or value < self.minimum:  # type: ignore[operator]
            self.minimum = value
        if self.maximum is None or value > self.maximum:  # type: ignore[operator]
            self.maximum = value

    def might_contain(self, value: SQLValue) -> bool:
        if value is None or self.minimum is None or self.maximum is None:
            return False
        try:
            return self.minimum <= value <= self.maximum  # type: ignore[operator]
        except TypeError:
            return True

    def merge(self, other: MinMaxFilter) -> None:
        self.add(other.minimum)
        self.add(other.maximum)

    def to_dict(self) -> dict[str, object]:
        return {
            "minimum": _encode_value(self.minimum),
            "maximum": _encode_value(self.maximum),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> MinMaxFilter:
        return cls(_decode_value(payload["minimum"]), _decode_value(payload["maximum"]))


@dataclass(slots=True)
class RuntimeFilter:
    bloom: BloomFilter
    ranges: tuple[MinMaxFilter, ...]

    @classmethod
    def create(
        cls,
        key_count: int,
        expected_items: int,
        false_positive_rate: float = 0.01,
    ) -> RuntimeFilter:
        if key_count < 1:
            raise ValueError("key_count must be positive")
        return cls(
            BloomFilter.create(max(expected_items, 1), false_positive_rate),
            tuple(MinMaxFilter() for _ in range(key_count)),
        )

    @classmethod
    def from_rows(
        cls,
        rows: Iterable[Mapping[str, SQLValue]],
        expressions: Sequence[Expression],
        *,
        expected_items: int,
        false_positive_rate: float = 0.01,
    ) -> RuntimeFilter:
        result = cls.create(len(expressions), expected_items, false_positive_rate)
        for row in rows:
            result.add(tuple(expression.evaluate(row) for expression in expressions))
        return result

    def add(self, values: Sequence[SQLValue]) -> None:
        if len(values) != len(self.ranges):
            raise ValueError("Runtime filter key width does not match")
        if any(value is None for value in values):
            return
        self.bloom.add(values)
        for range_filter, value in zip(self.ranges, values, strict=True):
            range_filter.add(value)

    def might_contain(self, values: Sequence[SQLValue]) -> bool:
        if len(values) != len(self.ranges) or any(value is None for value in values):
            return False
        return all(
            range_filter.might_contain(value)
            for range_filter, value in zip(self.ranges, values, strict=True)
        ) and self.bloom.might_contain(values)

    def merge(self, other: RuntimeFilter) -> None:
        if len(self.ranges) != len(other.ranges):
            raise ValueError("Runtime filters must have matching key widths")
        self.bloom.merge(other.bloom)
        for target, source in zip(self.ranges, other.ranges, strict=True):
            target.merge(source)

    def to_bytes(self) -> bytes:
        payload = {
            "version": 1,
            "bloom": self.bloom.to_dict(),
            "ranges": [item.to_dict() for item in self.ranges],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

    @classmethod
    def from_bytes(cls, payload: bytes) -> RuntimeFilter:
        raw = cast(dict[str, object], json.loads(payload))
        if raw.get("version") != 1:
            raise ValueError("Unsupported runtime filter version")
        return cls(
            BloomFilter.from_dict(cast(dict[str, object], raw["bloom"])),
            tuple(
                MinMaxFilter.from_dict(cast(dict[str, object], item))
                for item in cast(list[object], raw["ranges"])
            ),
        )


@dataclass(slots=True)
class RuntimeFilterChannel:
    value: RuntimeFilter | None = None

    def publish(self, runtime_filter: RuntimeFilter) -> None:
        self.value = runtime_filter


@dataclass(frozen=True, slots=True)
class RuntimeFilterBinding:
    expressions: tuple[Expression, ...]
    runtime_filter: RuntimeFilter | RuntimeFilterChannel

    def resolved(self) -> RuntimeFilter | None:
        if isinstance(self.runtime_filter, RuntimeFilterChannel):
            return self.runtime_filter.value
        return self.runtime_filter


@dataclass(slots=True)
class RuntimeFilterMetrics:
    input_rows: int = 0
    output_rows: int = 0
    input_batches: int = 0
    output_batches: int = 0
    filters_applied: int = 0

    @property
    def filtered_rows(self) -> int:
        return self.input_rows - self.output_rows

    def add(self, other: RuntimeFilterMetrics) -> RuntimeFilterMetrics:
        return RuntimeFilterMetrics(
            self.input_rows + other.input_rows,
            self.output_rows + other.output_rows,
            self.input_batches + other.input_batches,
            self.output_batches + other.output_batches,
            self.filters_applied + other.filters_applied,
        )


def apply_runtime_filters(
    batch: pa.RecordBatch,
    bindings: Sequence[RuntimeFilterBinding],
) -> tuple[pa.RecordBatch, int]:
    resolved = [
        (binding.expressions, runtime_filter)
        for binding in bindings
        if (runtime_filter := binding.resolved()) is not None
    ]
    if not resolved:
        return batch, 0
    rows = []
    for raw_row in batch.to_pylist():
        row = cast(Mapping[str, SQLValue], raw_row)
        if all(
            runtime_filter.might_contain(
                tuple(expression.evaluate(row) for expression in expressions)
            )
            for expressions, runtime_filter in resolved
        ):
            rows.append(raw_row)
    return pa.RecordBatch.from_pylist(rows, schema=batch.schema), len(resolved)


def runtime_filter_is_safe(join_type: str, build_side: str) -> bool:
    if join_type == "inner":
        return True
    probe_side = "right" if build_side == "left" else "left"
    preserved_sides = {
        "left": {"left"},
        "right": {"right"},
        "full": {"left", "right"},
    }.get(join_type, set())
    return probe_side not in preserved_sides

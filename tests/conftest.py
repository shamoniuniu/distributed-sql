from __future__ import annotations

from collections.abc import Iterable

import pytest

_INTEGRATION_MODULES = {
    "test_deployment.py",
    "test_distributed_execution.py",
    "test_interfaces.py",
    "test_local_cluster.py",
}
_FAULT_MODULES = {"test_worker_recovery.py"}


def pytest_collection_modifyitems(items: Iterable[pytest.Item]) -> None:
    """Give every test a stable Task 16 layer without rewriting legacy files."""

    for item in items:
        path = item.path.name
        name = item.name.casefold()
        if item.get_closest_marker("stress") is not None:
            continue
        if path in _FAULT_MODULES or any(
            token in name
            for token in ("failure", "cancel", "disk_full", "corrupt", "retry", "lease_loss")
        ):
            item.add_marker(pytest.mark.fault)
        elif path in _INTEGRATION_MODULES or "distributed" in name:
            item.add_marker(pytest.mark.integration)
        elif "duckdb" in name or "matches_reference" in name:
            item.add_marker(pytest.mark.differential)
        elif "property" in name or "invariant" in name:
            item.add_marker(pytest.mark.property)
        else:
            item.add_marker(pytest.mark.unit)

"""Pytest fixtures and plugins."""

import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def _clear_prometheus_registry() -> None:
    """Ensure tests don't fail due to duplicate Prometheus metric registration."""
    # unregister everything currently in the default registry
    collectors = list(REGISTRY._collector_to_names.keys())
    for c in collectors:
        REGISTRY.unregister(c)

"""Pytest configuration for integration tests."""

import os
from pathlib import Path

from prometheus_client import REGISTRY, generate_latest


def pytest_terminal_summary(terminalreporter, exitstatus, config) -> None:
    """Runs after all tests have completed."""
    metrics_path = Path(os.environ["DUMP_PROMETHEUS_METRICS_FILE"])
    metrics_path.write_bytes(generate_latest(REGISTRY))

    terminalreporter.write_line(f"Wrote Prometheus metrics to {metrics_path}")

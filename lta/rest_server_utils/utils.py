"""Utilities for the REST server."""

from typing import Any

import prometheus_client
from wipac_dev_tools import prometheus_tools

STATUS_POLLER_INTERVAL_MINIMUM = 30  # seconds
STATUS_POLLER_INTERVAL_LOGGING = 5 * 60  # seconds

TRANSFER_REQUESTS = "TransferRequests"
BUNDLES = "Bundles"

DatabaseType = dict[str, Any]


# -----------------------------------------------------------------------------

# Prometheus metrics
# -- make module-level so these are shared within this process (else, dups overwrite)

PROMETHEUS_HISTOGRAM = prometheus_client.Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    labelnames=("method", "route", "status"),
    buckets=prometheus_tools.HistogramBuckets.HTTP_API,
)

PROMETHEUS_STATUS_GAUGE = prometheus_client.Gauge(
    "lta_status",
    "Current count of LTA objects by collection and status",
    labelnames=("collection", "status"),
)

PROMETHEUS_QUARANTINE_GAUGE = prometheus_client.Gauge(
    "lta_status_quarantine",
    "Current count of LTA objects in quarantine by collection and original_status",
    labelnames=("collection", "original_status"),
)

PROMETHEUS_BUNDLE_CLAIMS_TOTAL = prometheus_client.Counter(
    "lta_bundle_claims_total",
    "Count of successful bundle claims",
    labelnames=("status",),
)

PROMETHEUS_STATUS_WRITES_TOTAL = prometheus_client.Counter(
    "lta_status_writes_total",
    "Count of LTA object status writes",
    labelnames=("collection", "to_status"),
)

PROMETHEUS_QUARANTINE_WRITES_TOTAL = prometheus_client.Counter(
    "lta_quarantine_writes_total",
    "Count of LTA object quarantine writes",
    labelnames=("collection", "original_status"),
)


def prometheus_record_status_write(
    collection: str,
    new_status: str,
    original_status_for_quarantine: str | None,
) -> None:
    """For Prometheus, record a write to the status field of a LTA object.

    If the new status is "quarantined", also record the original status for quarantine.
    """
    PROMETHEUS_STATUS_WRITES_TOTAL.labels(
        collection=collection,
        to_status=str(new_status),
    ).inc()

    # did the user quarantine?
    if new_status == "quarantined":
        PROMETHEUS_QUARANTINE_WRITES_TOTAL.labels(
            collection=collection,
            original_status=(
                "__unknown__"
                if original_status_for_quarantine is None
                else str(original_status_for_quarantine)
            ),
        ).inc()

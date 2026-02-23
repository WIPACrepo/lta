"""Status poller background async-task for LTA REST server metrics."""

import asyncio
import logging
from typing import Any, Callable, Mapping, Sequence

from pymongo.asynchronous.database import AsyncDatabase
from wipac_dev_tools.timing_tools import IntervalTimer

from .utils import (
    BUNDLES,
    DatabaseType,
    PROMETHEUS_QUARANTINE_GAUGE,
    PROMETHEUS_STATUS_GAUGE,
    STATUS_POLLER_INTERVAL_LOGGING,
    STATUS_POLLER_INTERVAL_MINIMUM,
    TRANSFER_REQUESTS,
)

LOGGER = logging.getLogger(__name__)


# 1) Aggregate per-status document counts for each collection
STATUS_PIPELINE: Sequence[Mapping[str, Any]] = [
    {"$match": {"status": {"$exists": True}}},
    {"$group": {"_id": "$status", "count": {"$sum": 1}}},
]

# 2) Aggregate quarantined docs by original_status (including missing/None)
QUARANTINE_BY_ORIGINAL_STATUS_PIPELINE: Sequence[Mapping[str, Any]] = [
    {"$match": {"status": "quarantined"}},
    {
        "$project": {
            "original_status_bucket": {
                "$switch": {
                    "branches": [
                        {
                            "case": {"$eq": [{"$type": "$original_status"}, "missing"]},
                            "then": "<missing>",
                        },
                        {
                            "case": {"$eq": ["$original_status", None]},
                            "then": "<none>",
                        },
                    ],
                    "default": "$original_status",
                }
            }
        }
    },
    {"$group": {"_id": "$original_status_bucket", "count": {"$sum": 1}}},
]


def _logger_if_time(logging_timer: IntervalTimer, logger: logging.Logger) -> Callable:
    # Note - we could go more generic (pass logger.info, etc.), but this is a simple case
    if logging_timer.has_interval_elapsed():
        return logger.info
    else:

        def noop(*_: Any, **__: Any) -> None:
            pass

        return noop


async def _update_gauge_from_aggregation(
    *,
    mongo_db: AsyncDatabase[DatabaseType],
    collection_name: str,
    pipeline: Sequence[Mapping[str, Any]],
    gauge: Any,
    gauge_label_name: str,
    current_labels: set[tuple[str, str]],
    log_prefix: str,
    _log: Callable[[str], None],
) -> None:
    """Run an aggregation and update one gauge from the grouped results."""
    cursor = await mongo_db[collection_name].aggregate(pipeline)
    async for doc in cursor:
        bucket_value = str(doc["_id"])
        count = int(doc["count"])
        gauge.labels(
            collection=collection_name,
            **{gauge_label_name: bucket_value},
        ).set(count)
        current_labels.add((collection_name, bucket_value))
        _log(f"{log_prefix} for {collection_name}.{bucket_value}: {count}")


def _reset_missing_gauge_labels(
    *,
    gauge: Any,
    gauge_label_name: str,
    previous_labels: set[tuple[str, str]],
    current_labels: set[tuple[str, str]],
    log_prefix: str,
    _log: Callable[[str], None],
) -> None:
    """Set gauge values to zero for labels that existed previously but disappeared."""
    for collection_name, bucket_value in previous_labels - current_labels:
        gauge.labels(
            collection=collection_name,
            **{gauge_label_name: bucket_value},
        ).set(0)
        _log(f"resetting {log_prefix} for {collection_name}.{bucket_value} to 0")


async def status_poller(
    mongo_db: AsyncDatabase[DatabaseType],
    status_poller_interval: int,
) -> None:
    """Periodically query MongoDB and update status-count gauges."""
    logging_timer = IntervalTimer(STATUS_POLLER_INTERVAL_LOGGING, None)
    status_poller_interval = max(STATUS_POLLER_INTERVAL_MINIMUM, status_poller_interval)

    # Track previously seen labels so we can zero out statuses that disappear.
    previous_status_labels: set[tuple[str, str]] = set()

    # Track previously seen quarantine original_status labels so we can zero out
    # original_status buckets that disappear.
    previous_quarantine_labels: set[tuple[str, str]] = set()

    while True:
        try:
            current_status_labels: set[tuple[str, str]] = set()
            current_quarantine_labels: set[tuple[str, str]] = set()

            for collection_name in (BUNDLES, TRANSFER_REQUESTS):
                _log = _logger_if_time(logging_timer, LOGGER)
                _log(
                    f"still alive -- cycle={status_poller_interval}s, "
                    f"logging={STATUS_POLLER_INTERVAL_LOGGING}s"
                )

                # Gauge 1: count by status
                await _update_gauge_from_aggregation(
                    mongo_db=mongo_db,
                    collection_name=collection_name,
                    pipeline=STATUS_PIPELINE,
                    gauge=PROMETHEUS_STATUS_GAUGE,
                    gauge_label_name="status",
                    current_labels=current_status_labels,
                    log_prefix="status count",
                    _log=_log,
                )

                # Gauge 2: quarantined count by original_status
                # (includes <missing> and <none> buckets)
                await _update_gauge_from_aggregation(
                    mongo_db=mongo_db,
                    collection_name=collection_name,
                    pipeline=QUARANTINE_BY_ORIGINAL_STATUS_PIPELINE,
                    gauge=PROMETHEUS_QUARANTINE_GAUGE,
                    gauge_label_name="original_status",
                    current_labels=current_quarantine_labels,
                    log_prefix="quarantine original_status count",
                    _log=_log,
                )

            # If a status existed last poll but not this poll, set it to 0.
            _reset_missing_gauge_labels(
                gauge=PROMETHEUS_STATUS_GAUGE,
                gauge_label_name="status",
                previous_labels=previous_status_labels,
                current_labels=current_status_labels,
                log_prefix="status count",
                _log=_log,
            )

            # If a quarantine original_status bucket existed last poll but not this poll, set it to 0.
            _reset_missing_gauge_labels(
                gauge=PROMETHEUS_QUARANTINE_GAUGE,
                gauge_label_name="original_status",
                previous_labels=previous_quarantine_labels,
                current_labels=current_quarantine_labels,
                log_prefix="quarantine original_status count",
                _log=_log,
            )

            previous_status_labels = current_status_labels
            previous_quarantine_labels = current_quarantine_labels

        except asyncio.CancelledError:
            LOGGER.error("Status poller cancelled")
            raise
        except Exception:
            LOGGER.exception("Failed -- sleeping then restarting")
            await asyncio.sleep(max(5 * 60, status_poller_interval))

        await asyncio.sleep(status_poller_interval)

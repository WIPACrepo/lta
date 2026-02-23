"""Status poller background async-task for LTA REST server metrics."""

import asyncio
import dataclasses
import logging
from typing import Any, Callable, Mapping, Sequence

from prometheus_client import Gauge
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


@dataclasses.dataclass
class _GaugeAggregationJob:
    """Store config and per-cycle label state for one aggregation-driven gauge."""

    pipeline: Sequence[Mapping[str, Any]]
    gauge: Any
    gauge_label_name: str
    log_prefix: str
    previous_labels: set[tuple[str, str]] = dataclasses.field(default_factory=set)
    current_labels: set[tuple[str, str]] = dataclasses.field(default_factory=set)

    def begin_cycle(self) -> None:
        """Clear labels collected during the current poll cycle."""
        self.current_labels.clear()

    def end_cycle(self) -> None:
        """Promote current labels to previous labels after the poll cycle."""
        self.previous_labels = set(self.current_labels)


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
    job: _GaugeAggregationJob,
    _log: Callable[[str], None],
) -> None:
    """Run one aggregation, update the gauge, and reset disappeared labels for this collection."""
    cursor = await mongo_db[collection_name].aggregate(job.pipeline)

    current_collection_labels: set[tuple[str, str]] = set()

    async for doc in cursor:
        bucket_value = str(doc["_id"])
        count = int(doc["count"])
        job.gauge.labels(
            collection=collection_name,
            **{job.gauge_label_name: bucket_value},
        ).set(count)

        current_collection_labels.add((collection_name, bucket_value))
        job.current_labels.add((collection_name, bucket_value))
        _log(f"{job.log_prefix} for {collection_name}.{bucket_value}: {count}")

    for _collection_name, bucket_value in job.previous_labels - job.current_labels:
        if _collection_name != collection_name:
            continue

        job.gauge.labels(
            collection=collection_name,
            **{job.gauge_label_name: bucket_value},
        ).set(0)
        _log(f"resetting {job.log_prefix} for {collection_name}.{bucket_value} to 0")


async def status_poller(
    mongo_db: AsyncDatabase[DatabaseType],
    status_poller_interval: int,
) -> None:
    """Periodically query MongoDB and update status-count gauges."""
    logging_timer = IntervalTimer(STATUS_POLLER_INTERVAL_LOGGING, None)
    status_poller_interval = max(STATUS_POLLER_INTERVAL_MINIMUM, status_poller_interval)

    jobs = [
        _GaugeAggregationJob(
            STATUS_PIPELINE,
            PROMETHEUS_STATUS_GAUGE,
            "status",
            "status count",
        ),
        _GaugeAggregationJob(
            QUARANTINE_BY_ORIGINAL_STATUS_PIPELINE,
            PROMETHEUS_QUARANTINE_GAUGE,
            "original_status",
            "quarantine original_status count",
        ),
    ]

    while True:
        try:
            _log = _logger_if_time(logging_timer, LOGGER)
            _log(
                f"still alive -- cycle={status_poller_interval}s, "
                f"logging={STATUS_POLLER_INTERVAL_LOGGING}s"
            )

            for j in jobs:
                j.begin_cycle()
                for collection_name in (BUNDLES, TRANSFER_REQUESTS):
                    await _update_gauge_from_aggregation(
                        mongo_db=mongo_db,
                        collection_name=collection_name,
                        job=j,
                        _log=_log,
                    )
                j.end_cycle()

        except asyncio.CancelledError:
            LOGGER.error("Status poller cancelled")
            raise
        except Exception:
            LOGGER.exception("Failed -- sleeping then restarting")
            await asyncio.sleep(max(5 * 60, status_poller_interval))

        await asyncio.sleep(status_poller_interval)

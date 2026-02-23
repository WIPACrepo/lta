"""Status poller background async-task for LTA REST server metrics."""

import asyncio
import dataclasses
import logging
from typing import Any, Mapping, Sequence

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
    """Store config and previous per-collection bucket labels for one gauge."""

    pipeline: Sequence[Mapping[str, Any]]
    gauge: Gauge
    gauge_label_name: str
    previous_label_values_by_collection: dict[str, set[str]] = dataclasses.field(
        default_factory=dict
    )


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


async def _update_gauge_from_aggregation(
    *,
    mongo_db: AsyncDatabase[DatabaseType],
    collection_name: str,
    job: _GaugeAggregationJob,
    do_log: bool,
) -> None:
    """Run one aggregation, update the gauge, and reset disappeared labels."""
    cursor = await mongo_db[collection_name].aggregate(job.pipeline)

    previous_label_values = job.previous_label_values_by_collection.get(
        collection_name, set()
    )
    current_label_values: set[str] = set()

    # update the gauge for each bucket
    async for result in cursor:
        label_value = str(result["_id"])
        count = int(result["count"])
        job.gauge.labels(
            collection=collection_name,
            **{job.gauge_label_name: label_value},
        ).set(count)
        current_label_values.add(label_value)
        if do_log:
            LOGGER.info(f"{job.gauge} for {collection_name}.{label_value}: {count}")

    # reset any buckets that disappeared from DB, to zero
    for label_value in previous_label_values - current_label_values:
        job.gauge.labels(
            collection=collection_name,
            **{job.gauge_label_name: label_value},
        ).set(0)
        if do_log:
            LOGGER.info(f"resetting {job.gauge} for {collection_name}.{label_value}: 0")

    job.previous_label_values_by_collection[collection_name] = current_label_values


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
        ),
        _GaugeAggregationJob(
            QUARANTINE_BY_ORIGINAL_STATUS_PIPELINE,
            PROMETHEUS_QUARANTINE_GAUGE,
            "original_status",
        ),
    ]

    while True:
        try:
            if do_log := logging_timer.has_interval_elapsed():
                LOGGER.info(
                    f"still alive -- cycle={status_poller_interval}s, "
                    f"logging={STATUS_POLLER_INTERVAL_LOGGING}s"
                )

            for j in jobs:
                for collection_name in (BUNDLES, TRANSFER_REQUESTS):
                    await _update_gauge_from_aggregation(
                        mongo_db=mongo_db,
                        collection_name=collection_name,
                        job=j,
                        do_log=do_log,
                    )

        except asyncio.CancelledError:
            LOGGER.error("Status poller cancelled")
            raise
        except Exception:
            LOGGER.exception("Failed -- sleeping then restarting")
            await asyncio.sleep(max(5 * 60, status_poller_interval))

        await asyncio.sleep(status_poller_interval)

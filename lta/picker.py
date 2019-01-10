# picker.py

import asyncio
from datetime import datetime
from typing import Dict
from .config import from_environment
from requests_futures.sessions import FuturesSession  # type: ignore
from logging import Logger
import logging
import requests
from .log_format import StructuredFormatter
from urllib.parse import urljoin

EXPECTED_CONFIG = [
    "FILE_CATALOG_REST_URL",
    "HEARTBEAT_SLEEP_DURATION_SECONDS",
    "LTA_REST_URL",
    "PICKER_NAME",
    "WORK_SLEEP_DURATION_SECONDS"
]

EXPECTED_STATE = [
    "file_catalog_ok",
    "last_work_begin_timestamp",
    "last_work_end_timestamp",
    "lta_ok"
]


class Picker:
    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        # validate provided configuration
        for name in EXPECTED_CONFIG:
            if name not in config:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")
        # assimilate provided configuration
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.heartbeat_sleep_duration_seconds = int(config["HEARTBEAT_SLEEP_DURATION_SECONDS"])
        self.lta_rest_url = config["LTA_REST_URL"]
        self.picker_name = config["PICKER_NAME"]
        self.work_sleep_duration_seconds = int(config["WORK_SLEEP_DURATION_SECONDS"])
        # assimilate provided logger
        self.logger = logger
        # record some default state
        timestamp = datetime.utcnow().isoformat()
        self.file_catalog_ok = False
        self.last_work_begin_timestamp = timestamp
        self.last_work_end_timestamp = timestamp
        self.lta_ok = False
        self.logger.info(f"Picker '{self.picker_name}' is configured:")
        for name in EXPECTED_CONFIG:
            self.logger.info(f"{name} = {config[name]}")

    async def run(self) -> None:
        self.logger.info("Starting picker work cycle")
        # start the work cycle stopwatch
        self.last_work_begin_timestamp = datetime.utcnow().isoformat()
        try:
            # TODO: Some actual work
            # 1. Ask the REST DB for the next TransferRequest to be picked
            # 2. Ask the File Catalog about the files indicated by the TransferRequest
            # 3. Update the REST DB with Files needed for bundling
            # 4. Return the TransferRequest to the REST DB as picked
            # 5. Repeat again starting at Step 1
            pass

        except Exception as e:
            # ut oh, something went wrong; log about it
            self.logger.error("Error occurred during the Picker work cycle")
            self.logger.error(e, exc_info=True)
        # stop the work cycle stopwatch
        self.last_work_end_timestamp = datetime.utcnow().isoformat()
        self.logger.info("Ending picker work cycle")

    # -----------------------------------------------------------------------


async def patch_status_heartbeat(picker: Picker) -> bool:
    picker.logger.info("Sending status heartbeat")
    # determine which resource to PATCH
    status_url = urljoin(picker.lta_rest_url, "/status/picker")
    # determine the body to PATCH with
    status_body = {
        picker.picker_name: {
            "timestamp": datetime.utcnow().isoformat()
        }
    }
    for name in EXPECTED_STATE:
        status_body[picker.picker_name][name] = getattr(picker, name)  # smh; picker[name]
    # attempt to PATCH the status resource
    picker.logger.info(f"PATCH {status_url} - {status_body}")
    try:
        session = FuturesSession()
        # r = requests.patch(status_url, data=status_body)
        r = await asyncio.wrap_future(session.patch(status_url, data=status_body))
        if (r.status_code < 200) or (r.status_code > 299):
            picker.logger.error("Unable to PATCH /status/picker with heartbeat")
            picker.lta_ok = False
            return picker.lta_ok
        picker.lta_ok = True
    except requests.exceptions.ConnectionError:
        picker.logger.error("ConnectionError trying to PATCH /status/picker with heartbeat")
        picker.lta_ok = False
    # indicate to the caller if the heartbeat was successful
    return picker.lta_ok


async def status_loop(picker: Picker) -> None:
    picker.logger.info("Starting status loop")
    while True:
        # PATCH /status/picker
        await patch_status_heartbeat(picker)
        # sleep until we PATCH the next heartbeat
        await asyncio.sleep(picker.heartbeat_sleep_duration_seconds)


async def work_loop(picker: Picker) -> None:
    picker.logger.info("Starting work loop")
    while True:
        # Do the work of the picker
        await picker.run()
        # sleep until we need to work again
        await asyncio.sleep(picker.work_sleep_duration_seconds)


def main() -> None:
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Picker',
        component_name=config["PICKER_NAME"],
        ndjson=False)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.picker")
    # create our Picker service
    picker = Picker(config, logger)
    # let's get to work
    picker.logger.info("Starting asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(picker))
    loop.create_task(work_loop(picker))
    loop.run_forever()


if __name__ == "__main__":
    main()

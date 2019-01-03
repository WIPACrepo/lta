# picker.py

import asyncio
from datetime import datetime
from lta.config import from_environment
from requests_futures.sessions import FuturesSession
from logging import getLogger
import requests
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
    def __init__(self, config, logger):
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

    async def run(self):
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

    # -----------------------------------------------------------------------


async def patch_status_heartbeat(picker):
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
    try:
        session = FuturesSession()
        # r = requests.patch(status_url, data=status_body)
        r = await asyncio.wrap_future(session.patch(status_url, data=status_body))
        if (r.status_code < 200) or (r.status_code > 299):
            picker.logger.error("Unable to PATCH /status/picker with heartbeat")
            picker.lta_ok = False
            return
        picker.lta_ok = True
    except requests.exceptions.ConnectionError:
        picker.logger.error("ConnectionError trying to PATCH /status/picker with heartbeat")
        picker.lta_ok = False
    # indicate to the caller if the heartbeat was successful
    return picker.lta_ok


async def picker_status_loop(picker):
    # until somebody kills this process dead
    while True:
        # PATCH /status/picker
        await patch_status_heartbeat(picker)
        # sleep until we PATCH the next heartbeat
        await asyncio.sleep(picker.heartbeat_sleep_duration_seconds)


async def main():
    # create our Picker service
    config = from_environment(EXPECTED_CONFIG)
    logger = getLogger("lta.picker")
    picker = Picker(config, logger)
    # start the heartbeat thread
    asyncio.get_event_loop().call_soon(picker_status_loop, picker)
    # until somebody kills this process dead
    while True:
        # do our picker work
        await picker.run()
        # then sleep until we should run again
        await asyncio.sleep(picker.sleep_duration_seconds)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())

# picker.py

from datetime import datetime
import requests
import time
from urllib.parse import urljoin

EXPECTED_CONFIG = [
    "FILE_CATALOG_REST_URL",
    "LTA_REST_URL",
    "PICKER_NAME",
    "SLEEP_DURATION_SECONDS"
]


class Picker:
    def __init__(self, config, logger):
        # validate provided configuration
        for name in EXPECTED_CONFIG:
            if name not in config:
                raise ValueError(f"Missing expected configuration parameter: '{name}'")
        # assimilate provided configuration
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.lta_rest_url = config["LTA_REST_URL"]
        self.picker_name = config["PICKER_NAME"]
        self.sleep_duration_seconds = int(config["SLEEP_DURATION_SECONDS"])
        # assimilate provided logger
        self.logger = logger
        # record some default state
        self.file_catalog_OK = False
        self.last_work = None
        self.lta_OK = False
        self.work_duration = 0

    def run(self):
        # start the work cycle stopwatch
        start_work = time.perf_counter_ns()
        try:
            # inform the LTA database that we're alive and working
            self._patch_status_heartbeat()

            # TODO: Some actual work
            # 2. Ask the REST DB for the next TransferRequest to be picked
            #     1. If none, then PATCH a status heartbeat and sleep.
            # 3. Ask the File Catalog about the files indicated by the TransferRequest
            # 4. Update the REST DB with Files needed for bundling
            # 5. Return the TransferRequest to the REST DB as picked
            # 6. Repeat again starting at Step 1

            # TODO: Timing for Main Work Loop
            # https://docs.python.org/3/library/asyncio-task.html
            pass

        except Exception as e:
            # ut oh, something went wrong; log about it
            self.logger.error("Error occurred during the Picker work cycle")
            self.logger.error(e, exc_info=True)
        # stop the work cycle stopwatch
        stop_work = time.perf_counter_ns()
        # record when we finished this work cycle
        self.last_work = datetime.utcnow().isoformat()
        # record how long the work cycle lasted in nanoseconds
        self.work_duration = (stop_work - start_work)

    # -----------------------------------------------------------------------

    def _patch_status_heartbeat(self):
        status_url = urljoin(self.lta_rest_url, "/status/picker")
        status_body = {}
        status_body[self.picker_name] = {
            "t": datetime.utcnow().isoformat(),
            "fc": self.file_catalog_OK,
            "lta": self.lta_OK,
            "last_work": self.last_work,
            "work_duration": self.work_duration
        }
        try:
            r = requests.patch(status_url, data=status_body)
            if (r.status_code < 200) or (r.status_code > 299):
                self.logger.error("Unable to PATCH /status/picker with heartbeat")
                self.lta_OK = False
                return
            self.lta_OK = True
        except requests.exceptions.ConnectionError:
            self.logger.error("ConnectionError trying to PATCH /status/picker with heartbeat")
            self.lta_OK = False
        # indicate to the caller if the heartbeat was successful
        return self.lta_OK

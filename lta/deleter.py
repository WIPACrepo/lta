# deleter.py
"""Module to implement the Deleter component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import sys
from typing import Any, Dict, List, Optional

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter
from .rucio import RucioClient


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "LTA_SITE_CONFIG": "etc/site.json",
    "RUCIO_ACCOUNT": None,
    "RUCIO_PASSWORD": None,
    "RUCIO_REST_URL": None,
    "RUCIO_RSE": None,
    "RUCIO_SCOPE": None,
    "RUCIO_USERNAME": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class Deleter(Component):
    """
    Deleter is a Long Term Archive component.

    A Deleter is responsible for deleteing intermediate copies of archive
    bundles that have finished processing at their destination site(s). The
    archive bundles are marked for deletion using the Rucio transfer service.
    Rucio will then remove the intermediate bundle files from the
    destination site(s).

    It uses the LTA DB to find verified bundles that need to be deleted.
    It de-registers the bundles with Rucio. It updates the Bundle and the
    corresponding TransferRequest in the LTA DB with a 'deleted' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Deleter component.

        config - A dictionary of required configuration values.
        logger - The object the deleter should use for logging.
        """
        super(Deleter, self).__init__("deleter", config, logger)
        self.rucio_account = config["RUCIO_ACCOUNT"]
        self.rucio_password = config["RUCIO_PASSWORD"]
        self.rucio_rest_url = config["RUCIO_REST_URL"]
        self.rucio_rse = config["RUCIO_RSE"]
        self.rucio_scope = config["RUCIO_SCOPE"]
        self.rucio_username = config["RUCIO_USERNAME"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        with open(config["LTA_SITE_CONFIG"]) as site_data:
            self.lta_site_config = json.load(site_data)
        self.sites = self.lta_site_config["sites"]
        pass

    def _do_status(self) -> Dict[str, Any]:
        """Provide additional status for the Deleter."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Provide expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        await self._consume_bundles_to_delete_from_destination_sites()

    async def _consume_bundles_to_delete_from_destination_sites(self) -> None:
        """Consume bundles from the LTA DB and delete them with Rucio."""
        # ensure that we can connect to and authenticate with Rucio
        rucio_rc = await self._get_valid_rucio_client()
        # create a client to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        # TODO: the work of the Deleter
        # 1. blah
        # 2. blah
        # inform the log that we've finished out work cycle
        self.logger.info(f"Deleter work cycle complete. Going on vacation.")

    async def _get_valid_rucio_client(self) -> RucioClient:
        """Ensure that we can connect to and authenticate with Rucio."""
        # create the RucioClient object and authenticate with Rucio
        rucio_rc = RucioClient(self.rucio_rest_url)
        await rucio_rc.auth(self.rucio_account, self.rucio_username, self.rucio_password)
        # check to see that our account authenticated properly
        r = await rucio_rc.get("/accounts/whoami")
        if r is None:
            raise Exception(f"/accounts/whoami returned None; expected dictionary")
        if isinstance(r, list):
            raise Exception(f"/accounts/whoami returned a list; expected dictionary")
        if r["status"] != "ACTIVE":
            raise Exception(f"/accounts/whoami status == '{r['status']}'; expected 'ACTIVE'")
        if r["account"] != self.rucio_account:
            raise Exception(f"/accounts/whoami account == '{r['account']}'; expected '{self.rucio_account}'")
        self.logger.info(f"Successful authentication of account '{self.rucio_account}' with Rucio.")
        # check to see that our expected RSE is present
        found_rse = False
        r = await rucio_rc.get("/rses/")
        if r is None:
            raise Exception(f"/rses/ returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"/rses/ returned a dictionary; expected a list")
        for rse in r:
            if rse["rse"] == self.rucio_rse:
                found_rse = True
                break
        if not found_rse:
            raise Exception(f"/rses/ expected RSE '{self.rucio_rse}' not found")
        self.logger.info(f"Expected Rucio Storage Element (RSE) of '{self.rucio_rse}' found.")
        # check to see that our expected datasets are present
        datasets_found: List[str] = []
        dids_scope = f"/dids/{self.rucio_scope}/"
        r = await rucio_rc.get(dids_scope)
        if r is None:
            raise Exception(f"{dids_scope} returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"{dids_scope} returned a dictionary; expected a list")
        for did in r:
            if did["scope"] == self.rucio_scope:
                if did["type"] == "DATASET":
                    datasets_found.append(did["name"])
        for site_name in self.sites.keys():
            site = self.sites[site_name]
            if not (site["rucio_dataset"] in datasets_found):
                raise Exception(f"site '{site_name}' expected Rucio to contain DATASET '{site['rucio_dataset']}'; not found")
            self.logger.info(f"Expected DATASET of '{site['rucio_dataset']}' found for site '{site_name}'.")
        # return the RucioClient to the caller, ready to use
        self.logger.info(f"Returning validated Rucio client to caller; all pre-flight checks passed.")
        return rucio_rc


def runner() -> None:
    """Configure a Deleter component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Deleter',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.deleter")
    # create our Deleter service
    deleter = Deleter(config, logger)
    # let's get to work
    deleter.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(deleter))
    loop.create_task(work_loop(deleter))


def main() -> None:
    """Configure a Deleter component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()

# deleter.py
"""Module to implement the Deleter component of the Long Term Archive."""

import asyncio
import json
from logging import Logger
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from rest_tools.client import RestClient  # type: ignore

from .component import COMMON_CONFIG, Component, status_loop, work_loop
from .config import from_environment
from .log_format import StructuredFormatter
from .lta_types import BundleType
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
        await self._consume_bundles_to_delete()

    async def _consume_bundles_to_delete(self) -> None:
        """Consume bundles from the LTA DB and delete them with Rucio."""
        # ensure that we can connect to and authenticate with Rucio
        rucio_rc = await self._get_valid_rucio_client()
        # create a client to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        # ask the LTA DB for a Bundle that needs to be deleted
        self.logger.info("Asking the LTA DB for Bundles to delete.")
        pop_body = {
            "deleter": self.name
        }
        site = self.source_site
        status = "deletable"
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?site={site}&status={status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        results = response["results"]
        if not results:
            self.logger.info(f"No Bundles are available to delete. Going on vacation.")
            return
        # delete the Bundles provided to us by the LTA DB
        await self._delete_bundles(lta_rc, rucio_rc, results)
        # inform the log that we've worked and now we're taking a break
        self.logger.info(f"Deleter work cycle complete. Going on vacation.")

    async def _delete_bundles(self,
                              lta_rc: RestClient,
                              rucio_rc: RucioClient,
                              results: List[Dict[str, Any]]) -> None:
        """Delete the Bundles provided to us by the LTA DB."""
        num_bundles = len(results)
        self.logger.info(f"LTA DB provided {num_bundles} to delete.")
        for result in results:
            await self._delete_bundle(lta_rc, rucio_rc, result)

    async def _delete_bundle(self,
                             lta_rc: RestClient,
                             rucio_rc: RucioClient,
                             bundle: Dict[str, Any]) -> None:
        """Delete the provided Bundle with Rucio and update the LTA DB."""
        # 1. remove the BUNDLE_DID from the site DEST_CONTAINER_DID
        #       rucio detach DEST_CONTAINER_DID BUNDLE_DID
        await self._detach_replica_from_dataset(rucio_rc, bundle)
        # 2. delete the bundle from the LTA DB
        uuid = bundle["uuid"]
        await lta_rc.request("DELETE", f"/Bundles/{uuid}")

    async def _detach_replica_from_dataset(self,
                                           rucio_rc: RucioClient,
                                           bundle: BundleType) -> None:
        """Detach the Bundle replica from the site specific Dataset within Rucio."""
        # detach the FILE DID from the DATASET DID within Rucio
        scope = self.rucio_scope
        dest_site = bundle["dest"]
        dataset_name = self.sites[dest_site]["rucio_dataset"]
        loc_split = bundle['location'].split(':', 1)
        loc_path = loc_split[1]
        replica_name = os.path.basename(loc_path)
        did_dict = {
            "dids": [
                {
                    "scope": scope,
                    "name": replica_name,
                },
            ],
        }
        detach_url = f"/dids/{scope}/{dataset_name}/dids"
        r = await rucio_rc.delete(detach_url, did_dict)
        if r:
            raise Exception(f"DELETE {detach_url} returned something; expected None")
        # check the DATASET DID to verify the replica as detached
        r = await rucio_rc.get(detach_url)
        if r is None:
            raise Exception(f"{detach_url} returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"{detach_url} returned a dictionary; expected a list")
        found_replica = False
        for replica in r:
            if replica["name"] == replica_name:
                found_replica = True
                break
        if found_replica:
            raise Exception(f"{detach_url} replica name found; expected name == '{replica_name}' NOT to be in the list")

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

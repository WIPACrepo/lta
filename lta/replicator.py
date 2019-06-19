# replicator.py
"""Module to implement the Replicator component of the Long Term Archive."""

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
    "RUCIO_PFN": None,
    "RUCIO_REST_URL": None,
    "RUCIO_RSE": None,
    "RUCIO_SCOPE": None,
    "RUCIO_USERNAME": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


class Replicator(Component):
    """
    Replicator is a Long Term Archive component.

    A Replicator is responsible for registering completed archive bundles
    with the Rucio transfer service. Rucio will then replicate the bundle
    from the source (i.e.: WIPAC Data Warehouse) to the destination(s),
    (i.e.: DESY, NERSC DTN).

    It uses the LTA DB to find completed bundles that need to be registered.
    It registers the bundles with Rucio. It updates the Bundle and the
    corresponding TransferRequest in the LTA DB with a 'transferring' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Replicator component.

        config - A dictionary of required configuration values.
        logger - The object the replicator should use for logging.
        """
        super(Replicator, self).__init__("replicator", config, logger)
        self.rucio_account = config["RUCIO_ACCOUNT"]
        self.rucio_password = config["RUCIO_PASSWORD"]
        self.rucio_pfn = config["RUCIO_PFN"]
        self.rucio_rest_url = config["RUCIO_REST_URL"]
        self.rucio_rse = config["RUCIO_RSE"]
        self.rucio_scope = config["RUCIO_SCOPE"]
        self.rucio_username = config["RUCIO_USERNAME"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        with open(config["LTA_SITE_CONFIG"]) as site_data:
            self.lta_site_config = json.load(site_data)
        self.sites = self.lta_site_config["sites"]

    def _do_status(self) -> Dict[str, Any]:
        """Replicator has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Replicator provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        await self._consume_bundles_to_replicate_to_destination_sites()

    async def _consume_bundles_to_replicate_to_destination_sites(self) -> None:
        """Consume bundles from the LTA DB and replicate them with Rucio."""
        # ensure that we can connect to and authenticate with Rucio
        rucio_rc = await self._get_valid_rucio_client()
        # create a client to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        # 1. Pop bundles from the LTA DB
        source = self.source_site
        response = await lta_rc.request("POST", f"/Bundles/actions/pop?site={source}&status=accessible")
        results = response["results"]
        # 2. for each bundle
        for bundle in results:
            uuid = bundle["uuid"]
            # 2.1. register the bundle as a replica within rucio
            #     rucio upload --rse $RSE --scope SCOPE --register-after-upload --pfn PFN --name NAME /PATH/TO/BUNDLE
            self._register_bundle_as_replica(rucio_rc, bundle)
            # 2.2 add the BUNDLE_DID from 2.1 to the replica
            #     rucio attach DEST_CONTAINER_DID BUNDLE_DID
            self._attach_replica_to_dataset(rucio_rc, bundle)
            # 2.3. update the Bundle in the LTA DB; registration information
            update_body = {
                "claimant": None,
                "claimed": False,
                "claim_time": None,
                "status": "transferring",
            }
            await lta_rc.request("PATCH", f"/Bundles/{uuid}", update_body)
        # inform the log that we've finished out work cycle
        self.logger.info(f"Replicator work cycle complete. Going on vacation.")

    async def _attach_replica_to_dataset(self,
                                         rucio_rc: RucioClient,
                                         bundle: BundleType) -> None:
        """Attach the Bundle replica to the site specific Dataset within Rucio."""
        # attach the FILE DID to the DATASET DID within Rucio
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
        attach_url = f"/dids/{scope}/{dataset_name}/dids"
        r = await rucio_rc.post(attach_url, did_dict)
        if r:
            raise Exception(f"POST {attach_url} returned something; expected None")
        # check the DATASET DID to verify the replica as attached
        r = await rucio_rc.get(attach_url)
        if r is None:
            raise Exception(f"{attach_url} returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"{attach_url} returned a dictionary; expected a list")
        found_replica = False
        for replica in r:
            if replica["name"] == replica_name:
                found_replica = True
                break
        if not found_replica:
            raise Exception(f"{attach_url} replica name not found; expected name == '{replica_name}' in the list")

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

    async def _register_bundle_as_replica(self,
                                          rucio_rc: RucioClient,
                                          bundle: BundleType) -> None:
        """Register the provided Bundle as a replica within Rucio."""
        loc_split = bundle['location'].split(':', 1)
        loc_path = loc_split[1]
        name = os.path.basename(loc_path)
        pfn = os.path.join(self.rucio_pfn, name)
        files = [
            {
                "scope": self.rucio_scope,
                "name": name,
                "bytes": bundle["size"],
                "adler32": bundle["checksum"]["adler32"],
                "pfn": pfn,
                "md5": bundle["checksum"]["md5"],
                "meta": {},
            },
        ]
        replicas_dict = {
            "rse": self.rucio_rse,
            "files": files,
            "ignore_availability": True,
        }
        r = await rucio_rc.post(f"/replicas/", replicas_dict)
        if r:
            raise Exception(f"POST /replicas/ returned something; expected None")
        # Query Rucio to verify that the replica has been created
        replica_url = f"/replicas/{self.rucio_scope}/{name}"
        r = await rucio_rc.get(replica_url)
        if r is None:
            raise Exception(f"{replica_url} returned None; expected a list")
        if not isinstance(r, list):
            raise Exception(f"{replica_url} returned a dictionary; expected a list")
        if len(r) != 1:
            raise Exception(f"{replica_url} returned a list of length {len(r)}; expected length 1")
        for replica in r:
            if not (replica["name"] == name):
                raise Exception(f"{replica_url} name == '{replica['name']}'; expected '{name}'")


def runner() -> None:
    """Configure a Replicator component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Replicator',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.replicator")
    # create our Replicator service
    replicator = Replicator(config, logger)
    # let's get to work
    replicator.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(replicator))
    loop.create_task(work_loop(replicator))


def main() -> None:
    """Configure a Replicator component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()

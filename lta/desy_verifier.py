# desy_verifier.py
"""Module to implement the DesyVerifier component of the Long Term Archive."""

import asyncio
from logging import Logger
import logging
import os
from subprocess import PIPE, run
import sys
from typing import Any, Dict, Optional

from rest_tools.client import RestClient  # type: ignore
from rest_tools.server import from_environment  # type: ignore

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .crypto import sha512sum
from .log_format import StructuredFormatter
from .lta_types import BundleType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "DESY_CRED_PATH": None,
    "DESY_GSIFTP": None,
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
    "WORKBOX_PATH": None,
})


def as_catalog_record(bundle_record: BundleType) -> Dict[str, Any]:
    """Cherry pick keys from a File Catalog record to include in Bundle metadata."""
    catalog_record = bundle_record.copy()
    uuids = [x["uuid"] for x in bundle_record["files"]]
    catalog_record["files"] = uuids
    return catalog_record


class DesyVerifier(Component):
    """
    DesyVerifier is a Long Term Archive component.

    A DesyVerifier uses GridFTP to copy the file from its final destination
    at DESY to a scratch directory at WIPAC. A checksum is then run on the
    scratch copy at WIPAC. If the checksums match, the file is verified as
    properly copied to the final destination at DESY.

    It uses the LTA DB to find bundles that have a 'verifying' status. After
    verifying the checksum, the Bundle is updated in the LTA DB to have a
    'completed' status.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a DesyVerifier component.

        config - A dictionary of required configuration values.
        logger - The object the desy_verifier should use for logging.
        """
        super(DesyVerifier, self).__init__("desy_verifier", config, logger)
        self.desy_cred_path = config["DESY_CRED_PATH"]
        self.desy_gsiftp = config["DESY_GSIFTP"]
        self.file_catalog_rest_token = config["FILE_CATALOG_REST_TOKEN"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.tape_base_path = config["TAPE_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["WORKBOX_PATH"]

    def _do_status(self) -> Dict[str, Any]:
        """DesyVerifier has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """DesyVerifier provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        work_claimed = True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on Bundles.")

    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be verified
        self.logger.info("Asking the LTA DB for a Bundle to verify at DESY.")
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to verify at DESY. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            if await self._verify_bundle_at_desy(lta_rc, bundle):
                await self._add_bundle_to_file_catalog(bundle)
                await self._update_bundle_in_lta_db(lta_rc, bundle)
            return True
        except Exception as e:
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:Exception during execution: {e}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        return False

    async def _add_bundle_to_file_catalog(self, bundle: BundleType) -> bool:
        """Add a FileCatalog entry for the bundle, then update existing records."""
        # configure a RestClient to talk to the File Catalog
        fc_rc = RestClient(self.file_catalog_rest_url,
                           token=self.file_catalog_rest_token,
                           timeout=self.work_timeout_seconds,
                           retries=self.work_retries)
        # determine the path where the bundle is stored on hpss
        basename = os.path.basename(bundle["bundle_path"])
        stupid_python_path = os.path.sep.join([self.tape_base_path, basename])
        desy_tape_path = os.path.normpath(stupid_python_path)
        # create a File Catalog entry for the bundle itself
        file_record = {
            "uuid": bundle["uuid"],
            "logical_name": desy_tape_path,
            "checksum": bundle["checksum"],
            "locations": [
                {
                    "site": "DESY",
                    "path": desy_tape_path,
                    "online": False,
                }
            ],
            "file_size": bundle["size"],
            # note: 'lta' is an application-private metadata field
            "lta": bundle,
        }
        # add the bundle file to the File Catalog
        try:
            self.logger.info(f"POST /api/files - {desy_tape_path}")
            await fc_rc.request("POST", "/api/files", file_record)
        except Exception as e:
            self.logger.error(f"Error: POST /api/files - {desy_tape_path}")
            self.logger.error(f"Message: {e}")
            uuid = bundle["uuid"]
            self.logger.info(f"PATCH /api/files/{uuid}")
            await fc_rc.request("PATCH", f"/api/files/{uuid}", file_record)
        # for each file contained in the bundle
        for fc_file in bundle["files"]:
            fc_file_uuid = fc_file["uuid"]
            # read the current file entry in the File Catalog
            fc_record = await fc_rc.request("GET", f"/api/files/{fc_file_uuid}")
            logical_name = fc_record["logical_name"]
            # add a location indicating the bundle archive
            new_location = {
                "locations": [
                    {
                        "site": "DESY",
                        "path": f"{desy_tape_path}:{logical_name}",
                        "archive": True,
                    }
                ]
            }
            self.logger.info(f"POST /api/files/{fc_file_uuid}/locations - {new_location}")
            # POST /api/files/{uuid}/locations will de-dupe locations for us
            await fc_rc.request("POST", f"/api/files/{fc_file_uuid}/locations", new_location)
        # indicate that our file catalog updates were successful
        return True

    async def _update_bundle_in_lta_db(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Update the LTA DB to indicate the Bundle is verified."""
        bundle_id = bundle["uuid"]
        patch_body = {
            "status": self.output_status,
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        # the morning sun has vanquished the horrible night
        return True

    async def _verify_bundle_at_desy(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Verify the checksum of a bundle from DESY."""
        # determine our work paths
        basename = os.path.basename(bundle["bundle_path"])
        stupid_python_path = os.path.sep.join([self.tape_base_path, basename])
        desy_tape_path = os.path.normpath(stupid_python_path)
        stupid_python_path = os.path.sep.join([self.workbox_path, basename])
        workbox_bundle_path = os.path.normpath(stupid_python_path)
        stupid_python_path = os.path.sep.join([self.desy_gsiftp, desy_tape_path])
        src_url = os.path.normpath(stupid_python_path)
        # use globus-url-copy to copy the file to our workbox directory
        #     -fast                  Recommended when using GridFTP servers. Use MODE E
        #                            for all data transfers, including reusing data channels
        #                            between list and transfer operations.
        #     -gridftp2              Use GridFTP v2 protocol enhancements when possible.
        #     -src-cred CRED-FILE    Set the credentials to use for source ftp connections.
        #     SOURCE-URL
        #     DESTINATION-URL
        args = ["globus-url-copy", "-fast", "-gridftp2", "-src-cred", self.desy_cred_path, src_url, workbox_bundle_path]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.error("Command to copy file from DESY via GridFTP failed")
            self.logger.info(f"Command: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            raise Exception(f"globus-url-copy {src_url} {workbox_bundle_path} Command Failed")
        # otherwise, we succeeded; verify that the file is in our workbox directory
        if not os.path.isfile(workbox_bundle_path):
            self.logger.error(f"Bundle file {workbox_bundle_path} does not exist after copying from DESY. Bad thing happen.")
            raise Exception(f"Bundle file {workbox_bundle_path} does not exist after copying from DESY. Bad thing happen.")
        # run a checksum on the bundle we copied to the workbox
        self.logger.info(f"Computing SHA512 checksum for bundle: '{workbox_bundle_path}'")
        checksum_sha512 = sha512sum(workbox_bundle_path)
        self.logger.info(f"Bundle '{workbox_bundle_path}' has SHA512 checksum '{checksum_sha512}'")
        # now we'll compare the bundle's checksum
        if bundle["checksum"]["sha512"] != checksum_sha512:
            self.logger.error(f"SHA512 checksum at the time of bundle creation: {bundle['checksum']['sha512']}")
            self.logger.error(f"SHA512 checksum of the file at the destination: {checksum_sha512}")
            self.logger.error("These checksums do NOT match, and the Bundle will NOT be verified.")
            raise Exception(f"Checksum mismatch between creation and destination: {checksum_sha512}")
        self.logger.info("Bundle checksum at DESY matches the checksum at the time of bundle creation.")
        # delete the file from the disk
        self.logger.info(f"Removing file {workbox_bundle_path} from the disk.")
        os.remove(workbox_bundle_path)
        # having passed the gauntlet, we indicate the checksums match
        return True


def runner() -> None:
    """Configure a DesyVerifier component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='DesyVerifier',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.desy_verifier")
    # create our DesyVerifier service
    desy_verifier = DesyVerifier(config, logger)
    # let's get to work
    desy_verifier.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(desy_verifier))
    loop.create_task(work_loop(desy_verifier))

def main() -> None:
    """Configure a DesyVerifier component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()

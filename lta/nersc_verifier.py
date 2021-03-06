# nersc_verifier.py
"""Module to implement the NerscVerifier component of the Long Term Archive."""

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
from .log_format import StructuredFormatter
from .lta_types import BundleType


EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_REST_TOKEN": None,
    "FILE_CATALOG_REST_URL": None,
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})


def as_catalog_record(bundle_record: BundleType) -> Dict[str, Any]:
    """Cherry pick keys from a File Catalog record to include in Bundle metadata."""
    catalog_record = bundle_record.copy()
    uuids = [x["uuid"] for x in bundle_record["files"]]
    catalog_record["files"] = uuids
    return catalog_record


class NerscVerifier(Component):
    """
    NerscVerifier is a Long Term Archive component.

    A NerscVerifier runs at the NERSC site and is responsible for issuing the
    command necessary to verify the checksum of an archive ZIP as stored in
    the High Performance Storage System (HPSS) tape system.

    See: https://docs.nersc.gov/filesystems/archive/

    It uses the LTA DB to find bundles that have a 'verifying' status. After
    issuing the HPSS command, the Bundle is updated in the LTA DB to have a
    'completed' status.

    The HSI commands used to interact with the HPSS tape system are documented
    online.

    See: http://www.mgleicher.us/index.html/hsi/hsi_reference_manual_2/hsi_commands/
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a NerscVerifier component.

        config - A dictionary of required configuration values.
        logger - The object the nersc_verifier should use for logging.
        """
        super(NerscVerifier, self).__init__("nersc_verifier", config, logger)
        self.file_catalog_rest_token = config["FILE_CATALOG_REST_TOKEN"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.tape_base_path = config["TAPE_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """NerscVerifier has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """NerscVerifier provides our expected configuration dictionary."""
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
        # 0. Do some pre-flight checks to ensure that we can do work
        # if the HPSS system is not available
        args = ["/usr/common/mss/bin/hpss_avail", "archive"]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        if completed_process.returncode != 0:
            # prevent this instance from claiming any work
            self.logger.error(f"Unable to do work; HPSS system not available (returncode: {completed_process.returncode})")
            return False
        # 1. Ask the LTA DB for the next Bundle to be verified
        self.logger.info("Asking the LTA DB for a Bundle to verify at NERSC with HPSS.")
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', '/Bundles/actions/pop?dest=NERSC&status=verifying', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to verify at NERSC with HPSS. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            if await self._verify_bundle_in_hpss(lta_rc, bundle):
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
        data_warehouse_path = bundle["path"]
        basename = os.path.basename(bundle["bundle_path"])
        stupid_python_path = os.path.sep.join([self.tape_base_path, data_warehouse_path, basename])
        hpss_path = os.path.normpath(stupid_python_path)
        # create a File Catalog entry for the bundle itself
        file_record = {
            "uuid": bundle["uuid"],
            "logical_name": hpss_path,
            "checksum": bundle["checksum"],
            "locations": [
                {
                    "site": "NERSC",
                    "path": hpss_path,
                    "hpss": True,
                    "online": False,
                }
            ],
            "file_size": bundle["size"],
            # note: 'lta' is an application-private metadata field
            "lta": bundle,
        }
        # add the bundle file to the File Catalog
        try:
            self.logger.info(f"POST /api/files - {hpss_path}")
            await fc_rc.request("POST", "/api/files", file_record)
        except Exception as e:
            self.logger.error(f"Error: POST /api/files - {hpss_path}")
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
                        "site": "NERSC",
                        "path": f"{hpss_path}:{logical_name}",
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
            "status": "completed",
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
        # the morning sun has vanquished the horrible night
        return True

    async def _verify_bundle_in_hpss(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Verify the checksum of the bundle in HPSS."""
        bundle_id = bundle["uuid"]
        # determine the path where it is stored on hpss
        data_warehouse_path = bundle["path"]
        basename = os.path.basename(bundle["bundle_path"])
        stupid_python_path = os.path.sep.join([self.tape_base_path, data_warehouse_path, basename])
        hpss_path = os.path.normpath(stupid_python_path)
        # run an hsi command to obtain the checksum of the archive as stored
        #     -P            -> ("popen" flag) - specifies that HSI is being run via popen (as a child process).
        #                      All messages (listable output,error message) are written to stdout.
        #                      HSI may not be used to pipe output to stdout if this flag is specified
        #                      It also results in setting "quiet" (no extraneous messages) mode,
        #                      disabling verbose response messages, and disabling interactive file transfer messages
        #     hashlist      -> List checksum hash for HPSS file(s)
        args = ["/usr/common/mss/bin/hsi", "-P", "hashlist", hpss_path]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.error("Command to list checksum in HPSS failed")
            self.logger.info(f"Command: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:hsi hashlist Command Failed",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return False
        # otherwise, we succeeded; output is on stderr
        # 1693e9d0273e3a2995b917c0e72e6bd2f40ea677f3613b6d57eaa14bd3a285c73e8db8b6e556b886c3929afe324bcc718711f2faddfeb43c3e030d9afe697873 sha512 /home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip [hsi]
        result = completed_process.stdout.decode("utf-8")
        lines = result.split("\n")
        cols = lines[0].split(" ")
        checksum_sha512 = cols[0]
        # now we'll compare the bundle's checksum
        if bundle["checksum"]["sha512"] != checksum_sha512:
            self.logger.error("Command to obtain bundle checksum in HPSS returned bad results")
            self.logger.info(f"SHA512 checksum at the time of bundle creation: {bundle['checksum']['sha512']}")
            self.logger.info(f"SHA512 checksum of the file at the destination: {checksum_sha512}")
            self.logger.info("These checksums do NOT match, and the Bundle will NOT be verified.")
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:Checksum mismatch between creation and destination: {checksum_sha512}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return False
        # run an hsi command to calculate the checksum of the archive as stored
        #     -P            -> ("popen" flag) - specifies that HSI is being run via popen (as a child process).
        #                      All messages (listable output,error message) are written to stdout.
        #                      HSI may not be used to pipe output to stdout if this flag is specified
        #                      It also results in setting "quiet" (no extraneous messages) mode,
        #                      disabling verbose response messages, and disabling interactive file transfer messages
        #     hashverify    -> Verify checksum hash for existing HPSS file(s)
        #     -A            -> enable auto-scheduling of retrievals
        args = ["/usr/common/mss/bin/hsi", "-P", "hashverify", "-A", hpss_path]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.error("Command to verify bundle in HPSS failed")
            self.logger.info(f"Command: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            bundle_id = bundle["uuid"]
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:hsi hashverify Command Failed",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return False
        # otherwise, we succeeded; output is on stderr
        # /home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha512) OK
        result = completed_process.stdout.decode("utf-8")
        lines = result.split("\n")
        cols = lines[0].split(" ")
        checksum_type = cols[1]
        checksum_result = cols[2]
        # now we'll compare the bundle's checksum
        if (checksum_type != '(sha512)') or (checksum_result != 'OK'):
            self.logger.error("Command to verify bundle in HPSS returned bad results")
            self.logger.info(f"Command: {completed_process.args}")
            self.logger.info(f"EXPECTED: {hpss_path}: (sha512) OK")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            self.logger.info("This result does NOT match, and the Bundle will NOT be verified.")
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:hashverify unable to verify checksum in HPSS: {result}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_id} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', patch_body)
            return False
        # having passed the gauntlet, we indicate the checksums match
        return True

def runner() -> None:
    """Configure a NerscVerifier component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='NerscVerifier',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.nersc_verifier")
    # create our NerscVerifier service
    nersc_verifier = NerscVerifier(config, logger)
    # let's get to work
    nersc_verifier.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(nersc_verifier))
    loop.create_task(work_loop(nersc_verifier))

def main() -> None:
    """Configure a NerscVerifier component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()

if __name__ == "__main__":
    main()

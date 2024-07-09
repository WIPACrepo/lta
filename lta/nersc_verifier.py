# nersc_verifier.py
"""Module to implement the NerscVerifier component of the Long Term Archive."""

import asyncio
import logging
import os
from subprocess import PIPE, run
import sys
from typing import Any, Dict, Optional

from prometheus_client import Counter, Gauge, start_http_server
from rest_tools.client import ClientCredentialsAuth, RestClient
import wipac_telemetry.tracing_tools as wtt

from .component import COMMON_CONFIG, Component, now, work_loop
from .lta_tools import from_environment
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "FILE_CATALOG_CLIENT_ID": None,
    "FILE_CATALOG_CLIENT_SECRET": None,
    "FILE_CATALOG_REST_URL": None,
    "HPSS_AVAIL_PATH": "/usr/bin/hpss_avail.py",
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

# maximum number of Metadata UUIDs to work with at a time
UPDATE_CHUNK_SIZE = 1000

# prometheus metrics
failure_counter = Counter('lta_failures', 'lta processing failures', ['component', 'level', 'type'])
load_gauge = Gauge('lta_load_level', 'lta work processed', ['component', 'level', 'type'])
success_counter = Counter('lta_successes', 'lta processing successes', ['component', 'level', 'type'])


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
        self.file_catalog_client_id = config["FILE_CATALOG_CLIENT_ID"]
        self.file_catalog_client_secret = config["FILE_CATALOG_CLIENT_SECRET"]
        self.file_catalog_rest_url = config["FILE_CATALOG_REST_URL"]
        self.hpss_avail_path = config["HPSS_AVAIL_PATH"]
        self.tape_base_path = config["TAPE_BASE_PATH"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])

    def _do_status(self) -> Dict[str, Any]:
        """NerscVerifier has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """NerscVerifier provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    @wtt.spanned()
    async def _do_work(self, lta_rc: RestClient) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        load_level = -1
        work_claimed = True
        while work_claimed:
            load_level += 1
            work_claimed = await self._do_work_claim(lta_rc)
            # if we are configured to run once and die, then die
            if self.run_once_and_die:
                sys.exit()
        load_gauge.labels(component='nersc_verifier', level='bundle', type='work').set(load_level)
        self.logger.info("Ending work on Bundles.")

    @wtt.spanned()
    async def _do_work_claim(self, lta_rc: RestClient) -> bool:
        """Claim a bundle and perform work on it."""
        # 0. Do some pre-flight checks to ensure that we can do work
        # if the HPSS system is not available
        args = [self.hpss_avail_path, "archive"]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        if completed_process.returncode != 0:
            # prevent this instance from claiming any work
            self.logger.error(f"Unable to do work; HPSS system not available (returncode: {completed_process.returncode})")
            return False
        # 1. Ask the LTA DB for the next Bundle to be verified
        self.logger.info("Asking the LTA DB for a Bundle to verify at NERSC with HPSS.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&dest={self.dest_site}&status={self.input_status}', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to verify at NERSC with HPSS. Going on vacation.")
            return False
        # process the Bundle that we were given
        try:
            if await self._verify_bundle_in_hpss(lta_rc, bundle):
                await self._add_bundle_to_file_catalog(lta_rc, bundle)
                await self._update_bundle_in_lta_db(lta_rc, bundle)
            success_counter.labels(component='nersc_verifier', level='bundle', type='work').inc()
            return True
        except Exception as e:
            failure_counter.labels(component='nersc_verifier', level='bundle', type='exception').inc()
            bundle_uuid = bundle["uuid"]
            right_now = now()
            patch_body = {
                "original_status": bundle["status"],
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:Exception during execution: {e}",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', patch_body)
        return False

    @wtt.spanned()
    async def _add_bundle_to_file_catalog(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Add a FileCatalog entry for the bundle, then update existing records."""
        # configure a RestClient to talk to the File Catalog
        fc_rc = ClientCredentialsAuth(address=self.file_catalog_rest_url,
                                      token_url=self.lta_auth_openid_url,
                                      client_id=self.file_catalog_client_id,
                                      client_secret=self.file_catalog_client_secret)
        # determine the path where the bundle is stored on hpss
        data_warehouse_path = bundle["path"]
        basename = os.path.basename(bundle["bundle_path"])
        stupid_python_path = os.path.sep.join([self.tape_base_path, data_warehouse_path, basename])
        hpss_path = os.path.normpath(stupid_python_path)
        # create a File Catalog entry for the bundle itself
        bundle_uuid = bundle["uuid"]
        right_now = now()
        file_record = {
            "uuid": bundle_uuid,
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
            "lta": {
                "date_archived": right_now,
            },
        }
        # add the bundle file to the File Catalog
        try:
            self.logger.info(f"POST /api/files - {hpss_path}")
            await fc_rc.request("POST", "/api/files", file_record)
        except Exception as e:
            self.logger.error(f"Error: POST /api/files - {hpss_path}")
            self.logger.error(f"Message: {e}")
            bundle_uuid = bundle["uuid"]
            self.logger.info(f"PATCH /api/files/{bundle_uuid}")
            await fc_rc.request("PATCH", f"/api/files/{bundle_uuid}", file_record)
        # update the File Catalog for each file contained in the bundle
        await self._update_files_in_file_catalog(fc_rc, lta_rc, bundle, hpss_path)
        # indicate that our file catalog updates were successful
        return True

    @wtt.spanned()
    async def _update_bundle_in_lta_db(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Update the LTA DB to indicate the Bundle is verified."""
        bundle_uuid = bundle["uuid"]
        patch_body = {
            "status": self.output_status,
            "reason": "",
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', patch_body)
        # the morning sun has vanquished the horrible night
        return True

    @wtt.spanned()
    async def _update_files_in_file_catalog(self,
                                            fc_rc: RestClient,
                                            lta_rc: RestClient,
                                            bundle: BundleType,
                                            hpss_path: str) -> bool:
        """Update the file records in the File Catalog."""
        bundle_uuid = bundle["uuid"]
        count = 0
        done = False
        limit = UPDATE_CHUNK_SIZE
        # until we've finished processing all the Metadata records
        while not done:
            # ask the LTA DB for the next chunk of Metadata records
            self.logger.info(f"GET /Metadata?bundle_uuid={bundle_uuid}&limit={limit}")
            lta_response = await lta_rc.request('GET', f'/Metadata?bundle_uuid={bundle_uuid}&limit={limit}')
            results = lta_response["results"]
            num_files = len(results)
            done = (num_files == 0)
            self.logger.info(f'LTA returned {num_files} Metadata documents to process.')

            # for each Metadata record returned by the LTA DB
            for metadata_record in results:
                # load the record from the File Catalog and add the new location to the record
                count = count + 1
                file_catalog_uuid = metadata_record["file_catalog_uuid"]
                fc_response = await fc_rc.request('GET', f'/api/files/{file_catalog_uuid}')
                logical_name = fc_response["logical_name"]
                # add a location indicating the bundle archive
                new_location = {
                    "locations": [
                        {
                            "site": self.dest_site,
                            "path": f"{hpss_path}:{logical_name}",
                            "archive": True,
                        }
                    ]
                }
                self.logger.info(f"POST /api/files/{file_catalog_uuid}/locations - {new_location}")
                # POST /api/files/{uuid}/locations will de-dupe locations for us
                await fc_rc.request("POST", f"/api/files/{file_catalog_uuid}/locations", new_location)

            # if we processed any Metadata records, we can now delete them
            if num_files > 0:
                delete_query = {
                    "metadata": [x['uuid'] for x in results]
                }
                self.logger.info(f"POST /Metadata/actions/bulk_delete - {num_files} Metadata records")
                bulk_response = await lta_rc.request('POST', '/Metadata/actions/bulk_delete', delete_query)
                delete_count = bulk_response['count']
                self.logger.info(f"LTA DB reports {delete_count} Metadata records are deleted.")
                if delete_count != num_files:
                    raise Exception(f"LTA DB gave us {num_files} records to process, but we only deleted {delete_count} records! BAD MOJO!")

        # the morning sun has vanquished the horrible night
        return True

    @wtt.spanned()
    async def _verify_bundle_in_hpss(self, lta_rc: RestClient, bundle: BundleType) -> bool:
        """Verify the checksum of the bundle in HPSS."""
        bundle_uuid = bundle["uuid"]
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
        args = ["/usr/bin/hsi", "-P", "hashlist", hpss_path]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.error("Command to list checksum in HPSS failed")
            self.logger.info(f"Command: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:hsi hashlist Command Failed",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', patch_body)
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
            self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', patch_body)
            return False
        # run an hsi command to calculate the checksum of the archive as stored
        #     -P            -> ("popen" flag) - specifies that HSI is being run via popen (as a child process).
        #                      All messages (listable output,error message) are written to stdout.
        #                      HSI may not be used to pipe output to stdout if this flag is specified
        #                      It also results in setting "quiet" (no extraneous messages) mode,
        #                      disabling verbose response messages, and disabling interactive file transfer messages
        #     hashverify    -> Verify checksum hash for existing HPSS file(s)
        #     -A            -> enable auto-scheduling of retrievals
        args = ["/usr/bin/hsi", "-P", "hashverify", "-A", hpss_path]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        # if our command failed
        if completed_process.returncode != 0:
            self.logger.error("Command to verify bundle in HPSS failed")
            self.logger.info(f"Command: {completed_process.args}")
            self.logger.info(f"returncode: {completed_process.returncode}")
            self.logger.info(f"stdout: {str(completed_process.stdout)}")
            self.logger.info(f"stderr: {str(completed_process.stderr)}")
            right_now = now()
            patch_body = {
                "status": "quarantined",
                "reason": f"BY:{self.name}-{self.instance_uuid} REASON:hsi hashverify Command Failed",
                "work_priority_timestamp": right_now,
            }
            self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', patch_body)
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
            self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{patch_body}'")
            await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', patch_body)
            return False
        # having passed the gauntlet, we indicate the checksums match
        return True


async def main(nersc_verifier: NerscVerifier) -> None:
    """Execute the work loop of the NerscVerifier component."""
    LOG.info("Starting asynchronous code")
    await work_loop(nersc_verifier)
    LOG.info("Ending asynchronous code")


def main_sync() -> None:
    """Configure a NerscVerifier component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure logging for the application
    log_level = getattr(logging, config["LOG_LEVEL"].upper())
    logging.basicConfig(
        format="{asctime} [{threadName}] {levelname:5} ({filename}:{lineno}) - {message}",
        level=log_level,
        stream=sys.stdout,
        style="{",
    )
    # create our NerscVerifier service
    LOG.info("Starting synchronous code")
    nersc_verifier = NerscVerifier(config, LOG)
    # let's get to work
    metrics_port = int(config["PROMETHEUS_METRICS_PORT"])
    start_http_server(metrics_port)
    asyncio.run(main(nersc_verifier))
    LOG.info("Ending synchronous code")


if __name__ == "__main__":
    main_sync()

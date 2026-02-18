# nersc_verifier.py
"""Module to implement the NerscVerifier component of the Long Term Archive."""

# fmt:off

import asyncio
import logging
import os
from pathlib import Path
from subprocess import PIPE, run
import sys
from typing import Any, Dict, Optional

from prometheus_client import start_http_server
from rest_tools.client import RestClient

from .utils import HSICommandFailedException, InvalidChecksumException, \
    log_completed_process_outputs
from .component import COMMON_CONFIG, Component, DoWorkClaimResult, work_loop
from .utils import now
from .lta_tools import from_environment
from .lta_types import BundleType

Logger = logging.Logger

LOG = logging.getLogger(__name__)

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "HPSS_AVAIL_PATH": "/usr/bin/hpss_avail.py",
    "TAPE_BASE_PATH": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

QUARANTINE_THEN_KEEP_WORKING = [InvalidChecksumException]


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

    async def _do_work_claim(self, lta_rc: RestClient) -> DoWorkClaimResult.ReturnType:
        """Claim a bundle and perform work on it -- see super for return value meanings."""
        # 0. Do some pre-flight checks to ensure that we can do work
        # if the HPSS system is not available
        args = [self.hpss_avail_path, "archive"]
        completed_process = run(args, stdout=PIPE, stderr=PIPE)
        if completed_process.returncode != 0:
            # prevent this instance from claiming any work
            self.logger.error(f"Unable to do work; HPSS system not available (returncode: {completed_process.returncode})")
            return DoWorkClaimResult.NothingClaimed(work_cycle_directive="PAUSE")
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
            return DoWorkClaimResult.NothingClaimed("PAUSE")

        # process the Bundle that we were given
        try:
            hpss_path = self._verify_bundle_in_hpss(bundle)
            await self._update_bundle_in_lta_db(lta_rc, bundle, hpss_path)
            return DoWorkClaimResult.Successful("CONTINUE")
        except Exception as e:
            if type(e) in QUARANTINE_THEN_KEEP_WORKING:
                return DoWorkClaimResult.QuarantineNow("CONTINUE", bundle, "BUNDLE", e)
            else:
                return DoWorkClaimResult.QuarantineNow("PAUSE", bundle, "BUNDLE", e)

    async def _update_bundle_in_lta_db(
            self,
            lta_rc: RestClient,
            bundle: BundleType,
            hpss_path: Path,
    ) -> bool:
        """Update the LTA DB to indicate the Bundle is verified."""
        bundle_uuid = bundle["uuid"]
        patch_body = {
            "status": self.output_status,
            "reason": "",
            "final_dest_location": {  # overwrite existing value, we know more now
                "path": str(hpss_path),
                "hpss": True,
                "online": False,
            },
            "update_timestamp": now(),
            "claimed": False,
        }
        self.logger.info(f"PATCH /Bundles/{bundle_uuid} - '{patch_body}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_uuid}', patch_body)
        # the morning sun has vanquished the horrible night
        return True

    def _verify_bundle_in_hpss(self, bundle: BundleType) -> Path:
        """Verify the checksum of the bundle in HPSS."""
        # determine the path where it is stored on hpss
        data_warehouse_path = bundle["path"]
        basename = os.path.basename(bundle["bundle_path"])
        stupid_python_path = os.path.sep.join([self.tape_base_path, data_warehouse_path, basename])
        hpss_path = os.path.normpath(stupid_python_path)

        # "What checksum do you have in your metadata?"
        # run an hsi command to retrieve the stored checksum of the archive (does not perform checksum calculation)
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
            raise HSICommandFailedException(
                "list checksum in HPSS (hashlist)", completed_process, self.logger
            )

        # now, check that the checksum value retrieval was ok
        # 1693e9d0273e3a2995b917c0e72e6bd2f40ea677f3613b6d57eaa14bd3a285c73e8db8b6e556b886c3929afe324bcc718711f2faddfeb43c3e030d9afe697873 sha512 /home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip [hsi]
        result = completed_process.stdout.decode("utf-8")
        lines = result.split("\n")
        cols = lines[0].split(" ")
        cached_checksum_sha512 = cols[0]
        # now we'll compare the bundle's checksum
        if bundle["checksum"]["sha512"] != cached_checksum_sha512:
            log_completed_process_outputs(
                completed_process, "list checksum in HPSS (hashlist)", self.logger)
            raise InvalidChecksumException(
                bundle['checksum']['sha512'],
                cached_checksum_sha512,
                self.logger,
            )

        # "Please read the bytes from tape, compute the checksum, and compare it to what you have in metadata."
        # run an hsi command to re-calculate the checksum of the archive and *compare against* the stored value
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
            raise HSICommandFailedException(
                "verify bundle in HPSS (hashverify)", completed_process, self.logger
            )

        # now, check that the stored checksum value is consistent with the actual value
        # /home/projects/icecube/data/exp/IceCube/2018/unbiased/PFDST/1230/50145c5c-01e1-4727-a9a1-324e5af09a29.zip: (sha512) OK
        result = completed_process.stdout.decode("utf-8")
        lines = result.split("\n")
        cols = lines[0].split(" ")
        checksum_type = cols[1]
        checksum_result = cols[2]
        # now we'll compare the bundle's checksum
        if (checksum_type != '(sha512)') or (checksum_result != 'OK'):
            self.logger.info(f"EXPECTED: {hpss_path}: (sha512) OK -- ({checksum_type=} {checksum_result=})")
            log_completed_process_outputs(
                completed_process, "verify bundle checksum in HPSS (hashverify)", self.logger)
            raise InvalidChecksumException(
                bundle['checksum']['sha512'],
                "[unknown but confirmed as different by hashverify]",
                self.logger,
            )

        return Path(hpss_path)


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

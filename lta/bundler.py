# bundler.py
"""Module to implement the Bundler component of the Long Term Archive."""

import asyncio
from datetime import datetime
import json
from logging import Logger
import logging
import os
import shutil
import sys
from typing import Any, Dict, Optional
from uuid import uuid4
from zipfile import ZIP_STORED, ZipFile

from rest_tools.client import RestClient  # type: ignore
import pymysql

from .component import COMMON_CONFIG, Component, now, status_loop, work_loop
from .config import from_environment
from .crypto import lta_checksums
from .log_format import StructuredFormatter
from .lta_types import BundleType

EXPECTED_CONFIG = COMMON_CONFIG.copy()
EXPECTED_CONFIG.update({
    "BUNDLER_OUTBOX_PATH": None,
    "BUNDLER_WORKBOX_PATH": None,
    "MYSQL_DB": None,
    "MYSQL_HOST": None,
    "MYSQL_PASSWORD": None,
    "MYSQL_PORT": "3306",
    "MYSQL_USER": None,
    "WORK_RETRIES": "3",
    "WORK_TIMEOUT_SECONDS": "30",
})

class Bundler(Component):
    """
    Bundler is a Long Term Archive component.

    A Bundler is responsible for creating large ZIP64 archives of files that
    should be moved to long term archive. It requests work from the LTA REST
    API in the form of files to put into a large archive. It creates the ZIP64
    archive and moves the file to staging disk. It then updates the LTA REST
    API to indicate that the provided files were so bundled.
    """

    def __init__(self, config: Dict[str, str], logger: Logger) -> None:
        """
        Create a Bundler component.

        config - A dictionary of required configuration values.
        logger - The object the bundler should use for logging.
        """
        super(Bundler, self).__init__("bundler", config, logger)
        self.db = config["MYSQL_DB"]
        self.host = config["MYSQL_HOST"]
        self.outbox_path = config["BUNDLER_OUTBOX_PATH"]
        self.password = config["MYSQL_PASSWORD"]
        self.port = int(config["MYSQL_PORT"])
        self.user = config["MYSQL_USER"]
        self.work_retries = int(config["WORK_RETRIES"])
        self.work_timeout_seconds = float(config["WORK_TIMEOUT_SECONDS"])
        self.workbox_path = config["BUNDLER_WORKBOX_PATH"]

    def _do_status(self) -> Dict[str, Any]:
        """Bundler has no additional status to contribute."""
        return {}

    def _expected_config(self) -> Dict[str, Optional[str]]:
        """Bundler provides our expected configuration dictionary."""
        return EXPECTED_CONFIG

    # NOTE: Remove this function when JADE LTA is retired
    def _check_mysql(self) -> bool:
        """Check our connection to the configured MySQL database."""
        conn = None
        cursor = None
        db_ok = False
        # make sure we clean up after ourselves
        try:
            # connect to the database
            conn = pymysql.connect(host=self.host,
                                   user=self.user,
                                   password=self.password,
                                   database=self.db,
                                   port=self.port,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)
            # create a cursor to execute a query
            self.logger.info(f"Checking MySQL Database: {self.user}@{self.host}:{self.port}/{self.db}")
            cursor = conn.cursor()
            sql = "SELECT 1"
            cursor.execute(sql)
            result = cursor.fetchone()
            self.logger.debug(f"result: {result}")
            db_ok = True
        except Exception as e:
            self.logger.info(f"Error while checking MySQL Database: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        # return the result of our database check
        self.logger.info(f"MySQL Database Connection: {db_ok}")
        return db_ok

    async def _do_work(self) -> None:
        """Perform a work cycle for this component."""
        self.logger.info("Starting work on Bundles.")
        # NOTE: Change this next line to just = True when JADE LTA is retired
        work_claimed = self._check_mysql()  # True
        while work_claimed:
            work_claimed = await self._do_work_claim()
            work_claimed &= not self.run_once_and_die
        self.logger.info("Ending work on Bundles.")

    async def _do_work_claim(self) -> bool:
        """Claim a bundle and perform work on it."""
        # 1. Ask the LTA DB for the next Bundle to be built
        # configure a RestClient to talk to the LTA DB
        lta_rc = RestClient(self.lta_rest_url,
                            token=self.lta_rest_token,
                            timeout=self.work_timeout_seconds,
                            retries=self.work_retries)
        self.logger.info("Asking the LTA DB for a Bundle to build.")
        pop_body = {
            "claimant": f"{self.name}-{self.instance_uuid}"
        }
        response = await lta_rc.request('POST', f'/Bundles/actions/pop?source={self.source_site}&status=specified', pop_body)
        self.logger.info(f"LTA DB responded with: {response}")
        bundle = response["bundle"]
        if not bundle:
            self.logger.info("LTA DB did not provide a Bundle to build. Going on vacation.")
            return False
        # process the Bundle that we were given
        await self._do_work_bundle(lta_rc, bundle)
        return True

    async def _do_work_bundle(self, lta_rc: RestClient, bundle: BundleType) -> None:
        """Build the archive file for a bundle and update the LTA DB."""
        # 0. Get our ducks in a row about what we're doing here
        num_files = len(bundle["files"])
        source = bundle["source"]
        dest = bundle["dest"]
        self.logger.info(f"There are {num_files} Files to bundle from '{source}' to '{dest}'.")
        # 1. Create a manifest of the bundle, including all metadata
        bundle_id = bundle["uuid"]
        self.logger.info(f"Bundle archive file will be '{bundle_id}.zip'")
        metadata_dict = {
            "uuid": bundle_id,
            "component": "bundler",
            "version": 2,
            "create_timestamp": now(),
            "files": bundle["files"],
        }
        metadata_file_path = os.path.join(self.workbox_path, f"{bundle_id}.metadata.json")
        with open(metadata_file_path, mode="w") as metadata_file:
            self.logger.info(f"Writing bundle metadata to '{metadata_file_path}'")
            metadata_file.write(json.dumps(metadata_dict))
        # 2. Create a ZIP bundle by writing constituent files to it
        bundle_file_path = os.path.join(self.workbox_path, f"{bundle_id}.zip")
        self.logger.info(f"Creating bundle as ZIP archive: '{bundle_file_path}'")
        with ZipFile(bundle_file_path, mode="x", compression=ZIP_STORED, allowZip64=True) as bundle_zip:
            self.logger.info(f"Adding bundle metadata '{metadata_file_path}' to bundle '{bundle_file_path}'")
            bundle_zip.write(metadata_file_path, os.path.basename(metadata_file_path))
            self.logger.info(f"Writing {num_files} files to bundle '{bundle_file_path}'")
            file_count = 1
            for bundle_me in bundle["files"]:
                bundle_me_path = bundle_me["logical_name"]
                self.logger.info(f"Writing file {file_count}/{num_files}: '{bundle_me_path}' to bundle '{bundle_file_path}'")
                bundle_zip.write(bundle_me_path, os.path.basename(bundle_me_path))
                file_count = file_count + 1
        # 3. Clean up generated JSON metadata file
        self.logger.info(f"Deleting bundle metadata file: '{metadata_file_path}'")
        os.remove(metadata_file_path)
        self.logger.info(f"Bundle metadata '{metadata_file_path}' was deleted.")
        # 4. Compute the size of the bundle
        bundle_size = os.path.getsize(bundle_file_path)
        self.logger.info(f"Archive bundle has size {bundle_size} bytes")
        # 5. Compute the LTA checksums for the bundle
        self.logger.info(f"Computing LTA checksums for bundle: '{bundle_file_path}'")
        checksum = lta_checksums(bundle_file_path)
        self.logger.info(f"Bundle '{bundle_file_path}' has adler32 checksum '{checksum['adler32']}'")
        self.logger.info(f"Bundle '{bundle_file_path}' has SHA512 checksum '{checksum['sha512']}'")
        # 6. Determine the final destination path of the bundle
        final_bundle_path = bundle_file_path
        if self.outbox_path != self.workbox_path:
            final_bundle_path = os.path.join(self.outbox_path, f"{bundle_id}.zip")
        self.logger.info(f"Finished archive bundle will be located at: '{final_bundle_path}'")
        # 7. Update the bundle record we have with all the information we collected
        bundle["status"] = "created"
        bundle["update_timestamp"] = now()
        bundle["bundle_path"] = final_bundle_path
        bundle["size"] = bundle_size
        bundle["checksum"] = checksum
        bundle["verified"] = False
        bundle["claimed"] = False
        # 8. Add a row to the JADE-LTA database with the bundle information
        # NOTE: Remove two lines below when JADE LTA is retired
        self.logger.info(f"Adding row to MySQL Database: {self.user}@{self.host}/{self.db}")
        self._insert_jade_row(bundle)
        # 9. Move the bundle from the work box to the outbox
        if final_bundle_path != bundle_file_path:
            self.logger.info(f"Moving bundle from '{bundle_file_path}' to '{final_bundle_path}'")
            shutil.move(bundle_file_path, final_bundle_path)
        self.logger.info(f"Finished archive bundle now located at: '{final_bundle_path}'")
        # 10. Update the Bundle record in the LTA DB
        self.logger.info(f"PATCH /Bundles/{bundle_id} - '{bundle}'")
        await lta_rc.request('PATCH', f'/Bundles/{bundle_id}', bundle)

    # NOTE: Remove this function when JADE LTA is retired
    def _insert_jade_row(self, bundle: BundleType) -> None:
        """Insert a row into jade_bundle in the JADE LTA DB."""
        # make sure we clean up after ourselves
        try:
            # connect to the database
            conn = pymysql.connect(host=self.host,
                                   user=self.user,
                                   password=self.password,
                                   database=self.db,
                                   port=self.port,
                                   charset='utf8mb4',
                                   cursorclass=pymysql.cursors.DictCursor)
            # create a cursor to execute a query
            cursor = conn.cursor()
            sql = ("INSERT INTO jade_bundle ("
                   "bundle_file, capacity, checksum, "
                   "closed, date_created, date_updated, "
                   "destination, reference_uuid, size, "
                   "uuid, version, jade_host_id, "
                   "extension, jade_parent_id) "
                   "VALUES ("
                   "%s, %s, %s, "
                   "%s, %s, %s, "
                   "%s, %s, %s, "
                   "%s, %s, %s, "
                   "%s, %s)")
            now = datetime.today()
            values = [
                # autogenerated                     # jade_bundle_id
                f"{bundle['uuid']}.zip",            # bundle_file
                0,                                  # capacity
                bundle['checksum']['sha512'],       # checksum
                True,                               # closed
                now,                                # date_created
                now,                                # date_updated
                bundle['path'],                     # destination
                str(uuid4()),                       # reference_uuid
                bundle['size'],                     # size
                bundle['uuid'],                     # uuid
                1,                                  # version
                2,                                  # jade_host_id (jade-lta)
                False,                              # extension
                None,                               # jade_parent_id
            ]
            self.logger.info(f"Executing: {sql}")
            self.logger.info(f"Values: {values}")
            cursor.execute(sql, values)
            conn.commit()  # type: ignore
        except Exception as e:
            # whoops, something bad happened; log it!
            self.logger.info(f"Error while adding row to MySQL Database: {e}")
        finally:
            # close the connection to the database
            if cursor:
                cursor.close()
            if conn:
                conn.close()

def runner() -> None:
    """Configure a Bundler component from the environment and set it running."""
    # obtain our configuration from the environment
    config = from_environment(EXPECTED_CONFIG)
    # configure structured logging for the application
    structured_formatter = StructuredFormatter(
        component_type='Bundler',
        component_name=config["COMPONENT_NAME"],
        ndjson=True)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(structured_formatter)
    root_logger = logging.getLogger(None)
    root_logger.setLevel(logging.NOTSET)
    root_logger.addHandler(stream_handler)
    logger = logging.getLogger("lta.bundler")
    # create our Bundler service
    bundler = Bundler(config, logger)
    # let's get to work
    bundler.logger.info("Adding tasks to asyncio loop")
    loop = asyncio.get_event_loop()
    loop.create_task(status_loop(bundler))
    loop.create_task(work_loop(bundler))


def main() -> None:
    """Configure a Bundler component from the environment and set it running."""
    runner()
    asyncio.get_event_loop().run_forever()


if __name__ == "__main__":
    main()

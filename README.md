<!--- Top of README Badges (automated) --->
[![GitHub release (latest by date including pre-releases)](https://img.shields.io/github/v/release/WIPACrepo/lta?include_prereleases)](https://github.com/WIPACrepo/lta/) [![GitHub issues](https://img.shields.io/github/issues/WIPACrepo/lta)](https://github.com/WIPACrepo/lta/issues?q=is%3Aissue+sort%3Aupdated-desc+is%3Aopen) [![GitHub pull requests](https://img.shields.io/github/issues-pr/WIPACrepo/lta)](https://github.com/WIPACrepo/lta/pulls?q=is%3Apr+sort%3Aupdated-desc+is%3Aopen)
<!--- End of README Badges (automated) --->
# lta
Long Term Archive

## Development

### Establishing a development environment
Follow these steps to get a development environment for LTA:

    cd ~/projects
    git clone git@github.com:WIPACrepo/lta.git
    cd lta
    python3 -m venv ./env
    source env/bin/activate
    pip install --upgrade pip
    pip install .[dev]

### Helper script
There is a helper script `suru` that defines some common project
tasks.

    Try one of the following tasks:

    suru check                 # Check dependency package versions
    suru clean                 # Remove build cruft
    suru coverage              # Perform coverage analysis
    suru dist                  # Create a distribution tarball and wheel
    suru lint                  # Run static analysis tools
    suru mongo                 # Start MongoDB in Docker (test-mongo)
    suru rebuild               # Test and lint the module
    suru test                  # Test the module

The task `rebuild` doesn't really build (no need to compile Python),
but it does run static analysis tools and unit/integration tests.

### Local test environment
It is possible to test LTA locally. Use the following environment variable
to disable authentication.

    export CI_TEST_ENV="TRUE"

You'll need an instance of MongoDB for testing. If you've got Docker installed,
the command `suru mongo` will start an in-memory instance of MongoDB. If you
need to point at a different MongoDB, configure these environment variables.

    export LTA_MONGODB_DATABASE_NAME="lta"
    export LTA_MONGODB_HOST="localhost"
    export LTA_MONGODB_PORT="27017"

Some LTA components depend on the File Catalog. You'll need to set up an
instance of the File Catalog for them to talk to. The instructions for
doing that can be found in the `README.md` file of the File Catalog
repository.

    [https://github.com/WIPACrepo/file_catalog](https://github.com/WIPACrepo/file_catalog)

LTA and the File Catalog can share the test instance of MongoDB, unless you
prefer that the File Catalog have its own instance.

You can start an instance of LTA's REST server (sometimes called LTA DB) with
a helper script:

    bin/rest-server.sh

#### Testing Data
Get yourself some testing data from the Data Warehouse.

    mkdir /data/exp/IceCube/2013/filtered/PFFilt/1109
    cd /data/exp/IceCube/2013/filtered/PFFilt/1109
    scp jadenorth-3:/data/exp/IceCube/2013/filtered/PFFilt/1109/* .

A script `resources/test-data-helper.sh` can be used to register these files with the
File Catalog. Here we invoke the 'add-catalog' subcommand to add files to the
catalog at the WIPAC site.

    resources/test-data-helper.sh add-catalog WIPAC /data/exp/IceCube/2013/filtered/PFFilt/1109

#### LTA Components
A script `bin/picker.sh` is used to start a Picker component that interacts
with the LTA DB.

    bin/picker.sh

A script `bin/bundler.sh` is used to start a Bundler component that interacts
with the LTA DB.

    bin/bundler.sh

#### LTA Archival Kickoff
A script `resources/make-transfer-request.sh` can be used to POST a TransferRequest object
to the LTA DB and get the data archival process started. An example of usage
would be:

    resources/make-transfer-request.sh WIPAC NERSC /data/exp/IceCube/2013/filtered/PFFilt/1109

This creates a transfer of `/data/exp/IceCube/2013/filtered/PFFilt/1109` from
WIPAC to the destination NERSC.

#### Test Data Reset
A script `resources/test-data-reset.sh` automates this process:
- Clear files from the File Catalog
- Clear transfer requests from the LTA DB
- Register files with the File catalog
- Create a transfer request in the LTA DB

This script can be used to restart testing conditions within the system. By
default, the component scripts have 30 second delays between work cycles, so
the next test should happen automatically after 30 seconds. You can also
change this configuration with environment variables; see the scripts used
to start the component.

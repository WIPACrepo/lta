# lta
Long Term Archive

[![CircleCI](https://circleci.com/gh/WIPACrepo/lta/tree/master.svg?style=shield)](https://circleci.com/gh/WIPACrepo/lta/tree/master)

## Development

### Installing Python 3.7 on Linux Mint
Source: https://tecadmin.net/install-python-3-7-on-ubuntu-linuxmint/

    sudo apt-get install build-essential
    sudo apt-get install libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev
    wget https://www.python.org/ftp/python/3.7.2/Python-3.7.2.tar.xz
    tar xvJf Python-3.7.2.tar.xz
    cd Python-3.7.2/
    ./configure --enable-optimizations
    sudo make altinstall

### Establishing a development environment
Follow these steps to get a development environment for LTA:

    cd ~/projects
    git clone git@github.com:WIPACrepo/lta.git
    cd lta
    python3.8 -m venv ./env
    source env/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt

### Updating requirements.txt after a pip install
If you install a new package using `pip install cool-pkg-here` then
be sure to update the `requirements.txt` file with the following
command:

    pip freeze --all >requirements.txt

### Helper script
There is a helper script `snake` that defines some common project
tasks.

    Try one of the following tasks:

    snake check                # Check dependency package versions
    snake clean                # Remove build cruft
    snake coverage             # Perform coverage analysis
    snake dist                 # Create a distribution tarball and wheel
    snake docker               # Create a docker container
    snake lint                 # Run static analysis tools
    snake rebuild              # Test and lint the module
    snake test                 # Test the module

The task `rebuild` doesn't really build (no need to compile Python),
but it does run static analysis tools and unit/integration tests.

### Bumping to the next version
If you need to increase the version number of the project, don't
forget to edit the following:

    CHANGELOG.md
    lta/__init__.py

### Log Lens
There is a small helper script `loglens` that can be used to format
structured log output in a human-readable way; like you might see in a
traditional log file.

For example, running a Picker and sending its log output through the
log lens:

    bin/picker.sh | loglens

### Log Filter
There is a small helper script `logfilter` that can be used to cull
non-JSON output from structured log output. If debugging messages
are crashing the log lens, this construction may help:

    bin/picker.sh | logfilter | loglens

### Local test environment
It is possible to test LTA locally, but the setup for the local test
environment has a few steps. This section walks you through that process.

#### Install docker
For Linux Mint 18, there is a script in the resources directory:

    resources/install-docker.sh

#### Install circleci-cli
As root, the following command installs circleci-cli locally:

    curl -fLSs https://circle.ci/cli | bash

There are [other installation options](https://github.com/CircleCI-Public/circleci-cli) as well.

##### Running a local CircleCI job
This command runs a local CircleCI job:

    circleci local execute --job JOB_NAME

For LTA, `JOB_NAME` is `test`

#### MongoDB
Get a MongoDB running on port 27017.

    docker run --rm -it --network=host circleci/mongo:3.7.9-ram

#### MySQL
Get a MySQL running on port 8890:

    docker run --name jade_lta_test -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_USER=jade -e MYSQL_PASSWORD=hunter2 -e MYSQL_DATABASE=jade-lta -p 8890:3306 mysql

Then use a MySQL client to populate it with the JADE LTA database schema:

    mysql -h 127.0.0.1 -P 8890 --user jade --password jade-lta < jade-lta-schema.sql

#### Token Service
Get a token service running on port 8888:

    docker run --env auth_secret=secret --rm -it --network=host wipac/token-service:latest python test_server.py

#### File Catalog
Get a File Catalog running on port 8889.

    TODO: Provide a Vagrantfile to easily create this service
    TODO: Provide a docker command to easily create a container running this service

#### LTA DB
Get an LTA DB running on port 8080.

    docker run --rm -it --network=host --env LTA_AUTH_ALGORITHM='HS512' --env LTA_AUTH_ISSUER='http://localhost:8888' --env LTA_AUTH_SECRET='secret' wipac/lta:latest python3 -m lta.rest_server

A file `local-secret` contains the secret credentials used by the LTA DB
to secure itself. Note, this is Bring Your Own Secret (BYOS) software, so
may want to run this command to create a secret:

    dd if=/dev/urandom bs=1 count=64 2>/dev/null | base64 >local-secret

A script `bin/rest-server.sh` is used to start the LTA DB service, secured with
the local secret.

A script `resources/make-token.sh` uses the local secret to create tokens for
components to authenticate themselves with the LTA DB.

A script `resources/solicit-token.sh` asks the docker container for a token for
components to authenticate themselves with the LTA DB.

#### Testing Data
Get yourself some testing data from the Data Warehouse.

    mkdir /data/exp/IceCube/2013/filtered/PFFilt/1109
    cd /data/exp/IceCube/2013/filtered/PFFilt/1109
    scp jadenorth-2:/data/exp/IceCube/2013/filtered/PFFilt/1109/* .

A script `resources/test-data-helper.sh` can be used to register these files with the
File Catalog. Here we invoke the 'add-catalog' subcommand to add files to the
catalog at the WIPAC site.

    resources/test-data-helper.sh add-catalog WIPAC /data/exp/IceCube/2013/filtered/PFFilt/1109

#### LTA Components
A script `bin/picker.sh` is used to generate a token and start a Picker component
that interacts with the LTA DB. The output will be JSON, so to get a more
traditional log output, use the `loglens` script to translate:

    bin/picker.sh | loglens

A script `bin/bundler.sh` is used to generate a token and start a Bundler component
that interacts with the LTA DB.  The output will be JSON, so to get a more
traditional log output, use the `loglens` script to translate:

    bin/bundler.sh | loglens

#### LTA Archival Kickoff
A script `resources/make-transfer-request.sh` can be used to POST a TransferRequest object
to the LTA DB and get the data archival process started. An example of usage
would be:

    resources/make-transfer-request.sh WIPAC:/data/exp/IceCube/2013/filtered/PFFilt/1109 DESY:/data/exp/IceCube/2013/filtered/PFFilt/1109 NERSC:/data/exp/IceCube/2013/filtered/PFFilt/1109

This creates a transfer of `/data/exp/IceCube/2013/filtered/PFFilt/1109` from
WIPAC to the destinations DESY and NESRC.

#### Test Data Reset
A script `resources/test-data-reset.sh` automates this process:
- Clear files from the File Catalog
- Clear transfer requests from the LTA DB
- Register files with the File catalog
- Create a transfer request in the LTA DB

This script can be used to restart testing conditions within the system. The
component scripts have 30 second delays between work cycles, so the next test
should happen automatically after 30 seconds.

### Pushing Docker containers to local registry in Kubernetes
Here are some commands to get the Docker container pushed to our Docker
register in our Kubernetes cluster:

    kubectl -n kube-system port-forward $(kubectl get pods --namespace kube-system -l "app=docker-registry" -o jsonpath="{.items[0].metadata.name}") 5000:5000 &
    docker tag wipac/lta:0.0.5 localhost:5000/wipac/lta:0.0.5
    docker push localhost:5000/wipac/lta:0.0.5

### Obtaining an authentication token
Use the IceCube Token Service to obtain a token with the `file-catalog`
and `lta` scopes:

    https://tokens.icecube.wisc.edu/service_token?scope=file-catalog%20lta

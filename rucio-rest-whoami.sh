#!/usr/bin/env bash
export RUCIO_ACCOUNT=${RUCIO_ACCOUNT:="root"}
export RUCIO_APP_ID=${RUCIO_APP_ID:=""}
export RUCIO_PASSWORD=${RUCIO_PASSWORD:="hunter2"} # http://bash.org/?244321
export RUCIO_REST_URL=${RUCIO_REST_URL:="http://rancher-worker-1:30475"}
export RUCIO_USERNAME=${RUCIO_USERNAME:="icecube"}
python -m resources.rucio_rest_whoami $@

#!/usr/bin/env bash
export RUCIO_ACCOUNT=${RUCIO_ACCOUNT:="root"}
export RUCIO_APP_ID=${RUCIO_APP_ID:=""}
export RUCIO_PASSWORD=${RUCIO_PASSWORD:="hunter2"} # http://bash.org/?244321
export RUCIO_REST_URL=${RUCIO_REST_URL:="http://k8s-worker-1:32172"}
export RUCIO_USERNAME=${RUCIO_USERNAME:="root"}
python -m resources.rucio_workbench $@

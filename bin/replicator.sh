#!/usr/bin/env bash
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-replicator"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(resources/solicit-token.sh)"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
export LTA_SITE_CONFIG=${LTA_SITE_CONFIG:="etc/site.json"}
export RUCIO_ACCOUNT=${RUCIO_ACCOUNT:="root"}
export RUCIO_PASSWORD=${RUCIO_PASSWORD:="hunter2"} # http://bash.org/?244321
export RUCIO_PFN=${RUCIO_PFN:="gsiftp://gridftp.icecube.wisc.edu:2811/mnt/lfss/rucio-test/LTA-ND-A"}
export RUCIO_REST_URL=${RUCIO_REST_URL:="http://rancher-worker-1:30475"}
export RUCIO_RSE=${RUCIO_RSE:="LTA-ND-A"}
export RUCIO_SCOPE=${RUCIO_RSE:="lta"}
export RUCIO_USERNAME=${RUCIO_USERNAME:="icecube"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="5"}
python -m lta.replicator

#!/usr/bin/env bash
cd /global/homes/i/icecubed/NEWLTA/lta
source env/bin/activate
cd /global/homes/i/icecubed/NEWLTA/lta/bin
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-nersc-retriever"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(resources/solicit-token.sh)"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export RSE_BASE_PATH=${RSE_BASE_PATH:="/global/cscratch1/sd/icecubed/jade-disk/lta"}
export TAPE_BASE_PATH=${TAPE_BASE_PATH:="/home/projects/icecube"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export SOURCE_SITE=${SOURCE_SITE:="NERSC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.nersc_retriever

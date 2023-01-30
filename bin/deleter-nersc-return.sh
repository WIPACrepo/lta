#!/usr/bin/env bash
# export OUTPUT_STATUS=${OUTPUT_STATUS:="source-deleted"}
cd /global/homes/i/icecubed/NEWLTA/lta
source env/bin/activate
cd /global/homes/i/icecubed/NEWLTA/lta/bin
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-deleter-return"}
export DEST_SITE=${DEST_SITE:="WIPAC"}
export DISK_BASE_PATH=${DISK_BASE_PATH:="/global/cscratch1/sd/icecubed/jade-disk/lta"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="500"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
#export INPUT_STATUS=${INPUT_STATUS:="detached"}
export INPUT_STATUS=${INPUT_STATUS:="completed"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="source-deleted"}
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION="python"
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export SOURCE_SITE=${SOURCE_SITE:="NERSC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="500"}
python -m lta.deleter

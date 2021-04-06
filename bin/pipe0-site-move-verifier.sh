#!/usr/bin/env bash
cd /global/homes/i/icecubed/NEWLTA/lta
source env/bin/activate
cd /global/homes/i/icecubed/NEWLTA/lta/bin
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-pipe0-site-move-verifier"}
export DEST_ROOT_PATH=${DEST_ROOT_PATH:="/global/cscratch1/sd/icecubed/jade-disk/lta"}
export DEST_SITE=${DEST_SITE:="NERSC"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="30"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="transferring"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(<service-token)"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="taping"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export USE_FULL_BUNDLE_PATH=${USE_FULL_BUNDLE_PATH:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.site_move_verifier

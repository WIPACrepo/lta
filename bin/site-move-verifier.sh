#!/usr/bin/env bash
cd /global/homes/i/icecubed/NEWLTA/lta
source env/bin/activate
cd /global/homes/i/icecubed/NEWLTA/lta/bin
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-site-move-verifier"}
export DEST_ROOT_PATH=${DEST_ROOT_PATH:="/global/cscratch1/sd/icecubed/jade-disk/lta"}
export DEST_SITE=${DEST_SITE:="NERSC"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(resources/solicit-token.sh)"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export NEXT_STATUS=${NEXT_STATUS:="taping"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export SOURCE_SITE=${SOURCE_SITE:="ICECUBE"}
# export TRANSFER_CONFIG_PATH=${TRANSFER_CONFIG_PATH:="../examples/rucio.json"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="5"}
python -m lta.site_move_verifier

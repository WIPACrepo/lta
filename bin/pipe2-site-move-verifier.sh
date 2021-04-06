#!/usr/bin/env bash
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-pipe2-site-move-verifier"}
export DEST_SITE=${DEST_SITE:="DESY"}
#export GLOBUS_PROXY_OUTPUT=${GLOBUS_PROXY_OUTPUT:=""}
#export GLOBUS_PROXY_PASSPHRASE=${GLOBUS_PROXY_PASSPHRASE:="hunter2"}  # http://bash.org/?244321
export GLOBUS_PROXY_VOMS_ROLE=${GLOBUS_PROXY_VOMS_ROLE:="archive"}
export GLOBUS_PROXY_VOMS_VO=${GLOBUS_PROXY_VOMS_VO:="icecube"}
voms-proxy-init
export GRIDFTP_DEST_URL=${GRIDFTP_DEST_URL:="gsiftp://gridftp.zeuthen.desy.de:2811/pnfs/ifh.de/acs/icecube/archive/"}
export GRIDFTP_TIMEOUT=${GRIDFTP_TIMEOUT:="43200"}  # 43200 sec = 12 hours
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="30"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="transferring"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(<service-token)"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="verifying"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
export WORKBOX_PATH=${WORKBOX_PATH:="/mnt/lfss/jade-lta/returncheck"}
python -m lta.desy_move_verifier
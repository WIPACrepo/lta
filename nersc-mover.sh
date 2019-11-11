#!/usr/bin/env bash
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-nersc-mover"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(solicit-token.sh)"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
export RSE_BASE_PATH=${RSE_BASE_PATH:="/path/to/rse"}
export TAPE_BASE_PATH=${RSE_BASE_PATH:="/path/to/hpss"}
export SOURCE_SITE=${SOURCE_SITE:="NERSC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="5"}
python -m lta.nersc_mover
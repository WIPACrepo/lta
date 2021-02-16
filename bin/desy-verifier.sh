#!/usr/bin/env bash

# TODO: See if these variables make sense for this component
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-desy-verifier"}
export DEST_SITE=${DEST_SITE:="DESY"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="verifying"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(resources/solicit-token.sh)"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="completed"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="5"}

# TODO: See if these variables make sense for this component
export BUNDLE_DEST_PATH=${BUNDLE_DEST_PATH:="/mnt/lfss/jade-lta/bundler_out"}
export BUNDLE_SOURCE_PATH=${BUNDLE_SOURCE_PATH:="/mnt/lfss/jade-lta/bundler_stage"}

python -m lta.desy_verifier

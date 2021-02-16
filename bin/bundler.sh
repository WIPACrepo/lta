#!/usr/bin/env bash
export BUNDLER_OUTBOX_PATH=${BUNDLER_OUTBOX_PATH:="/data/user/lta/bundler_out"}
export BUNDLER_WORKBOX_PATH=${BUNDLER_WORKBOX_PATH:="/data/user/lta/bundler_work"}
export DEST_SITE=${SOURCE_SITE:="NERSC"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-bundler"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="specified"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(resources/solicit-token.sh)"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="created"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="5"}
python -m lta.bundler

#!/usr/bin/env bash
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-desy-verifier"}
export DEST_SITE=${DEST_SITE:="DESY"}
export FILE_CATALOG_REST_TOKEN=${FILE_CATALOG_REST_TOKEN:="$(resources/solicit-token.sh)"}
export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="http://127.0.0.1:8889"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="verifying"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(resources/solicit-token.sh)"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="localhost:4317"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="completed"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export TAPE_BASE_PATH=${RSE_BASE_PATH:="/pnfs/ifh.de/acs/icecube/archive"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="5"}
python -m lta.desy_verifier

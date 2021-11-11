#!/usr/bin/env bash
export BUNDLER_OUTBOX_PATH=${BUNDLER_OUTBOX_PATH:="/mnt/lfss/jade-lta/bundler_stage"}
export BUNDLER_WORKBOX_PATH=${BUNDLER_WORKBOX_PATH:="/mnt/lfss/jade-lta/bundler_work"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-pipe0-bundler"}
export DEST_SITE=${DEST_SITE:="NERSC"}
export FILE_CATALOG_REST_TOKEN=${FILE_CATALOG_REST_TOKEN:="$(<service-token)"}
export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="https://file-catalog.icecube.wisc.edu"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="30"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="specified"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(<service-token)"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="created"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.bundler

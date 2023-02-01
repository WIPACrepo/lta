#!/usr/bin/env bash
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-unpacker"}
export DEST_SITE=${DEST_SITE:="WIPAC"}
export FILE_CATALOG_REST_TOKEN=${FILE_CATALOG_REST_TOKEN:="$(solicit-token.sh)"}
export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="http://127.0.0.1:8889"}
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="5"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="unpacking"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="completed"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export SOURCE_SITE=${SOURCE_SITE:="NERSC"}
export UNPACKER_OUTBOX_PATH=${UNPACKER_OUTBOX_PATH:="/mnt/lfss/jade-lta/bundler_work"}
export UNPACKER_WORKBOX_PATH=${UNPACKER_WORKBOX_PATH:="/mnt/lfss/jade-lta/bundler_out/other/icecube-dataset"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.unpacker

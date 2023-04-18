#!/usr/bin/env bash
export BUNDLER_OUTBOX_PATH=${BUNDLER_OUTBOX_PATH:="/mnt/lfss/jade-lta/bundler_stage"}
export BUNDLER_WORKBOX_PATH=${BUNDLER_WORKBOX_PATH:="/mnt/lfss/jade-lta/bundler_work"}
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-pipe2-bundler"}
export DEST_SITE=${DEST_SITE:="DESY"}
export FILE_CATALOG_CLIENT_ID=${FILE_CATALOG_CLIENT_ID:="file-catalog"}
export FILE_CATALOG_CLIENT_SECRET=${FILE_CATALOG_CLIENT_SECRET:="$(<file-catalog-client-secret)"}
export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="https://file-catalog.icecube.wisc.edu"}
export INPUT_STATUS=${INPUT_STATUS:="specified"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="created"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export RUN_UNTIL_NO_WORK=${RUN_UNTIL_NO_WORK:="FALSE"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.bundler

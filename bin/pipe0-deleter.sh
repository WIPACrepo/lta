#!/usr/bin/env bash
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-pipe0-deleter"}
export DEST_SITE=${DEST_SITE:="NERSC"}
export DISK_BASE_PATH=${DISK_BASE_PATH:="/data/user/jadelta/ltatemp/bundler_out"}
export INPUT_STATUS=${INPUT_STATUS:="completed"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="source-deleted"}
export PROMETHEUS_METRICS_PORT=${PROMETHEUS_METRICS_PORT:="8080"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export RUN_UNTIL_NO_WORK=${RUN_UNTIL_NO_WORK:="FALSE"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.deleter

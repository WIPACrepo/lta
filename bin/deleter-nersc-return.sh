#!/usr/bin/env bash
# export OUTPUT_STATUS=${OUTPUT_STATUS:="source-deleted"}
cd /global/homes/i/icecubed/NEWLTA/lta
source env/bin/activate
cd /global/homes/i/icecubed/NEWLTA/lta/bin
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-deleter-return"}
export DEST_SITE=${DEST_SITE:="WIPAC"}
export DISK_BASE_PATH=${DISK_BASE_PATH:="/global/cfs/cdirs/icecubed"}
# export INPUT_STATUS=${INPUT_STATUS:="detached"}
export INPUT_STATUS=${INPUT_STATUS:="completed"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="source-deleted"}
export PROMETHEUS_METRICS_PORT=${PROMETHEUS_METRICS_PORT:="8080"}
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION="python"
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export RUN_UNTIL_NO_WORK=${RUN_UNTIL_NO_WORK:="FALSE"}
export SOURCE_SITE=${SOURCE_SITE:="NERSC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="500"}
python -m lta.deleter

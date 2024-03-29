#!/usr/bin/env bash
cd ${HOME}/lta
source env/bin/activate
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-pipe0-nersc-mover"}
export DEST_SITE=${DEST_SITE:="NERSC"}
export HPSS_AVAIL_PATH=${HPSS_AVAIL_PATH:="/usr/bin/hpss_avail.py"}
export INPUT_STATUS=${INPUT_STATUS:="taping"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export MAX_COUNT=${MAX_COUNT:="2"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="verifying"}
export PROMETHEUS_METRICS_PORT=${PROMETHEUS_METRICS_PORT:="8080"}
export RSE_BASE_PATH=${RSE_BASE_PATH:="/global/cfs/cdirs/icecubed"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="FALSE"}
export RUN_UNTIL_NO_WORK=${RUN_UNTIL_NO_WORK:="TRUE"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export TAPE_BASE_PATH=${TAPE_BASE_PATH:="/home/projects/icecube"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.nersc_mover

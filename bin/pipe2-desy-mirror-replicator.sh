#!/usr/bin/env bash
# pipe2-desy-mirror-replicator.sh

export CI_TEST=${CI_TEST:="FALSE"}
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-desy-mirror-replicator"}
export DEST_BASE_PATH=${DEST_BASE_PATH:="/pnfs/ifh.de/acs/icecube/archive"}
export DEST_SITE=${DEST_SITE:="DESY"}
export DEST_URL=${DEST_URL:="https://globe-door.ifh.de:2880"}
export INPUT_PATH=${INPUT_PATH:="/data/user/jadelta/ltatemp/bundler_todesy"}
export INPUT_STATUS=${INPUT_STATUS:="staged"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export MAX_PARALLEL=${MAX_PARALLEL:="100"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="transferring"}
export PROMETHEUS_METRICS_PORT=${PROMETHEUS_METRICS_PORT:="8080"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="FALSE"}
export RUN_UNTIL_NO_WORK=${RUN_UNTIL_NO_WORK:="FALSE"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="60"}
python -m lta.desy_mirror_replicator

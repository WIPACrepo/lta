#!/usr/bin/env bash
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-gridftp-replicator"}
export DEST_SITE=${DEST_SITE:="NERSC"}
export GLOBUS_PROXY_DURATION=${GLOBUS_PROXY_DURATION:="72"}
#export GLOBUS_PROXY_OUTPUT=${GLOBUS_PROXY_OUTPUT:=""}
#export GLOBUS_PROXY_PASSPHRASE=${GLOBUS_PROXY_PASSPHRASE:="hunter2"}  # http://bash.org/?244321
#export GLOBUS_PROXY_VOMS_ROLE=${GLOBUS_PROXY_VOMS_ROLE:=""}
#export GLOBUS_PROXY_VOMS_VO=${GLOBUS_PROXY_VOMS_VO:=""}
export GRIDFTP_DEST_URLS=${GRIDFTP_DEST_URLS:="gsiftp://dtn01.nersc.gov:2811/global/cfs/cdirs/icecubed;gsiftp://dtn02.nersc.gov:2811/global/cfs/cdirs/icecubed;gsiftp://dtn03.nersc.gov:2811/global/cfs/cdirs/icecubed;gsiftp://dtn04.nersc.gov:2811/global/cfs/cdirs/icecubed"}
export GRIDFTP_TIMEOUT=${GRIDFTP_TIMEOUT:="43200"}  # 43200 sec = 12 hours
export INPUT_STATUS=${INPUT_STATUS:="staged"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="transferring"}
export PROMETHEUS_METRICS_PORT=${PROMETHEUS_METRICS_PORT:="8080"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export RUN_UNTIL_NO_WORK=${RUN_UNTIL_NO_WORK:="FALSE"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export USE_FULL_BUNDLE_PATH=${USE_FULL_BUNDLE_PATH:="FALSE"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="5"}
python -m lta.gridftp_replicator

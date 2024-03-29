#!/usr/bin/env bash
cd /global/homes/i/icecubed/NEWLTA/lta
source env/bin/activate
cd /global/homes/i/icecubed/NEWLTA/lta/bin
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-pipe1-gridftp-replicator"}
export DEST_SITE=${DEST_SITE:="WIPAC"}
export GLOBUS_PROXY_DURATION=${GLOBUS_PROXY_DURATION:="72"}
export GLOBUS_PROXY_PASSPHRASE=${GLOBUS_PROXY_PASSPHRASE:="$(<globus-proxy-passphrase)"}
#export GRIDFTP_DEST_URL=${GRIDFTP_DEST_URL:="gsiftp://gridftp.icecube.wisc.edu:2811/mnt/lfss/jade-lta/welcome_home/"}
export GRIDFTP_DEST_URL=${GRIDFTP_DEST_URL:="gsiftp://gridftp.icecube.wisc.edu:2811/data/user/jadelta/ltatemp/welcome_home/"}
export GRIDFTP_TIMEOUT=${GRIDFTP_TIMEOUT:="43200"}  # 43200 sec = 12 hours
export INPUT_STATUS=${INPUT_STATUS:="staged"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
# export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="transferring"}
export PROMETHEUS_METRICS_PORT=${PROMETHEUS_METRICS_PORT:="8080"}
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION="python"
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="True"}
export RUN_UNTIL_NO_WORK=${RUN_UNTIL_NO_WORK:="FALSE"}
export SOURCE_SITE=${SOURCE_SITE:="NERSC"}
export USE_FULL_BUNDLE_PATH=${USE_FULL_BUNDLE_PATH:="FALSE"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
export X509_CERT_DIR=${X509_CERT_DIR:="/cvmfs/icecube.opensciencegrid.org/data/voms/share/certificates"}
python -m lta.gridftp_replicator

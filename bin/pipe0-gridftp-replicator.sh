#!/usr/bin/env bash
deftarget="5"
if [[  "$1" != "" ]]
  then
    if [[ "$1" == "6" || "$1" == "7" || "$1" == "8" ]]
      then
        deftarget="$1"
      fi
  fi
export COMPONENT_NAME=${COMPONENT_NAME:="$(hostname)-gridftp-replicator"}
export DEST_SITE=${DEST_SITE:="NERSC"}
export GLOBUS_PROXY_DURATION=${GLOBUS_PROXY_DURATION:="72"}
#export GLOBUS_PROXY_OUTPUT=${GLOBUS_PROXY_OUTPUT:=""}
#export GLOBUS_PROXY_PASSPHRASE=${GLOBUS_PROXY_PASSPHRASE:="hunter2"}  # http://bash.org/?244321
#export GLOBUS_PROXY_VOMS_ROLE=${GLOBUS_PROXY_VOMS_ROLE:=""}
#export GLOBUS_PROXY_VOMS_VO=${GLOBUS_PROXY_VOMS_VO:=""}
export GRIDFTP_DEST_URL=${GRIDFTP_DEST_URL:="gsiftp://dtn0${deftarget}.nersc.gov:2811/global/cscratch1/sd/icecubed/jade-disk/lta"}
export GRIDFTP_TIMEOUT=${GRIDFTP_TIMEOUT:="43200"}  # 43200 sec = 12 hours
export HEARTBEAT_PATCH_RETRIES=${HEARTBEAT_PATCH_RETRIES:="3"}
export HEARTBEAT_PATCH_TIMEOUT_SECONDS=${HEARTBEAT_PATCH_TIMEOUT_SECONDS:="30"}
export HEARTBEAT_SLEEP_DURATION_SECONDS=${HEARTBEAT_SLEEP_DURATION_SECONDS:="30"}
export INPUT_STATUS=${INPUT_STATUS:="staged"}
export LTA_REST_TOKEN=${LTA_REST_TOKEN:="$(<service-token)"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq:443"}
export OUTPUT_STATUS=${OUTPUT_STATUS:="transferring"}
export RUN_ONCE_AND_DIE=${RUN_ONCE_AND_DIE:="False"}
export SOURCE_SITE=${SOURCE_SITE:="WIPAC"}
export USE_FULL_BUNDLE_PATH=${USE_FULL_BUNDLE_PATH:="FALSE"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_SLEEP_DURATION_SECONDS=${WORK_SLEEP_DURATION_SECONDS:="30"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="30"}
python -m lta.gridftp_replicator

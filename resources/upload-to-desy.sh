# upload-to-desy.sh
# Upload a file to DESY using Sync

export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export DEST_BASE_PATH=${DEST_BASE_PATH:="/pnfs/ifh.de/acs/icecube/archive"}
export DEST_URL=${DEST_URL:="https://globe-door.ifh.de:2880"}
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export MAX_PARALLEL=${MAX_PARALLEL:="100"}
export WORK_RETRIES=${WORK_RETRIES:="3"}
export WORK_TIMEOUT_SECONDS=${WORK_TIMEOUT_SECONDS:="60"}

python3 -m resources.upload_to_desy $@

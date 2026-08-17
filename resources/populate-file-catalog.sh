#!/usr/bin/env bash
# populate-file-catalog.sh

#-------------------------------------------------------------------------------
# test configuration
#-------------------------------------------------------------------------------
# export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
# export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
# export FILE_CATALOG_REST_TOKEN=${FILE_CATALOG_REST_TOKEN:="$(resources/solicit-token.sh)"}
# export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="http://127.0.0.1:8889"}
# export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
# export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
# export METADATA_INBOX_PATH=${METADATA_INBOX_PATH:="${PWD}/metadata"}
# export METADATA_OUTBOX_PATH=${METADATA_OUTBOX_PATH:="${PWD}/finished"}
# export METADATA_QUARANTINE_PATH=${METADATA_INBOX_PATH:="${PWD}/problem_files"}

#-------------------------------------------------------------------------------
# production configuration
#-------------------------------------------------------------------------------
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export FILE_CATALOG_CLIENT_ID=${FILE_CATALOG_CLIENT_ID:="long-term-archive"}
export FILE_CATALOG_CLIENT_SECRET=${FILE_CATALOG_CLIENT_SECRET:="$(<keycloak-client-secret)"}
export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="https://file-catalog.icecube.wisc.edu"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_REST_URL=${LTA_REST_URL:="https://lta.icecube.aq"}
export METADATA_INBOX_PATH=${METADATA_INBOX_PATH:="${PWD}/metadata"}
export METADATA_OUTBOX_PATH=${METADATA_OUTBOX_PATH:="${PWD}/finished"}
export METADATA_QUARANTINE_PATH=${METADATA_QUARANTINE_PATH:="${PWD}/problem_files"}

# run the command
export PYTHONWARNINGS=${PYTHONWARNINGS:="ignore"}
python3 -m resources.populate_file_catalog $@

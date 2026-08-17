#!/usr/bin/env bash
# export-bundle-metadata.sh

#-------------------------------------------------------------------------------
# test configuration
#-------------------------------------------------------------------------------
# export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
# export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
# export FILE_CATALOG_REST_TOKEN=${FILE_CATALOG_REST_TOKEN:="$(resources/solicit-token.sh)"}
# export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="http://127.0.0.1:8889"}
# export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
# export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
# export METADATA_BUNDLE_UUID_JSON=${METADATA_BUNDLE_UUID_JSON:="${PWD}/metadata_bundles_uuid.json"}
# export METADATA_EXPORT_PATH=${METADATA_EXPORT_PATH:="${PWD}/metadata"}

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
export METADATA_BUNDLE_UUID_JSON=${METADATA_BUNDLE_UUID_JSON:="${PWD}/metadata_bundles_uuid.json"}
export METADATA_EXPORT_PATH=${METADATA_EXPORT_PATH:="${PWD}/metadata"}

# run the command
export PYTHONWARNINGS=${PYTHONWARNINGS:="ignore"}
python3 -m resources.export_bundle_metadata $@

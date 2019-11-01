#!/usr/bin/env bash
export LTA_AUTH_ALGORITHM=${LTA_AUTH_ALGORITHM:="HS512"}
export LTA_AUTH_ISSUER=${LTA_AUTH_ISSUER:="http://localhost:8888"}
export LTA_AUTH_SECRET=${LTA_AUTH_SECRET:="$(<local-secret)"}
export LTA_MAX_CLAIM_AGE_HOURS=${LTA_MAX_CLAIM_AGE_HOURS:="12"}
export LTA_MONGODB_NAME=${LTA_MONGODB_NAME:="lta"}
export LTA_MONGODB_URL=${LTA_MONGODB_URL:="mongodb://localhost:27017/"}
export LTA_REST_HOST=${LTA_REST_HOST:="127.0.0.1"}
export LTA_REST_PORT=${LTA_REST_PORT:="8080"}
python -m lta.rest_server

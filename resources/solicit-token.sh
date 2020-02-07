#!/usr/bin/env bash
export LTA_AUTH_ROLE=${LTA_AUTH_ROLE:="lta:system"}
export TOKEN_SERVICE_URL=${TOKEN_SERVICE_URL:="http://localhost:8888"}
python -m resources.solicit_token

#!/usr/bin/env bash
export LTA_AUTH_ALGORITHM=${LTA_AUTH_ALGORITHM:="HS256"}
export LTA_AUTH_EXPIRE_SECONDS=${LTA_AUTH_EXPIRE_SECONDS:="3600"}
export LTA_AUTH_ISSUER=${LTA_AUTH_ISSUER:="lta"}
export LTA_AUTH_ROLE=${LTA_AUTH_ROLE:="system"}
export LTA_AUTH_SECRET=${LTA_AUTH_SECRET:="$(<local-secret)"}
export LTA_AUTH_SUBJECT=${LTA_AUTH_SUBJECT:="foo"}
export LTA_AUTH_TYPE=${LTA_AUTH_TYPE:="temp"}
python -m resources.make_token

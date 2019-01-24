#!/usr/bin/env bash
export LTA_AUTH_ALGORITHM="HS256"
export LTA_AUTH_EXPIRE_SECONDS="3600"
export LTA_AUTH_ISSUER="lta"
export LTA_AUTH_ROLE="system"
export LTA_AUTH_SECRET="TpnW/sVYy/5gPpTOTSWuv1E4io8HyUa4vja8qZoYqa0="
export LTA_AUTH_SUBJECT="foo"
export LTA_AUTH_TYPE="temp"
python -m lta.make_token

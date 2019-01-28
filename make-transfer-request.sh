#!/usr/bin/env bash
export LTA_REST_TOKEN="$(make-token.sh)"
export LTA_REST_URL="http://127.0.0.1:8080"
python -m resources.make_transfer_request $@

#!/usr/bin/env bash
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
python3 -m resources.make_transfer_request $@

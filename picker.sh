#!/usr/bin/env bash
export FILE_CATALOG_REST_URL="http://127.0.0.1:8080"
export HEARTBEAT_SLEEP_DURATION_SECONDS="60"
export LTA_REST_URL="http://127.0.0.1:9090"
export PICKER_NAME="$(hostname)-picker"
export WORK_SLEEP_DURATION_SECONDS="300"
python -m lta.picker
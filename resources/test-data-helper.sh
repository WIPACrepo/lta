#!/usr/bin/env bash
export FAKE_CHECKSUM=${FAKE_CHECKSUM:="True"}
export FILE_CATALOG_REST_URL=${FILE_CATALOG_REST_URL:="http://127.0.0.1:8889"}
export LTA_REST_URL=${LTA_REST_URL:="http://127.0.0.1:8080"}
python3 -m resources.test_data_helper $@

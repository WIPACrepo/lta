#!/usr/bin/env bash
export FAKE_CHECKSUM="True"
export FILE_CATALOG_REST_TOKEN="$(solicit-token.sh)"
export FILE_CATALOG_REST_URL="http://127.0.0.1:8889"
export LTA_REST_TOKEN="$(solicit-token.sh)"
export LTA_REST_URL="http://127.0.0.1:8080"
python -m resources.test_data_helper $@

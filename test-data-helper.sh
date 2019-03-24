#!/usr/bin/env bash
export FAKE_CHECKSUM="True"
export FILE_CATALOG_REST_TOKEN="$(make-token.sh)"
export FILE_CATALOG_REST_URL="http://127.0.0.1:8888"
export LTA_REST_TOKEN="$(make-token.sh)"
export LTA_REST_URL="http://127.0.0.1:8080"
python -m resources.test_data_helper $@

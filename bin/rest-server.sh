#!/usr/bin/env bash
export LOG_LEVEL=${LOG_LEVEL:="DEBUG"}
export LTA_AUTH_AUDIENCE=${LTA_AUTH_AUDIENCE:="long-term-archive"}
export LTA_AUTH_OPENID_URL=${LTA_AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export LTA_MONGODB_AUTH_USER=${LTA_MONGODB_AUTH_USER:=""}
export LTA_MONGODB_AUTH_PASS=${LTA_MONGODB_AUTH_PASS:=""}
export LTA_MONGODB_DATABASE_NAME=${LTA_MONGODB_DATABASE_NAME:="lta"}
export LTA_MONGODB_HOST=${LTA_MONGODB_HOST:="localhost"}
export LTA_MONGODB_PORT=${LTA_MONGODB_PORT:="27017"}
export LTA_REST_HOST=${LTA_REST_HOST:="127.0.0.1"}
export LTA_REST_PORT=${LTA_REST_PORT:="8080"}
export OTEL_EXPORTER_OTLP_ENDPOINT=${OTEL_EXPORTER_OTLP_ENDPOINT:="https://telemetry.dev.icecube.aq/v1/traces"}
export WIPACTEL_EXPORT_STDOUT=${WIPACTEL_EXPORT_STDOUT:="FALSE"}
python -m lta.rest_server

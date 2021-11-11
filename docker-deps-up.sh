# docker-deps-up.sh
# Run some docker containers to provide dev/test dependencies
docker run \
    --detach \
    --name test-lta-mongo \
    --network=host \
    --rm \
    circleci/mongo:latest-ram &

docker run \
    --detach \
    --env auth_secret=secret \
    --env OTEL_EXPORTER_OTLP_ENDPOINT='localhost:4318' \
    --env WIPACTEL_EXPORT_STDOUT='FALSE' \
    --name test-lta-token \
    --network=host \
    --rm \
    wipac/token-service:latest python test_server.py &

docker run \
    --env LTA_AUTH_ALGORITHM='HS512' \
    --env LTA_AUTH_ISSUER='http://localhost:8888' \
    --env LTA_AUTH_SECRET='secret' \
    --env OTEL_EXPORTER_OTLP_ENDPOINT='localhost:4318' \
    --env WIPACTEL_EXPORT_STDOUT='TRUE' \
    --name test-lta-rest \
    --network=host \
    --rm \
    wipac/lta:latest python3 -m lta.rest_server &

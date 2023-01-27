# docker-deps-up.sh
# Run some docker containers to provide dev/test dependencies
docker run \
    --detach \
    --name test-lta-mongo \
    --network=host \
    --rm \
    circleci/mongo:latest-ram &

# docker run \
#     --env LTA_AUTH_AUDIENCE='lta' \
#     --env LTA_AUTH_OPENID_URL='' \
#     --env OTEL_EXPORTER_OTLP_ENDPOINT='localhost:4318' \
#     --env WIPACTEL_EXPORT_STDOUT='FALSE' \
#     --name test-lta-rest \
#     --network=host \
#     --rm \
#     wipac/lta:latest python3 -m lta.rest_server &

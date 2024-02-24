# docker-deps-up.sh
# Run some docker containers to provide dev/test dependencies

docker pull circleci/mongo:latest-ram

docker run \
    --detach \
    --name test-lta-mongo \
    --network=host \
    --rm \
    circleci/mongo:latest-ram

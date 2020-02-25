# docker-deps-down.sh
# Stop the docker containers that provide dev/test dependencies
docker stop test-lta-mongo
docker stop test-lta-token
docker stop test-lta-rest
docker stop jade_lta_test

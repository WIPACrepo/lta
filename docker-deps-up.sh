# docker-deps-up.sh
# Run some docker containers to provide dev/test dependencies
docker run --name test-lta-mongo --rm --network=host circleci/mongo:3.7.9-ram &
docker run --name test-lta-token --rm --network=host --env auth_secret=secret wipac/token-service:latest python test_server.py &
docker run --name test-lta-rest --rm --network=host --env LTA_AUTH_ALGORITHM='HS512' --env LTA_AUTH_ISSUER='http://localhost:8888' --env LTA_AUTH_SECRET='secret' wipac/lta:latest python3 -m lta.rest_server &
#docker run --name jade_lta_test -e MYSQL_ALLOW_EMPTY_PASSWORD=true -e MYSQL_USER=jade -e MYSQL_PASSWORD=hunter2 -e MYSQL_DATABASE=jade-lta -p 8890:3306 mysql &
docker start jade_lta_test

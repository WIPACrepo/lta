# webdav_proxy.sh
# Proxy a WebDAV service to add Authorization header

export AUTH_OPENID_URL=${AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export DEPTH=${DEPTH:="1"}
export LOG_LEVEL=${LOG_LEVEL:="INFO"}
export PROXY_HOST=${PROXY_HOST:="localhost"}
export PROXY_PORT=${PROXY_PORT:="8080"}
export RETRIES=${RETRIES:="3"}
export TIMEOUT_SECONDS=${TIMEOUT_SECONDS:="60"}
export UPSTREAM_URL=${UPSTREAM_URL:="https://globe-door.ifh.de:2880"}

python3 -m resources.webdav_proxy

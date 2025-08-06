# make_lta_token.sh
# Ask keycloak for an LTA token

export AUTH_OPENID_URL=${AUTH_OPENID_URL:="https://keycloak.icecube.wisc.edu/auth/realms/IceCube"}
export CLIENT_ID=${CLIENT_ID:="long-term-archive"}
export CLIENT_SECRET=${CLIENT_SECRET:="$(<keycloak-client-secret)"}
export LOG_LEVEL=${LOG_LEVEL:="ERROR"}

python3 -m resources.make_lta_token

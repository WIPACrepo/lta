"""
Token printing factory.

Run with `python -m lta.make_token`.
"""

from rest_tools.server import Auth, from_environment  # type: ignore

EXPECTED_CONFIG = {
    'LTA_AUTH_ALGORITHM': None,
    'LTA_AUTH_EXPIRE_SECONDS': None,
    'LTA_AUTH_ISSUER': None,
    'LTA_AUTH_ROLE': None,
    'LTA_AUTH_SECRET': None,
    'LTA_AUTH_SUBJECT': None,
    'LTA_AUTH_TYPE': None
}

if __name__ == '__main__':
    config = from_environment(EXPECTED_CONFIG)
    payload = {
        'aud': ["ANY"],
        'scope': f'lta:{config["LTA_AUTH_ROLE"]}',
    }
    a = Auth(config["LTA_AUTH_SECRET"],
             issuer=config["LTA_AUTH_ISSUER"],
             algorithm=config["LTA_AUTH_ALGORITHM"])
    t = a.create_token(config["LTA_AUTH_SUBJECT"],
                       expiration=int(config["LTA_AUTH_EXPIRE_SECONDS"]),
                       type=config["LTA_AUTH_TYPE"],
                       payload=payload)
    print(t)

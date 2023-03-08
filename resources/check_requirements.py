# check_requirements.py
"""Check the versions of most dependencies against the latest from PyPi."""

import json
import os
import requests


def check_versions(requirements_txt: str) -> None:
    """Check the versions of every requirement pinned to a specific version."""
    requirements = requirements_txt.split("\n")
    for requirement in requirements:
        if "==" in requirement:
            # split on the == sign
            equals_index = requirement.index("==")
            package_name = requirement[0:equals_index]
            package_version = requirement[(equals_index + 2):]

            # fix up package names with extras
            if "[" in package_name:
                extra_index = package_name.index("[")
                package_name = package_name[0:extra_index]

            # ask pypi about the versions of this package that are available
            url = f"https://pypi.python.org/pypi/{package_name}/json"
            r = requests.get(url)
            if r:
                data = json.loads(r.text)
                latest_version = data["info"]["version"]
                if package_version != latest_version:
                    print(f"{package_name} can be upgraded from {package_version} to {latest_version}")


current_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(current_path, '..', 'requirements.txt')) as f:
    requirements_txt = f.read()
check_versions(requirements_txt)

current_path = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(current_path, '..', 'requirements-dev.txt')) as f:
    requirements_txt = f.read()
check_versions(requirements_txt)

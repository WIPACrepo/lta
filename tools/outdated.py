#!/usr/bin/env python3
# outdated.py
"""Check the versions of dependencies against the latest from PyPi."""

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


def load_requirements_txt(path: str) -> str:
    """Load the text from the provided path into a string."""
    requirements_txt = ""
    with open(path, "r") as f:
        requirements_txt = f.read()
    return requirements_txt


req_files = [x for x in os.listdir() if "require" in x]
for req_file in req_files:
    print(f"Checking {req_file}...")
    requirements_txt = load_requirements_txt(req_file)
    check_versions(requirements_txt)
    print("")

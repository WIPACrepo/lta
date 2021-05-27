#!/usr/bin/env python

import glob
import os
from setuptools import setup  # type: ignore[import]
import subprocess

subprocess.run(
    "pip install --upgrade pip".split(),
    check=True,
)
subprocess.run(
    "pip install git+https://github.com/WIPACrepo/wipac-dev-tools.git".split(),
    check=True,
)
from wipac_dev_tools import SetupShop  # noqa: E402  # pylint: disable=C0413

shop = SetupShop(
    "lta",
    os.path.abspath(os.path.dirname(__file__)),
    ((3, 6), (3, 8)),
    "LTA is the Long Term Archive service and related tools, developed for the IceCube Collaboration.",
)

setup(
    scripts=glob.glob("bin/*"),
    url="https://github.com/WIPACrepo/lta",
    # package_data={shop.name: ["py.typed"]},
    **shop.get_kwargs(
        subpackages=["transfer"],
        other_classifiers=[
            "Operating System :: POSIX :: Linux",
            "Topic :: System :: Distributed Computing",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: Implementation :: CPython",
        ],
    ),
    zip_safe=False,
)

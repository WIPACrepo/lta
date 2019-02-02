#!/bin/sh
unset PYTHONPATH
virtualenv -p python3 env
echo "unset PYTHONPATH" >> env/bin/activate
. env/bin/activate
pip install --upgrade pip
pip install flake8 flake8-docstrings mypy prometheus_client pytest pytest-asyncio pytest-cov pytest-mock requests tornado wheel
pip install git+https://github.com/WIPACrepo/rest-tools

#!/bin/sh
unset PYTHONPATH
virtualenv -p python3 env
echo "unset PYTHONPATH" >> env/bin/activate
. env/bin/activate
pip install requests tornado pytest pytest-mock pytest-cov pytest-asyncio git+https://github.com/WIPACrepo/rest-tools

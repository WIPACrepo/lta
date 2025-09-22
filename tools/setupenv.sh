#!/bin/sh
unset PYTHONPATH
# virtualenv -p python3 env
python3 -m venv env
echo "unset PYTHONPATH" >> env/bin/activate
. env/bin/activate
pip install --upgrade pip
pip install -e .[dev,monitoring]

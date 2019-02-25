#!/bin/sh
unset PYTHONPATH
virtualenv -p python3 env
echo "unset PYTHONPATH" >> env/bin/activate
. env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

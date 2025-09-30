#!/bin/bash

set -euxo pipefail

# Run README first
ipython README.py

# Run all cleaned tutorial .py scripts
for pyfile in py_scripts/*.py; do
    ipython "$pyfile"
done

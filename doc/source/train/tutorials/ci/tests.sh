#!/bin/bash

set -euxo pipefail

# Run all cleaned tutorial .py scripts
for pyfile in ci/py_scripts/*.py; do
    ipython "$pyfile"
done

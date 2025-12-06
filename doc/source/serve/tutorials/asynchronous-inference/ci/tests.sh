#!/usr/bin/env bash
set -euxo pipefail

python ci/nb2py.py content/asynchronous-inference.ipynb /tmp/asynchronous-inference.py
python /tmp/asynchronous-inference.py

rm /tmp/asynchronous-inference.py

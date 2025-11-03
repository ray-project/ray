#!/usr/bin/env bash
set -euxo pipefail

# Example: convert and run a notebook
python ci/nb2py.py content/asynchronous-inference.ipynb --out /tmp/asynchronous-inference.py
python /tmp/asynchronous-inference.py

python content/client.py

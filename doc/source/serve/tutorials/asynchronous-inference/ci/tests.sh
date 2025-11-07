#!/usr/bin/env bash
set -euxo pipefail

cp content/server.py server.py
cp content/client.py client.py

# Example: convert and run a notebook
python ci/nb2py.py content/asynchronous-inference.ipynb /tmp/asynchronous-inference.py
python /tmp/asynchronous-inference.py

python content/client.py

rm server.py
rm client.py
rm /tmp/asynchronous-inference.py

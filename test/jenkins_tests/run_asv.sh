#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

git clone https://github.com/ray-project/asv.git /tmp/asv/ || true
cd /tmp/asv/
pip install -e .

cd /ray/python/
asv machine --yes
asv run --show-stderr --python=same --force-record-commit=$(cat ../git-rev)

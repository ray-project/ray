#!/usr/bin/env bash
set -euxo pipefail

# Generate README.sh from the notebook
python ci/nb2sh.py content/README.ipynb content/README.sh

# Run from the content directory so relative paths work
cd content
bash README.sh

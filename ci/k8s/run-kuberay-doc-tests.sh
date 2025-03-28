#!/bin/bash

set -euo pipefail

echo "--- Setup k8s environment"
SKIP_CREATE_KIND_CLUSTER=1 bash ci/k8s/prep-k8s-environment.sh

echo "--- Install Python dependencies"
pip install -c python/requirements_compiled.txt pytest nbval bash_kernel
python -m bash_kernel.install
pip install "ray[default]==2.41.0"

echo "--- Run doc tests"
cd doc/source/cluster/kubernetes
py.test --nbval getting-started/raycluster-quick-start.ipynb --nbval-kernel-name bash --sanitize-with doc_sanitize.cfg

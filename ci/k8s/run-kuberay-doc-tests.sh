#!/bin/bash

set -euo pipefail

echo "--- Setup k8s environment"
SKIP_CREATE_KIND_CLUSTER=1 bash ci/k8s/prep-k8s-environment.sh

echo "--- Run doc tests"
cd doc/source/cluster/kubernetes
py.test --nbval getting-started/raycluster-quick-start.ipynb --nbval-kernel-name bash --sanitize-with doc_sanitize.cfg

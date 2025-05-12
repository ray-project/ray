#!/bin/bash

set -euo pipefail

echo "--- Setup k8s environment"
SKIP_CREATE_KIND_CLUSTER=1 bash ci/k8s/prep-k8s-environment.sh

echo "--- Install Python dependencies"
pip install -c python/requirements_compiled.txt pytest nbval bash_kernel
python -m bash_kernel.install
pip install "ray[default]==2.41.0"

echo "--- Run a deliberate failure test to ensure the test script fails on error"
# The following Jupyter notebook only contains a single cell that runs the `date` command.
# The test script should fail because the output of the `date` command is different everytime.
cat <<EOF > test.ipynb
{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "43a8bb95-f6f2-45a8-ba48-b16856b2106d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Wed Mar 26 06:28:51 PM CST 2025\n"
     ]
    }
   ],
   "source": [
    "date"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Bash",
   "language": "bash",
   "name": "bash"
  },
  "language_info": {
   "codemirror_mode": "shell",
   "file_extension": ".sh",
   "mimetype": "text/x-sh",
   "name": "bash"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
EOF
set +e
if pytest --nbval test.ipynb --nbval-kernel-name bash; then
  echo "The test script should have failed but it didn't."
  exit 1
fi
set -e

echo "--- Run doc tests"
cd doc/source/cluster/kubernetes
TESTS=(
  "getting-started/raycluster-quick-start.ipynb"
  "getting-started/rayjob-quick-start.ipynb"
  "getting-started/rayservice-quick-start.ipynb"
  "user-guides/kuberay-gcs-ft.ipynb"
)
for test in "${TESTS[@]}"; do
  echo "Running test: ${test}"
  pytest --nbval "${test}" --nbval-kernel-name bash --sanitize-with doc_sanitize.cfg
done

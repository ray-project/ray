#!/bin/bash

# NOTE: Only working for Python 3.7 on MacOS.
# NOTE: Please modify the wheel URL.
DASK_VERSION=("2021.5.0" "2021.4.1" "2021.4.0" "2021.3.1" "2021.2.0" "2021.1.1" "2020.12.0")

unset RAY_ADDRESS

pip install --upgrade pip

# Comment out the below block for testing.
echo "Please run vi dask-on-ray-test.sh and modify the ray wheel properly."
echo "Also make sure that you are in the right branch on your repo."
echo "For example, if you are using releases/1.3.0 wheel, you should checkout to that repo."
echo "Example: git checkout -b releases/1.3.0 upstream/releases/1.3.0"
#exit 1
pip uninstall -y ray
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/1.4.0/0e95428a5975b774d266893102c39c8e137da5d8/ray-1.4.0-cp38-cp38-manylinux2014_x86_64.whl

for dask_version in "${DASK_VERSION[@]}"
do   # The quotes are necessary here
    echo "=================================================="
    echo "Downloading Dask of version '${dask_version}'"
    pip uninstall -y dask
    pip install -U "dask[complete]==${dask_version}"
    printf "==================================================\n\n\n"
    echo "=================================================="
    echo "Running tests against dask version ${dask_version}"
    python -m pytest -v ../../python/ray/tests/test_dask_scheduler.py
    python -m pytest -v ../../python/ray/tests/test_dask_callback.py
    python -m pytest -v ../../python/ray/tests/test_dask_optimization.py
    printf "==================================================\n\n\n"
done

#!/bin/bash

# NOTE: Only working for Python 3.7 on MacOS.
# NOTE: Please modify the wheel URL.
# TODO:(sang) Automate this to run on buildkite along with other release tests
DASK_VERSION=(
    "2021.7.0" "2021.6.2" "2021.6.1" "2021.6.0" 
    "2021.5.1" "2021.5.0" "2021.4.1" "2021.4.0" 
    "2021.3.1" "2021.2.0" "2021.1.1" "2020.12.0"
)
unset RAY_ADDRESS

pip install --upgrade pip
pip install pytest

# Comment out the below block for testing.
echo "Please run vi dask-on-ray-test.sh and modify the ray wheel properly."
echo "Also make sure that you are in the right branch on your repo."
echo "For example, if you are using releases/1.5.0 wheel, you should checkout to that repo."
echo "Example: git checkout -b releases/1.5.0 upstream/releases/1.5.0"

echo "Please ensure the following setup to run this script manually: "
echo "1) You're on a brand new conda environment, not nested in others"
echo "2) Copy over the shell script as well as test files to a new dir without ray folder in it"
echo "3) Modify this script's test file path below to your newly created directory"
#exit 1
pip uninstall -y ray
pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/1.5.0/ddad6a2458bded21035797ec84ba67b8a8c15ca5/ray-1.5.0-cp37-cp37m-macosx_10_13_intel.whl

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

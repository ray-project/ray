#!/usr/bin/env bash

git add release/
git commit -m 'WIP'

#CADE_RAY_WHEEL_URL=file://Users/cade/dev/oss-ray-cluster-test-infra/ray-3.0.0.dev0-cp37-cp37m-macosx_10_15_intel.whl \
NO_INSTALL=1 \
    RELEASE_AWS_BUCKET=cade-test \
    RAY_TEST_REPO=/Users/cade/dev/oss-ray-cluster-test-infra/ray \
    RAY_TEST_BRANCH=cade-oss-vm-launcher-test-infra-wip \
    CADE_SKIP_REINSTALL_RAY=1 \
    ./release/run_release_test.sh \
    cade-test \
    --ray-wheels http://localhost:8000/ray-3.0.0.dev0-cp37-cp37m-macosx_10_15_intel.whl
   # --ray-wheels https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-3.0.0.dev0-cp37-cp37m-macosx_10_15_intel.whl

    #CADE_SKIP_RAY_DOWN_COMMAND=1 \
    #CADE_SKIP_RAY_UP_COMMAND=1 \

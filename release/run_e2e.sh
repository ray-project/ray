#!/bin/bash

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    --ray-repo)
    shift
    RAY_REPO=$1
    ;;
    --ray-branch)
    shift
    RAY_BRANCH=$1
    ;;
    --ray-version)
    shift
    RAY_VERSION=$1
    ;;
    --ray-wheels)
    shift
    RAY_WHEELS=$1
    ;;
    --release-results-dir)
    shift
    RELEASE_RESULTS_DIR=$1
    ;;
    *)
    break
esac
shift
done

export RAY_REPO RAY_BRANCH RAY_VERSION RAY_WHEELS RAY_TEST_REPO RAY_TEST_BRANCH RELEASE_RESULTS_DIR

echo pip install -q -r release/requirements.txt,
echo pip install -U boto3 botocore,
echo git clone -b "${RAY_TEST_BRANCH}" "${RAY_TEST_REPO}" ~/ray

echo python e2e.py "$@"
EXIT_CODE=$?

echo sudo cp -rf /tmp/artifacts/* /tmp/ray_release_test_artifacts || true
echo "e2e command exited with exit code ${EXIT_CODE}"
exit $EXIT_CODE

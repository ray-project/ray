#!/bin/bash

set -x

cd "${0%/*}" || exit 1

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

RAY_TEST_REPO=${RAY_TEST_BRANCH-https://github.com/ray-project/ray.git}
RAY_TEST_BRANCH=${RAY_TEST_BRANCH-master}

export RAY_REPO RAY_BRANCH RAY_VERSION RAY_WHEELS RAY_TEST_REPO RAY_TEST_BRANCH RELEASE_RESULTS_DIR

# Disable this for now as it also affects manual retries
# RETRY_COUNT=${BUILDKITE_RETRY_COUNT-0}
# if [ $RETRY_COUNT -ge 1 ]; then
#   SLEEP_TIME=$((600 * RETRY_COUNT))
#   echo "Retry count: ${RETRY_COUNT}. Sleeping for ${SLEEP_TIME} seconds before retrying the run."
#   sleep ${SLEEP_TIME}
# fi

pip install -q -r requirements.txt
pip install -U boto3 botocore
git clone -b "${RAY_TEST_BRANCH}" "${RAY_TEST_REPO}" ~/ray

python e2e.py "$@"
EXIT_CODE=$?

sudo cp -rf /tmp/artifacts/* /tmp/ray_release_test_artifacts || true
echo "e2e command exited with exit code ${EXIT_CODE}"
exit $EXIT_CODE

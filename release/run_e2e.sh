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
    --ray-test-repo)
    shift
    RAY_TEST_REPO=$1
    ;;
    --ray-test-branch)
    shift
    RAY_TEST_BRANCH=$1
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

RAY_TEST_REPO=${RAY_TEST_REPO-https://github.com/ray-project/ray.git}
RAY_TEST_BRANCH=${RAY_TEST_BRANCH-master}

export RAY_REPO RAY_BRANCH RAY_VERSION RAY_WHEELS RAY_TEST_REPO RAY_TEST_BRANCH RELEASE_RESULTS_DIR

pip install -q -r requirements.txt
pip install -U boto3 botocore
git clone -b "${RAY_TEST_BRANCH}" "${RAY_TEST_REPO}" ~/ray

RETRY_NUM=1
MAX_RETRIES=${MAX_RETRIES-3}

if [ "${BUILDKITE_RETRY_COUNT-0}" -ge 1 ]; then
  echo "This is a manually triggered retry from the Buildkite web UI, so we set the number of infra retries to 1."
  MAX_RETRIES=1
fi

while [ "$RETRY_NUM" -le "$MAX_RETRIES" ]; do
  python e2e.py "$@"
  EXIT_CODE=$?

  case ${EXIT_CODE} in
    0)
    echo "Script finished successfully on try ${RETRY_NUM}"
    break
    ;;
    7 | 9 | 10)
    echo "Script failed on try ${RETRY_NUM} with exit code ${EXIT_CODE}, restarting."
    ;;
    *)
    echo "Script failed on try ${RETRY_NUM} with exit code ${EXIT_CODE}, aborting."
    break
    ;;
  esac

  SLEEP_TIME=$((600 * RETRY_NUM))
  echo "Retry count: ${RETRY_NUM}. Sleeping for ${SLEEP_TIME} seconds before retrying the run."
  sleep ${SLEEP_TIME}

  RETRY_NUM=$((RETRY_NUM + 1))
done

echo OVER

sudo cp -rf /tmp/artifacts/* /tmp/ray_release_test_artifacts || true
echo "e2e command exited with exit code ${EXIT_CODE}"
exit $EXIT_CODE

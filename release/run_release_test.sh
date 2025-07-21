#!/bin/bash
# shellcheck disable=SC2317

set -e

if [ -n "$DEBUG" ]; then
  set -x
fi

cd "${0%/*}" || exit 1

reason() {
  # Keep in sync with e2e.py ExitCode enum
  if [ "$1" -eq 0 ]; then
    REASON="success"
  elif [ "$1" -ge 1 ] && [ "$1" -lt 10 ]; then
    REASON="runtime error"
  elif [ "$1" -ge 10 ] && [ "$1" -lt 20 ]; then
    REASON="infra error"
  elif [ "$1" -ge 30 ] && [ "$1" -lt 40 ]; then
    REASON="infra timeout"
  elif [ "$1" -eq 42 ]; then
    REASON="command timeout"
  elif [ "$1" -ge 40 ] && [ "$1" -lt 50 ]; then
    REASON="command error"
  fi
  echo "${REASON}"
}

RAY_TEST_SCRIPT=${RAY_TEST_SCRIPT-"python ray_release/scripts/run_release_test.py"}
RELEASE_RESULTS_DIR=${RELEASE_RESULTS_DIR-/tmp/artifacts}
BUILDKITE_MAX_RETRIES=1
BUILDKITE_RETRY_CODE=79
BUILDKITE_TIME_LIMIT_FOR_RETRY=10800 # 3 hours

export RAY_TEST_REPO RAY_TEST_BRANCH RELEASE_RESULTS_DIR BUILDKITE_MAX_RETRIES BUILDKITE_RETRY_CODE BUILDKITE_TIME_LIMIT_FOR_RETRY

if [ -n "${RAY_COMMIT_OF_WHEEL-}" ]; then
  git config --global --add safe.directory /workdir
  HEAD_COMMIT=$(git rev-parse HEAD)
  echo "The test repo has head commit of ${HEAD_COMMIT}"
  if [[ "${HEAD_COMMIT}" != "${RAY_COMMIT_OF_WHEEL}" ]]; then
    echo "The checked out test code doesn't match with the installed wheel. \
          This is likely due to a racing condition when a PR is landed between \
          a wheel is installed and test code is checked out."
    echo "Hard resetting from ${HEAD_COMMIT} to ${RAY_COMMIT_OF_WHEEL}."
    git reset --hard "${RAY_COMMIT_OF_WHEEL}"
  fi
fi

if [ -z "${NO_INSTALL}" ]; then
  # Strip the hashes from the constraint file
  # TODO(aslonnie): use bazel run..
  grep '==' ./requirements_buildkite.txt > /tmp/requirements_buildkite_nohash.txt
  sed -i 's/ \\//' /tmp/requirements_buildkite_nohash.txt  # Remove ending slashes.
  sed -i 's/\[.*\]//g' /tmp/requirements_buildkite_nohash.txt  # Remove extras.
  pip install -c /tmp/requirements_buildkite_nohash.txt -e .
fi

RETRY_NUM=0
MAX_RETRIES=${MAX_RETRIES-1}

if [ "${BUILDKITE_RETRY_COUNT-0}" -ge 1 ]; then
  echo "This is a manually triggered retry from the Buildkite web UI, so we set the number of infra retries to 1."
  MAX_RETRIES=1
fi

ALL_EXIT_CODES=()
while [ "$RETRY_NUM" -lt "$MAX_RETRIES" ]; do
  RETRY_NUM=$((RETRY_NUM + 1))

  if [ "$RETRY_NUM" -gt 1 ]; then
    # Sleep for random time between 30 and 90 minutes
    SLEEP_TIME=$((1800 + RANDOM % 5400))

    if [ -n "${OVERRIDE_SLEEP_TIME}" ]; then
      SLEEP_TIME=${OVERRIDE_SLEEP_TIME}
    fi

    echo "----------------------------------------"
    echo "Retry count: ${RETRY_NUM}/${MAX_RETRIES}. Sleeping for ${SLEEP_TIME} seconds before retrying the run."
    echo "----------------------------------------"
    sleep "${SLEEP_TIME}"
  fi

  if [ -z "${NO_ARTIFACTS}" ]; then
    rm -rf "${RELEASE_RESULTS_DIR:?}"/* || true
  fi

  _term() {
    echo "[SCRIPT $(date +'%Y-%m-%d %H:%M:%S'),...] Caught SIGTERM signal, sending SIGTERM to release test script"
    kill "$proc"
    wait "$proc"
  }

  START=$(date +%s)
  set +e

  if [[ "$1" == *".kuberay"* ]]; then
    export GOOGLE_CLOUD_PROJECT=dhyey-dev
    export AWS_REGION="us-west-2"
  fi

  trap _term SIGINT SIGTERM
  ${RAY_TEST_SCRIPT} "$@" &
  proc=$!

  wait "$proc"
  EXIT_CODE=$?

  set -e
  END=$(date +%s)

  REASON=$(reason "${EXIT_CODE}")
  RUNTIME=$((END-START))
  ALL_EXIT_CODES[${#ALL_EXIT_CODES[@]}]=$EXIT_CODE

  case ${EXIT_CODE} in
    0)
    echo "Script finished successfully on try ${RETRY_NUM}/${MAX_RETRIES}"
    break
    ;;
    30 | 31 | 32 | 33)
    echo "Script failed on try ${RETRY_NUM}/${MAX_RETRIES} with exit code ${EXIT_CODE} (${REASON})."
    ;;
    *)
    echo "Script failed on try ${RETRY_NUM}/${MAX_RETRIES} with exit code ${EXIT_CODE} (${REASON}), aborting."
    break
    ;;
  esac

done

if [ -z "${NO_ARTIFACTS}" ]; then
  rm -rf /tmp/ray_release_test_artifacts/* || true
  cp -rf "${RELEASE_RESULTS_DIR}"/* /tmp/ray_release_test_artifacts/ || true
fi

echo "----------------------------------------"
echo "Release test finished with final exit code ${EXIT_CODE} after ${RETRY_NUM}/${MAX_RETRIES} tries"
echo "Run results:"

COUNTER=1
for EX in "${ALL_EXIT_CODES[@]}"; do
  REASON=$(reason "${EX}")
  echo "  Run $COUNTER: Exit code = ${EX} (${REASON})"
  COUNTER=$((COUNTER + 1))
done

echo "----------------------------------------"

REASON=$(reason "${EXIT_CODE}")
echo "Final release test exit code is ${EXIT_CODE} (${REASON}). Took ${RUNTIME}s"

if [ "$EXIT_CODE" -eq 0 ]; then
  echo "RELEASE MANAGER: This test seems to have passed."
elif [ "$EXIT_CODE" -ge 30 ] && [ "$EXIT_CODE" -lt 40 ]; then
  echo "RELEASE MANAGER: This is likely an infra error that can be solved by RESTARTING this test."
else
  echo "RELEASE MANAGER: This could be an error in the test. Please REVIEW THE LOGS and ping the test owner."
fi

if [[ "$EXIT_CODE" -ne 0 && "$RUNTIME" -le "$BUILDKITE_TIME_LIMIT_FOR_RETRY" ]]; then
  exit "$BUILDKITE_RETRY_CODE"
else
  exit "$EXIT_CODE"
fi

#!/usr/bin/env bash

# This script should be run as follows:
#     ./run_application_stress_tests.sh <ray-version> <ray-commit>
# For example, <ray-version> might be 0.7.1
# and <ray-commit> might be bc3b6efdb6933d410563ee70f690855c05f25483. The commit
# should be the latest commit on the branch "releases/<ray-version>".

# This script runs all of the application tests.
# Currently includes an IMPALA stress test and a SGD stress test.
# on both Python 2.7 and 3.6.
# All tests use a separate cluster, and each cluster
# will be destroyed upon test completion (or failure).

# Note that if the environment variable DEBUG_MODE is detected,
# the clusters will not be automatically shut down after the test runs.

# This script will exit with code 1 if the test did not run successfully.

# Show explicitly which commands are currently running. This should only be AFTER
# the private key is placed.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RESULT_FILE=$ROOT_DIR/"results-$(date '+%Y-%m-%d_%H-%M-%S').log"

touch "$RESULT_FILE"
echo "Logging to" "$RESULT_FILE"

if [[ -z  "$1" ]]; then
  echo "ERROR: The first argument must be the Ray version string."
  exit 1
else
  RAY_VERSION=$1
fi

if [[ -z  "$2" ]]; then
  echo "ERROR: The second argument must be the commit hash to test."
  exit 1
else
  RAY_COMMIT=$2
fi

echo "Testing ray==$RAY_VERSION at commit $RAY_COMMIT."
echo "The wheels used will live under https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_COMMIT/"

# This function identifies the right string for the Ray wheel.
_find_wheel_str(){
    local python_version=$1
    # echo "PYTHON_VERSION", $python_version
    local wheel_str=""
    if [ "$python_version" == "p27" ]; then
        wheel_str="cp27-cp27mu"
    else
        wheel_str="cp36-cp36m"
    fi
    echo $wheel_str
}

# Total time is roughly 25 minutes.
# Actual test runtime is roughly 10 minutes.
test_impala(){
    local PYTHON_VERSION=$1
    local WHEEL_STR=$(_find_wheel_str "$PYTHON_VERSION")

    pushd "$ROOT_DIR"
        local TEST_NAME="rllib_impala_$PYTHON_VERSION"
        local CLUSTER="$TEST_NAME.yaml"
        echo "Creating IMPALA cluster YAML from template."

        cat application_cluster_template.yaml |
            sed -e "
                s/<<<RAY_VERSION>>>/$RAY_VERSION/g;
                s/<<<RAY_COMMIT>>>/$RAY_COMMIT/;
                s/<<<CLUSTER_NAME>>>/$TEST_NAME/;
                s/<<<HEAD_TYPE>>>/p3.16xlarge/;
                s/<<<WORKER_TYPE>>>/m4.16xlarge/;
                s/<<<MIN_WORKERS>>>/9/;
                s/<<<MAX_WORKERS>>>/9/;
                s/<<<PYTHON_VERSION>>>/$PYTHON_VERSION/;
                s/<<<WHEEL_STR>>>/$WHEEL_STR/;" > "$CLUSTER"

        echo "Try running IMPALA stress test."
        {
            RLLIB_DIR=../../python/ray/rllib/
            ray --logging-level=DEBUG up -y "$CLUSTER" &&
            ray rsync_up "$CLUSTER" $RLLIB_DIR/tuned_examples/ tuned_examples/ &&
            # HACK: the test will deadlock if it scales up slowly, so we have to wait
            # for the cluster to be fully launched first. This is because the first
            # trial will occupy all the CPU slots if it can, preventing GPU access.
            sleep 200 &&
            ray --logging-level=DEBUG exec "$CLUSTER" "source activate tensorflow_p36 && rllib train -f tuned_examples/atari-impala-large.yaml --ray-address='localhost:6379' --queue-trials" &&
            echo "PASS: IMPALA Test for" "$PYTHON_VERSION" >> "$RESULT_FILE"
        } || echo "FAIL: IMPALA Test for" "$PYTHON_VERSION" >> "$RESULT_FILE"

        # Tear down cluster.
        if [ "$DEBUG_MODE" = "" ]; then
            ray down -y "$CLUSTER"
            rm "$CLUSTER"
        else
            echo "Not tearing down cluster" "$CLUSTER"
        fi
    popd
}

# Total runtime is about 20 minutes (if the AWS spot instance order is fulfilled).
# Actual test runtime is roughly 10 minutes.
test_sgd(){
    local PYTHON_VERSION=$1
    local WHEEL_STR=$(_find_wheel_str $PYTHON_VERSION)

    pushd "$ROOT_DIR"
        local TEST_NAME="sgd_$PYTHON_VERSION"
        local CLUSTER="$TEST_NAME.yaml"
        echo "Creating SGD cluster YAML from template."

        cat application_cluster_template.yaml |
            sed -e "
                s/<<<RAY_VERSION>>>/$RAY_VERSION/g;
                s/<<<RAY_COMMIT>>>/$RAY_COMMIT/;
                s/<<<CLUSTER_NAME>>>/$TEST_NAME/;
                s/<<<HEAD_TYPE>>>/p3.16xlarge/;
                s/<<<WORKER_TYPE>>>/p3.16xlarge/;
                s/<<<MIN_WORKERS>>>/3/;
                s/<<<MAX_WORKERS>>>/3/;
                s/<<<PYTHON_VERSION>>>/$PYTHON_VERSION/;
                s/<<<WHEEL_STR>>>/$WHEEL_STR/;" > "$CLUSTER"

        echo "Try running SGD stress test."
        {
            SGD_DIR=$ROOT_DIR/../../python/ray/experimental/sgd/
            ray --logging-level=DEBUG up -y "$CLUSTER" &&
            # TODO: fix submit so that args work
            ray rsync_up "$CLUSTER" "$SGD_DIR/mnist_example.py" mnist_example.py &&
            sleep 1 &&
            ray --logging-level=DEBUG exec "$CLUSTER" "
                python mnist_example.py --redis-address=localhost:6379 --num-iters=2000 --num-workers=8 --devices-per-worker=2 --gpu" &&
            echo "PASS: SGD Test for" "$PYTHON_VERSION" >> "$RESULT_FILE"
        } || echo "FAIL: SGD Test for" "$PYTHON_VERSION" >> "$RESULT_FILE"

        # Tear down cluster.
        if [ "$DEBUG_MODE" = "" ]; then
            ray down -y "$CLUSTER"
            rm "$CLUSTER"
        else
            echo "Not tearing down cluster" "$CLUSTER"
        fi
    popd
}

# RUN TESTS
for PYTHON_VERSION in "p36"
do
    test_impala $PYTHON_VERSION
done

cat "$RESULT_FILE"
cat "$RESULT_FILE" | grep FAIL > test.log
[ ! -s test.log ] || exit 1

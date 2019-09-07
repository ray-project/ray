#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RESULT_FILE=$ROOT_DIR/results-$(date '+%Y-%m-%d_%H-%M-%S').log

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

run_test(){
    local test_name=$1

    local CLUSTER="stress_testing_config_temporary.yaml"

    cat stress_testing_config.yaml |
        sed -e "
            s/<<<RAY_VERSION>>>/$RAY_VERSION/g;
            s/<<<RAY_COMMIT>>>/$RAY_COMMIT/;" > "$CLUSTER"

    echo "Try running $test_name."
    {
        ray up -y $CLUSTER --cluster-name "$test_name" &&
        sleep 1 &&
        ray --logging-level=DEBUG submit "$CLUSTER" --cluster-name "$test_name" "$test_name.py"
    } || echo "FAIL: $test_name" >> "$RESULT_FILE"

    # Tear down cluster.
    if [ "$DEBUG_MODE" = "" ]; then
        ray down -y $CLUSTER --cluster-name "$test_name"
        rm "$CLUSTER"
    else
        echo "Not tearing down cluster" "$CLUSTER"
    fi
}

pushd "$ROOT_DIR"
    run_test test_many_tasks
    run_test test_dead_actors
popd

cat "$RESULT_FILE"
[ ! -s "$RESULT_FILE" ] || exit 1

#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)


run_test(){
    local test_name=$1
    local RESULT_FILE=$2

    local CLUSTER="stress_testing_config.yaml"
    echo "Try running $test_name."
    {
        ray up -y $CLUSTER --cluster-name "$test_name"
        sleep 1
        ray submit $CLUSTER --cluster-name "$test_name" "$test_name.py"
        echo "PASS: $test_name" >> $RESULT_FILE
    } || echo "FAIL: $test_name" >> $RESULT_FILE

    # Tear down cluster.
    if [ "$DEBUG_MODE" = "" ]; then
        ray down -y $CLUSTER --cluster-name "$test_name"
    else
        echo "Not tearing down cluster" $CLUSTER
    fi
}

pushd "$ROOT_DIR"
    RESULT_FILE="results-$(date '+%Y-%m-%d_%H-%M-%S').log"
    echo "Logging to" $RESULT_FILE
    touch $RESULT_FILE

    run_test test_many_tasks_and_transfers $RESULT_FILE &
    run_test test_dead_actors $RESULT_FILE &

    wait
    cat $RESULT_FILE
popd


#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RESULT_FILE="results-$(date '+%Y-%m-%d_%H-%M-%S').log"
echo "Logging to" $RESULT_FILE
touch $RESULT_FILE


run_test(){
    local test_name=$1
    pushd "$ROOT_DIR"

        local CLUSTER="stress_testing_config.yaml"
        echo "Try running $1."
        {
            ray up -y $CLUSTER --cluster-name "$1"
            sleep 1
            ray submit $CLUSTER --cluster-name "$1" "$1.py"
            echo "PASS: $1" >> $RESULT_FILE
        } || echo "FAIL: $1" >> $RESULT_FILE

        # Tear down cluster.
        if [ "$DEBUG_MODE" = "" ]; then
            ray down -y $CLUSTER --cluster-name "$1"
        else
            echo "Not tearing down cluster" $CLUSTER
        fi
    popd
}

run_test test_many_tasks_and_transfers &
run_test test_dead_actors &

wait
cat RESULT_FILE

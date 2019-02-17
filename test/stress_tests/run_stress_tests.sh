#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RESULT_FILE=$ROOT_DIR/results-$(date '+%Y-%m-%d_%H-%M-%S').log
echo "Logging to" $RESULT_FILE
echo -e $RAY_AWS_SSH_KEY > /root/.ssh/ray-autoscaler_us-west-2.pem && chmod 400 /root/.ssh/ray-autoscaler_us-west-2.pem || true


# Show explicitly which commands are currently running. This should only be AFTER
# the private key is placed.
set -x

touch $RESULT_FILE

run_test(){
    local test_name=$1

    local CLUSTER="stress_testing_config.yaml"
    echo "Try running $test_name."
    {
        ray up -y $CLUSTER --cluster-name "$test_name" &&
        sleep 1 &&
        ray --logging-level=DEBUG submit $CLUSTER --cluster-name "$test_name" "$test_name.py"
    } || echo "FAIL: $test_name" >> $RESULT_FILE

    # Tear down cluster.
    if [ "$DEBUG_MODE" = "" ]; then
        ray down -y $CLUSTER --cluster-name "$test_name"
    else
        echo "Not tearing down cluster" $CLUSTER
    fi
}

pushd "$ROOT_DIR"
    run_test test_many_tasks_and_transfers
    run_test test_dead_actors
popd

cat $RESULT_FILE
[ ! -s $RESULT_FILE ] || exit 1

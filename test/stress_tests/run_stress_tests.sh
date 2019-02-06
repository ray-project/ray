#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RESULT_FILE=$ROOT_DIR/results-$(date '+%Y-%m-%d_%H-%M-%S').log
echo "Logging to" $RESULT_FILE
touch $RESULT_FILE

kill_nodes() {
  # Wait for the cluster to come up.
  sleep 30;
  for i in `seq 1 10`; do
    echo "Killing $i$-th node";
    nohup ray kill_random_node -y --cluster-name test_shuffle stress_testing_config.yaml;
  done
}

run_test(){
    local test_name=$1

    local CLUSTER="stress_testing_config.yaml"
    echo "Try running $test_name."
    {
        ray up -y $CLUSTER --cluster-name "$test_name" &&
        sleep 1 &&
        ray submit $CLUSTER --cluster-name "$test_name" "$test_name.py"
    } || echo "FAIL: $test_name" >> $RESULT_FILE

    # Tear down cluster.
    if [ "$DEBUG_MODE" = "" ]; then
        ray down -y $CLUSTER --cluster-name "$test_name"
    else
        echo "Not tearing down cluster" $CLUSTER
    fi
}

run_failure_test(){
    local test_name=$1

    local CLUSTER="stress_testing_config.yaml"
    echo "Try running $test_name."
    {
        ray up -y $CLUSTER --cluster-name "$test_name" &&
        sleep 1 &&
        (sleep 20 &&
        for i in `seq 1 10`; do
          echo "Killing node $i";
          nohup ray kill_random_node -y --cluster-name test_shuffle stress_testing_config.yaml;
        done) & pid=$! &&
        ray submit $CLUSTER --cluster-name "$test_name" "$test_name.py" &&
        wait $pid
    } || echo "FAIL: $test_name" >> $RESULT_FILE

    # Tear down cluster.
    if [ "$DEBUG_MODE" = "" ]; then
        ray down -y $CLUSTER --cluster-name "$test_name"
    else
        echo "Not tearing down cluster" $CLUSTER
    fi
}

run_failure_test(){
    local test_name="$1-failure"

    local CLUSTER="stress_testing_config.yaml"
    echo "Try running $test_name."
    {
        ray up -y $CLUSTER --cluster-name "$test_name" &&
        sleep 1
        # Kill 10 nodes in the background.
        kill_nodes & pid=$!
        ray submit $CLUSTER --cluster-name "$test_name" "$test_name.py" &&
        wait $pid
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
    run_test test_shuffle
    run_test test_actor_event_loop
    # Blocked on #3958.
    #run_failure_test test_shuffle
popd

cat $RESULT_FILE
[ ! -s $RESULT_FILE ] || exit 1

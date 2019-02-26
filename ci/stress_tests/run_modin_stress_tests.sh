#!/usr/bin/env bash

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RESULT_FILE=$ROOT_DIR/results-$(date '+%Y-%m-%d_%H-%M-%S').log
RAY_VERSION=$(git describe --tags --abbrev=0)
echo "Logging to" $RESULT_FILE
touch $RESULT_FILE

run_modin_tests(){
    pushd "$ROOT_DIR"
    git clone https://github.com/modin-project/modin.git
    bash modin/stress_tests/run_stress_tests.sh "$ROOT_DIR/../.."
    popd
}

pushd "$ROOT_DIR"
    run_test test_many_tasks_and_transfers
    run_test test_dead_actors
popd

cat $RESULT_FILE
[ ! -s $RESULT_FILE ] || exit 1

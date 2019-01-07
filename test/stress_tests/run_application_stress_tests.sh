#!/usr/bin/env bash
# This script runs all of the application tests.


ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
RAY_VERSION=$(git describe --tags --abbrev=0)
echo "Testing on latest version of Ray: $RAY_VERSION"

# This function identifies the right string for the Ray wheel.
_find_wheel_str(){
    local python_version=$1
    # echo "PYTHON_VERSION", $python_version
    local wheel_str=""
    if [ $python_version == "p27" ]; then
        wheel_str="cp27-cp27mu"
    else
        wheel_str="cp36-cp36m"
    fi
    echo $wheel_str
}


test_impala(){
    local PYTHON_VERSION=$1
    local WHEEL_STR=$(_find_wheel_str $PYTHON_VERSION)

    pushd "$ROOT_DIR"

        local CLUSTER="rllib_impala.yaml"
        echo "Creating IMPALA cluster YAML from template."
        echo "Current Directory:" $(pwd)
        cat application_cluster_template.yaml |
            sed -e "
                s/<<<CLUSTER_NAME>>>/rllib-impala-testing/;
                s/<<<RAY_VERSION>>>/$RAY_VERSION/;
                s/<<<HEAD_TYPE>>>/g3.16xlarge/;
                s/<<<WORKER_TYPE>>>/m5.24xlarge/;
                s/<<<MIN_WORKERS>>>/5/;
                s/<<<MAX_WORKERS>>>/5/;
                s/<<<PYTHON_VERSION>>>/$PYTHON_VERSION/;
                s/<<<WHEEL_STR>>>/$WHEEL_STR/;" > $CLUSTER

        echo "Try running IMPALA stress test."
        {
            RLLIB_DIR=../../python/ray/rllib/
            ray up -y $CLUSTER
            ray rsync_up $CLUSTER $RLLIB_DIR/tuned_examples/ tuned_examples/
            sleep 1
            ray exec $CLUSTER "
                rllib train -f tuned_examples/atari-impala-large.yaml --redis-address='localhost:6379' --queue-trials"
        } || echo "IMPALA Test Failed."

        # Tear down cluster.
        if [ "$DEBUG_MODE" = "" ]; then
            ray down -y $CLUSTER
            rm $CLUSTER
        else
            echo "Not tearing down cluster" $CLUSTER
        fi
    popd
}


test_sgd(){
    local PYTHON_VERSION=$1
    local WHEEL_STR=$(_find_wheel_str $PYTHON_VERSION)

    pushd "$ROOT_DIR"
        local CLUSTER="sgd.yaml"
        echo "Creating SGD cluster YAML from template."

        cat application_cluster_template.yaml |
            sed -e "
                s/<<<CLUSTER_NAME>>>/sgd-testing/;
                s/<<<RAY_VERSION>>>/$RAY_VERSION/;
                s/<<<HEAD_TYPE>>>/g3.16xlarge/;
                s/<<<WORKER_TYPE>>>/g3.16xlarge/;
                s/<<<MIN_WORKERS>>>/3/;
                s/<<<MAX_WORKERS>>>/3/;
                s/<<<PYTHON_VERSION>>>/$PYTHON_VERSION/;
                s/<<<WHEEL_STR>>>/$WHEEL_STR/;" > $CLUSTER

        echo "Try running SGD stress test."
        {
            SGD_DIR=$ROOT_DIR/../../python/ray/experimental/sgd/
            ray up -y $CLUSTER
            # TODO: fix submit so that args work
            ray rsync_up $CLUSTER $SGD_DIR/mnist_example.py mnist_example.py
            sleep 1
            ray exec $CLUSTER "
                python mnist_example.py --redis-address=localhost:6379 --num-iters=2000 --num-workers=8 --devices-per-worker=2 --gpu"
        } || echo "SGD Test Failed."

        # Tear down cluster.
        if [ "$DEBUG_MODE" = "" ]; then
            ray down -y $CLUSTER
            rm $CLUSTER
        else
            echo "Not tearing down cluster" $CLUSTER
        fi
    popd
}

# RUN TESTS
for PYTHON_VERSION in "p27" "p36"
do
    test_impala $PYTHON_VERSION
    test_sgd $PYTHON_VERSION
done

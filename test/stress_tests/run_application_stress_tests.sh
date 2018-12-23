#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# Start a cluster for IMPALA using the autoscaler.
CLUSTER=$ROOT_DIR/rllib_cluster.yaml
RLLIB_DIR=$ROOT_DIR/../../python/ray/rllib/

ray up -y $CLUSTER

ray rsync_up $CLUSTER $RLLIB_DIR/tuned_examples/ tuned_examples/
sleep 1
# TODO(rliaw): For some reason, command doesn't run without this prefix
# Also, CUDA path raised a couple issues.
ray exec $CLUSTER 'source activate tensorflow_p36 && rllib train -f tuned_examples/atari-impala-large.yaml  --redis-address="localhost:6379" --queue-trials'

ray down -y $CLUSTER

CLUSTER=$ROOT_DIR/sgd_cluster.yaml
SGD_DIR=$ROOT_DIR/../../python/ray/experimental/sgd/

ray up -y $CLUSTER

ray rsync_up $CLUSTER $SGD_DIR/mnist_example.py mnist_example.py
# TODO: fix submit so that args work
ray exec $CLUSTER "python mnist_example.py"
                    " --redis-address=localhost:6379"
                    " --num-iters=2000"
                    " --num-workers=8"
                    " --devices-per-worker=2"
                    " --gpu"

ray down -y $CLUSTER

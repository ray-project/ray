#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

CLUSTER=$ROOT_DIR/stress_testing_config.yaml

# Start a large cluster using the autoscaler.
ray up -y $CLUSTER

# Run a bunch of stress tests.
ray submit $CLUSTER test_many_tasks_and_transfers.py
ray submit $CLUSTER test_dead_actors.py

# Tear down the cluster.
ray down -y $CLUSTER

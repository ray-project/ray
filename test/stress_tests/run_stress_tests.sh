#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# Start a large cluster using the autoscaler.
ray up -y $ROOT_DIR/stress_testing_config.yaml

# Run a bunch of stress tests.
ray submit $ROOT_DIR/stress_testing_config.yaml test_many_tasks_and_transfers.py
ray submit $ROOT_DIR/stress_testing_config.yaml test_dead_actors.py

# Tear down the cluster.
ray down -y $ROOT_DIR/stress_testing_config.yaml

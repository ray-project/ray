#!/usr/bin/env bash

set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR"

# Kill all of the workloads.
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- $workload_file)
  workload_name="${file_name%.*}"
  ray down -y config.yaml --cluster-name="$workload_name" &
done
# Wait for all of the ray down commands to finish.
wait

popd

#!/usr/bin/env bash

# set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR"

for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- $workload_file)
  workload_name="${file_name%.*}"
  echo "======================================================================"
  echo "WORKLOAD: $workload_name"
  echo "======================================================================"

  ray exec config.yaml --cluster-name="$workload_name" "tmux capture-pane -p"
  echo ""
  echo "ssh to this machine with:"
  echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=$workload_name"
  echo ""
  echo ""
done

popd

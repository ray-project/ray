#!/usr/bin/env bash

set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR"

# Start one instance per workload.
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- $workload_file)
  workload_name="${file_name%.*}"
  ray up -y config.yaml --cluster-name="$workload_name" &
done

# Wait for all of the nodes to be up.
for pid in `jobs -p`; do
  wait $pid
done

# Start the workloads running.
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- $workload_file)
  workload_name="${file_name%.*}"
  ray rsync_up config.yaml --cluster-name="$workload_name" "$workload_file" "$file_name"
  ray exec config.yaml --cluster-name="$workload_name" "python $file_name" --tmux
done

popd

# Print some helpful information.

echo ""
echo ""

echo "To kill the instances, use the following commands."
echo ""
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- $workload_file)
  workload_name="${file_name%.*}"
  echo "    ray down -y $ROOT_DIR/config.yaml --cluster-name=$workload_name"
done

echo ""
echo ""

echo "Use the following commands to attach to the relevant drivers."
echo ""
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- $workload_file)
  workload_name="${file_name%.*}"
  echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=$workload_name --tmux"
done

echo ""
echo ""

echo "To check up on the scripts, run the following."
echo "    $ROOT_DIR/check_workloads.sh"

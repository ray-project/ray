#!/usr/bin/env bash

set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
pushd "$ROOT_DIR"

# Substitute in the appropriate Ray version and commit in the config file and
# store it in a temporary file.
CLUSTER_CONFIG="config.yaml"

if grep -q RAY_WHEEL_TO_TEST_HERE $CLUSTER_CONFIG; then
    echo "You must replace the RAY_WHEEL_TO_TEST_HERE string in $CLUSTER_CONFIG."
    exit 1
fi

# Start one instance per workload.
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- "$workload_file")
  workload_name="${file_name%.*}"
  ray up -y $CLUSTER_CONFIG --cluster-name="$workload_name" &
done
# Wait for all of the nodes to be up.
wait

status=$?
if [ $status != 0 ]; then
    echo "Some update processes failed with $status"
    exit 1
fi

# Start the workloads running.
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- "$workload_file")
  workload_name="${file_name%.*}"
  (
      # Copy the workload to the cluster.
      ray rsync_up $CLUSTER_CONFIG --cluster-name="$workload_name" "$workload_file" "$file_name"
      # Clean up previous runs if relevant.
      ray exec $CLUSTER_CONFIG --cluster-name="$workload_name" "source activate tensorflow_p36 && ray stop; rm -r /tmp/ray; tmux kill-server | true"
      # Start the workload.
      ray exec $CLUSTER_CONFIG --cluster-name="$workload_name" "source activate tensorflow_p36 && python $file_name" --tmux
   ) &
done
# Wait for child processes to finish.
wait

popd

# Print some helpful information.

echo ""
echo ""

echo "Use the following commands to attach to the relevant drivers."
echo ""
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- "$workload_file")
  workload_name="${file_name%.*}"
  echo "    ray attach $ROOT_DIR/$CLUSTER_CONFIG --cluster-name=$workload_name --tmux"
done

echo ""
echo ""

echo "To shut down all instances, run the following."
echo "    $ROOT_DIR/shut_down_workloads.sh"

echo ""
echo ""

echo "To check up on the scripts, run the following."
echo "    $ROOT_DIR/check_workloads.sh --load"
echo "    $ROOT_DIR/check_workloads.sh --logs"

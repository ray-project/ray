#!/usr/bin/env bash

set -e

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [[ -z  "$1" ]]; then
  echo "ERROR: The first argument must be the Ray branch to test (e.g., 'master')."
  exit 1
else
  RAY_BRANCH=$1
fi

if [[ -z  "$2" ]]; then
  echo "ERROR: The second argument must be the Ray version to test (e.g., '0.8.0.dev1')."
  exit 1
else
  RAY_VERSION=$2
fi

if [[ -z  "$3" ]]; then
  echo "ERROR: The third argument must be the Ray commit to test (e.g., '62e4b591e3d6443ce25b0f05cc32b43d5e2ebb3d')."
  exit 1
else
  RAY_COMMIT=$3
fi

echo "Testing ray==$RAY_VERSION at commit $RAY_COMMIT."
echo "The wheels used will live under https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_BRANCH/$RAY_VERSION/$RAY_COMMIT/"


pushd "$ROOT_DIR"

# Substitute in the appropriate Ray version and commit in the config file and
# store it in a temporary file.
CLUSTER_CONFIG="config_temporary.yaml"
sed -e "s/<<<RAY_BRANCH>>>/$RAY_BRANCH/g;
        s/<<<RAY_VERSION>>>/$RAY_VERSION/g;
        s/<<<RAY_COMMIT>>>/$RAY_COMMIT/;" config.yaml > "$CLUSTER_CONFIG"

# Start one instance per workload.
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- "$workload_file")
  workload_name="${file_name%.*}"
  ray up -y $CLUSTER_CONFIG --cluster-name="$workload_name" &
done
# Wait for all of the nodes to be up.
wait

# Start the workloads running.
for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- "$workload_file")
  workload_name="${file_name%.*}"
  (
      # Copy the workload to the cluster.
      ray rsync_up $CLUSTER_CONFIG --cluster-name="$workload_name" "$workload_file" "$file_name"
      # Clean up previous runs if relevant.
      ray exec $CLUSTER_CONFIG --cluster-name="$workload_name" "ray stop; rm -r /tmp/ray; tmux kill-server | true"
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

#!/usr/bin/env bash

# set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

if [ "$1" == "--load" ]; then
    check_load=true
elif [ "$1" == "--logs" ]; then
    check_load=false
else
    echo "Usage: $0 [--load|--logs]"
    exit 1
fi

cd "$ROOT_DIR"

for workload_file in "$ROOT_DIR"/workloads/*; do
  file_name=$(basename -- $workload_file)
  workload_name="${file_name%.*}"
  if $check_load; then
    echo -n "$workload_name: "
    ray --logging-level=WARNING exec config.yaml --cluster-name="$workload_name" uptime 2>/dev/null || echo "<offline>"
  else
    echo "======================================================================"
    echo "WORKLOAD: $workload_name"
    echo "======================================================================"

    ray exec config.yaml --cluster-name="$workload_name" "tmux capture-pane -p"
    echo ""
    echo "ssh to this machine with:"
    echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=$workload_name"
    echo ""
    echo ""
  fi
done

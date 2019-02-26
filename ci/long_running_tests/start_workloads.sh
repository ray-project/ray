#!/usr/bin/env bash

set -ex

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR"

ray up -y config.yaml --cluster-name workload1
ray rsync_up config.yaml --cluster-name workload1 workload1.py workload1.py
ray exec config.yaml --cluster-name workload1 "python workload1.py" --tmux

ray up -y config.yaml --cluster-name workload2
ray rsync_up config.yaml --cluster-name workload2 workload2.py workload2.py
ray exec config.yaml --cluster-name workload2 "python workload2.py" --tmux

ray up -y config.yaml --cluster-name workload3
ray rsync_up config.yaml --cluster-name workload3 workload3.py workload3.py
ray exec config.yaml --cluster-name workload3 "python workload3.py" --tmux

popd

echo ""
echo ""

echo "To kill the instances, use the following commands."
echo ""
echo "    ray down -y $ROOT_DIR/config.yaml --cluster-name=workload1"
echo "    ray down -y $ROOT_DIR/config.yaml --cluster-name=workload2"
echo "    ray down -y $ROOT_DIR/config.yaml --cluster-name=workload3"

echo ""
echo ""

echo "Use the following commands to attach to the relevant drivers."
echo ""
echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=workload1 --tmux"
echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=workload2 --tmux"
echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=workload3 --tmux"

echo ""
echo ""

echo "To check up on the scripts, run $ROOT_DIR/check_workloads.sh"

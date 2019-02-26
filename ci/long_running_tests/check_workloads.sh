#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR"

echo "WORKLOAD 1"
ray exec config.yaml --cluster-name=workload1 "tmux capture-pane -p"
echo ""
echo "ssh to this machine with:"
echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=workload1"
echo ""
echo ""

echo "WORKLOAD 2"
ray exec config.yaml --cluster-name=workload2 "tmux capture-pane -p"
echo ""
echo "ssh to this machine with:"
echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=workload2"
echo ""
echo ""

echo "WORKLOAD 3"
ray exec config.yaml --cluster-name=workload3 "tmux capture-pane -p"
echo ""
echo "ssh to this machine with:"
echo "    ray attach $ROOT_DIR/config.yaml --cluster-name=workload3"
echo ""
echo ""

popd

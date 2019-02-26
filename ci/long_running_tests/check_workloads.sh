#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

pushd "$ROOT_DIR"

echo "WORKLOAD 1"
ray exec config.yaml --cluster-name workload1 "tmux capture-pane -p"
echo ""
echo ""

echo "WORKLOAD 2"
ray exec config.yaml --cluster-name workload2 "tmux capture-pane -p"
echo ""
echo ""

echo "WORKLOAD 3"
ray exec config.yaml --cluster-name workload3 "tmux capture-pane -p"
echo ""
echo ""

popd

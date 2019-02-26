#!/usr/bin/env bash

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

ray up -y config1.yaml

ray exec config1.yaml --cluster-name workload1 'python workload1.py' --tmux

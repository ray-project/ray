#!/usr/bin/env bash
set -x
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
cd "$ROOT_DIR"

clean_up() {
  ray stop --force
}
trap clean_up EXIT

which cypress || sudo npm install cypress

ray stop --force
ray start --head --dashboard-port=8653
cypress run --project . --headless

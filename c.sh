#!/bin/bash

set -euo pipefail

echo "compile requirements_compiled"

bash ci/ci.sh compile_pip_dependencie

echo "compile all depsets"

bazelisk run //ci/raydepsets:raydepsets -- build --all-configs

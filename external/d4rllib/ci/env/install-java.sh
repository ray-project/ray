#!/usr/bin/env bash

# Push caller's shell options (quietly)
{ SHELLOPTS_STACK="${SHELLOPTS_STACK-}|$(set +o); set -$-"; } 2> /dev/null

set -euxo pipefail

SCRIPT_DIR=$(builtin cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
WORKSPACE_DIR="${SCRIPT_DIR}/../.."

sudo apt-get install -y maven openjdk-8-jre openjdk-8-jdk
"${WORKSPACE_DIR}"/java/build-jar-multiplatform.sh linux

# Pop caller's shell options (quietly)
{ set -vx; eval "${SHELLOPTS_STACK##*|}"; SHELLOPTS_STACK="${SHELLOPTS_STACK%|*}"; } 2> /dev/null

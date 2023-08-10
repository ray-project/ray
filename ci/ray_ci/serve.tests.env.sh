#!/bin/bash
# This script is used to setup test environment for running core tests.

set -exo pipefail

TORCH_VERSION=1.9.0 ./ci/env/install-dependencies.sh

# The docker is only used in the ha test and it uses nightly. We will
# skip that test because it is not hermetic.
# bash ./ci/ci.sh prepare_docker

if [[ ! -f /usr/local/bin/wrk ]]; then
	git clone https://github.com/wg/wrk.git /tmp/wrk
	make -j -C /tmp/wrk
	cp /tmp/wrk/wrk /usr/local/bin/.
	rm -rf /tmp/wrk
fi

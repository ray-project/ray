#!/bin/bash
# This script is used to setup test environment for running core tests.

set -exo pipefail

pip install -U torch==1.9.0 torchvision==0.10.0
pip install -U -c python/requirements_compiled.txt \
	tensorflow tensorflow-probability
pip install -U --ignore-installed \
  -c python/requirements_compiled.txt \
  -r python/requirements.txt \
  -r python/requirements/test-requirements.txt \

# The docker is only used in the ha test and it uses nightly. We will
# skip that test because it is not hermetic.
# bash ./ci/ci.sh prepare_docker

if [[ ! -f /usr/local/bin/wrk ]]; then
	git clone https://github.com/wg/wrk.git /tmp/wrk
	make -j -C /tmp/wrk
	cp /tmp/wrk/wrk /usr/local/bin/.
	rm -rf /tmp/wrk
fi

#!/bin/bash

set -euo pipefail

export PATH=/opt/miniconda/bin:$PATH

# Unset dind settings; we are using the host's docker daemon.
unset DOCKER_TLS_CERTDIR
unset DOCKER_HOST
unset DOCKER_TLS_VERIFY
unset DOCKER_CERT_PATH

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

bash ./ci/env/env_info.sh

bazel test --config=ci --test_tag_filters=-post_wheel_build,-gpu,-xcommit \
    //python/ray/serve/... //python/ray/anyscale/serve/...

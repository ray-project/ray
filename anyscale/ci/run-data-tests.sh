#!/bin/bash

set -euo pipefail

export PATH=/opt/miniconda/bin:$PATH

# Unset dind settings; we are using the host's docker daemon.
unset DOCKER_TLS_CERTDIR
unset DOCKER_HOST
unset DOCKER_TLS_VERIFY
unset DOCKER_CERT_PATH

DATA_PROCESSING_TESTING=1 ARROW_VERSION="$1" ./ci/env/install-dependencies.sh

bash ./ci/env/env_info.sh

bazel test --config=ci --action_env=RAY_DATA_USE_STREAMING_EXECUTOR=1 \
	--test_tag_filters=-data_integration,-doctest \
	-- python/ray/data/... python/ray/anyscale/data/...

bazel test --config=ci --action_env=RAY_DATA_USE_STREAMING_EXECUTOR=1 \
	--test_tag_filters=ray_data,-doctest \
	-- python/ray/air/...

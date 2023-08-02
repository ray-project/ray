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

./ci/run/run_bazel_test_with_sharding.sh --config=ci --test_tag_filters=-data_integration,-doctest python/ray/data/...
./ci/run/run_bazel_test_with_sharding.sh --config=ci --test_tag_filters=ray_data,-doctest python/ray/air/...
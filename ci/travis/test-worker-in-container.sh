#!/usr/bin/env bash

set -euxo pipefail

cleanup() { if [ "${BUILDKITE_PULL_REQUEST}" = "false" ]; then ./ci/travis/upload_build_info.sh; fi }; trap cleanup EXIT

export STORAGE_DRIVER=vfs
podman load --input /var/lib/containers/images.tar

cat > /ray/docker/ray-nest-container/test-Dockerfile << EOF
FROM rayproject/ray-nest-container:nightly-py36-cpu
RUN $HOME/anaconda3/bin/pip --no-cache-dir install pandas
EOF

podman build -f /ray/docker/ray-nest-container/test-Dockerfile -t rayproject/ray-nest-container:nightly-py36-cpu-pandas .

export RAY_BACKEND_LOG_LEVEL=debug
"$HOME/anaconda3/bin/pip" install --no-cache-dir pytest
/ray/ci/travis/install-bazel.sh
if [ -f /etc/profile.d/bazel.sh ]; then
  . /etc/profile.d/bazel.sh
fi
bazel test --config=ci $(/ray/scripts/bazel_export_options) \
--test_tag_filters=-kubernetes,-jenkins_only,worker-nest-container,-flaky \
/ray/python/ray/tests/...

#!/usr/bin/env bash
cleanup() { if [ "${BUILDKITE_PULL_REQUEST}" = "false" ]; then ./ci/build/upload_build_info.sh; fi }; trap cleanup EXIT

set -exo pipefail

# option metacopy doesn't work on xfs
#sed -i 's/nodev,metacopy=on/nodev/' /etc/containers/storage.conf

podman load --input /var/lib/containers/images.tar

cat > /ray/docker/ray-worker-container/test-Dockerfile << EOF
FROM rayproject/ray-worker-container:nightly-py36-cpu
RUN $HOME/anaconda3/bin/pip --no-cache-dir install pandas
EOF

podman build --cgroup-manager=cgroupfs -f /ray/docker/ray-worker-container/test-Dockerfile -t rayproject/ray-worker-container:nightly-py36-cpu-pandas .

export RAY_BACKEND_LOG_LEVEL=debug
"$HOME/anaconda3/bin/pip" install --no-cache-dir pytest

pushd /ray || true
bash ./ci/env/install-bazel.sh --system

# shellcheck disable=SC2046
bazel test --test_timeout 60 --config=ci $(./ci/run/bazel_export_options) \
--test_tag_filters=-kubernetes,worker-container \
python/ray/tests/...  --test_output=all

#pytest python/ray/tests/test_actor_in_container.py  -s

popd || true

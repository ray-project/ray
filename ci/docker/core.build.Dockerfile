ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build-py3.10
FROM $DOCKER_IMAGE_BASE_BUILD

ARG PYTHON=3.10
ARG BUILD_VARIANT=build
ARG PYTHON_DEPSET=python/deplocks/ci/core-${BUILD_VARIANT}-ci_depset_py${PYTHON}.lock

SHELL ["/bin/bash", "-ice"]

COPY . .

COPY "$PYTHON_DEPSET" /home/ray/python_depset.lock

RUN <<EOF
#!/bin/bash

set -euo pipefail

uv pip install -r /home/ray/python_depset.lock --no-deps --system --index-strategy unsafe-best-match

uv pip uninstall --system ray

EOF

# erofs-utils: `mkfs.erofs --tar` builds the EROFS root filesystems that the
# sandbox tests (TEST_SANDBOX=1) cache container images as. Ubuntu 22.04
# packages erofs-utils 1.4, which predates --tar, so build a release from source.
RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -qq
apt-get install -y -qq --no-install-recommends autoconf automake libtool pkg-config uuid-dev
git clone -q --depth 1 --branch v1.9.4 https://github.com/erofs/erofs-utils.git /tmp/erofs-utils
cd /tmp/erofs-utils
test "$(git rev-parse HEAD)" = f36cadb5c563995ab3aa8572a60ed6b721b9557d
./autogen.sh
./configure --disable-fuse
make -j"$(nproc)"
make install
cd /
rm -rf /tmp/erofs-utils
# Read the whole help text: `grep -q` would close the pipe on the first
# match and turn mkfs.erofs's SIGPIPE into a pipefail failure (exit 141).
help_text="$(mkfs.erofs --help 2>&1)"
[[ "$help_text" == *--tar* ]]

EOF

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

# mount-s3 (AWS Mountpoint) and FUSE: the sandbox tests mount public buckets
# into sandboxes (bucket_mounts / modal.CloudBucketMount). mount-s3 links
# libfuse2 and unmounts through its fusermount; the job runs privileged, so
# /dev/fuse is available. Pinned to the version the code requires, by hash.
RUN <<EOF
#!/bin/bash

set -euo pipefail

apt-get update -qq
apt-get install -y -qq --no-install-recommends fuse libfuse2
case "$(uname -m)" in
  x86_64) arch=x86_64; sha256=a99bea20510eaabaf9d7cbfe95ab221a11005cacaa77cfc311a2912bbbdbfd72 ;;
  aarch64) arch=arm64; sha256=9f97026c9feabd0e4d128206f1846ee317e6c4f21846914dfe4268d110c892c4 ;;
esac
curl -sSfL -o /tmp/mount-s3.tar.gz \
  "https://s3.amazonaws.com/mountpoint-s3-release/1.24.0/${arch}/mount-s3-1.24.0-${arch}.tar.gz"
echo "${sha256}  /tmp/mount-s3.tar.gz" | sha256sum -c -
mkdir -p /opt/mount-s3
tar -xzf /tmp/mount-s3.tar.gz -C /opt/mount-s3
ln -s /opt/mount-s3/bin/mount-s3 /usr/local/bin/mount-s3
rm /tmp/mount-s3.tar.gz
mount-s3 --version

EOF

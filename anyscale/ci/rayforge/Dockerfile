# syntax=docker/dockerfile:1.3-labs

# rayforge contains the additional toolchain required for building a ray linux
# wheel.

ARG BASE_IMAGE=quay.io/pypa/manylinux2014_x86_64:2022-12-20-b4884d9
FROM $BASE_IMAGE

RUN <<EOF
#!/bin/bash

set -exuo pipefail

if [[ ! -e /usr/bin/nproc ]]; then
  echo -e '#!/bin/bash\necho 10' > "/usr/bin/nproc"
  chmod +x /usr/bin/nproc
fi

NODE_VERSION="14"

YUM_PKGS=(unzip zip sudo openssl xz)

if [[ "${HOSTTYPE-}" == "x86_64" ]]; then
  YMB_PKGS+=(
    "libasan-4.8.5-44.el7.x86_64"
    "libubsan-7.3.1-5.10.el7.x86_64"
    "devtoolset-8-libasan-devel.x86_64"
  )
fi

yum -y install "${YUM_PKGS[@]}"

# Install and use the latest version of Node.js in order to build the dashboard.
set +x
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
source "$HOME"/.nvm/nvm.sh

nvm install "$NODE_VERSION"
nvm use "$NODE_VERSION"

# Install bazelisk via npm.
npm install -g @bazel/bazelisk
ln -s "$(which bazelisk)" /usr/local/bin/bazel

echo "build --config=ci" > ~/.bazelrc
echo "build --announce_rc" >> ~/.bazelrc
echo "build:ci --remote_cache=https://bk-premerge-first-jawfish-artifacts.s3.us-west-2.amazonaws.com/bazel/cache/runtime" >> ~/.bazelrc

EOF

CMD ["/bin/bash"]

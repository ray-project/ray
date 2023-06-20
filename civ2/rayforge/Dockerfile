# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE=quay.io/pypa/manylinux2014_x86_64
FROM $BASE_IMAGE

RUN <<EOF
#!/bin/bash

set -exuo pipefail

if [[ ! -e /usr/bin/nproc ]]; then
  echo -e '#!/bin/bash\necho 10' > "/usr/bin/nproc"
  chmod +x /usr/bin/nproc
fi

NODE_VERSION="14"

YUM_PKGS=(
  unzip zip sudo openssl xz
  java-1.8.0-openjdk java-1.8.0-openjdk-devel maven
)

yum -y install "${YUM_PKGS[@]}"

java -version

# /ray/ci/env/install-bazel.sh

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

EOF
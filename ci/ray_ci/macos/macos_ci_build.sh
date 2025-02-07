#!/bin/bash

set -ex

export CI="true"
export PYTHON="3.9"
export RAY_USE_RANDOM_PORTS="1"
export RAY_DEFAULT_BUILD="1"
export LC_ALL="en_US.UTF-8"
export LANG="en_US.UTF-8"
export BUILD="1"
export DL="1"
export TORCH_VERSION=2.0.1
export TORCHVISION_VERSION=0.15.2


build_x86_64() {
  # Cleanup environments
  rm -rf /tmp/bazel_event_logs
  cleanup() { if [ "${BUILDKITE_PULL_REQUEST}" = "false" ]; then ./ci/build/upload_build_info.sh; fi }; trap cleanup EXIT
  (which bazel && bazel clean) || true
  # TODO(simon): make sure to change both PR and wheel builds
  # Special setup for jar builds (will be installed to the machine instead)
  # - brew remove --force java & brew uninstall --force java & rm -rf /usr/local/Homebrew/Library/Taps/homebrew/homebrew-cask
  # - brew install --cask adoptopenjdk/openjdk/adoptopenjdk8
  diskutil list external physical
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home
  java -version
  # Build wheels
  export UPLOAD_WHEELS_AS_ARTIFACTS=1
  export MAC_WHEELS=1
  export MAC_JARS=1
  export RAY_INSTALL_JAVA=1
  export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
  . ./ci/ci.sh init && source ~/.zshenv
  source ~/.zshrc
  ./ci/ci.sh build
  # Test wheels
  ./ci/ci.sh test_wheels
  # Build jars
  bash ./java/build-jar-multiplatform.sh darwin
  # Upload the wheels and jars
  # We don't want to push on PRs, in fact, the copy_files will fail because unauthenticated.
  if [ "$BUILDKITE_PULL_REQUEST" != "false" ]; then exit 0; fi
  pip install -q docker aws_requests_auth boto3
  # Upload to branch directory.
  python .buildkite/copy_files.py --destination branch_wheels --path ./.whl
  python .buildkite/copy_files.py --destination branch_jars --path ./.jar/darwin
  # Upload to latest directory.
  if [ "$BUILDKITE_BRANCH" = "master" ]; then python .buildkite/copy_files.py --destination wheels --path ./.whl; fi
  if [ "$BUILDKITE_BRANCH" = "master" ]; then python .buildkite/copy_files.py --destination jars --path ./.jar/darwin; fi
}

build_aarch64() {
  # Cleanup environments
  rm -rf /tmp/bazel_event_logs
  cleanup() { if [ "${BUILDKITE_PULL_REQUEST}" = "false" ]; then ./ci/build/upload_build_info.sh; fi }; trap cleanup EXIT
  (which bazel && bazel clean) || true
  brew install pkg-config nvm node || true
  # TODO(simon): make sure to change both PR and wheel builds
  # Special setup for jar builds (will be installed to the machine instead)
  # - brew remove --force java & brew uninstall --force java & rm -rf /usr/local/Homebrew/Library/Taps/homebrew/homebrew-cask
  # - brew install --cask adoptopenjdk/openjdk/adoptopenjdk8
  diskutil list external physical
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home
  java -version
  # Build wheels
  export UPLOAD_WHEELS_AS_ARTIFACTS=1
  export MAC_WHEELS=1
  export MAC_JARS=1
  export RAY_INSTALL_JAVA=1
  export RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER=1
  export MINIMAL_INSTALL=1
  . ./ci/ci.sh init && source ~/.zshenv
  source ~/.zshrc
  ./ci/ci.sh build
  # Test wheels
  ./ci/ci.sh test_wheels
  # Build jars
  bash ./java/build-jar-multiplatform.sh darwin
  # Upload the wheels and jars
  # We don't want to push on PRs, in fact, the copy_files will fail because unauthenticated.
  if [ "$BUILDKITE_PULL_REQUEST" != "false" ]; then exit 0; fi
  python -m pip install -q docker aws_requests_auth boto3
  # Upload to branch directory.
  python .buildkite/copy_files.py --destination branch_wheels --path ./.whl
  python .buildkite/copy_files.py --destination branch_jars --path ./.jar/darwin
  # Upload to latest directory.
  if [ "$BUILDKITE_BRANCH" = "master" ]; then python .buildkite/copy_files.py --destination wheels --path ./.whl; fi
  if [ "$BUILDKITE_BRANCH" = "master" ]; then python .buildkite/copy_files.py --destination jars --path ./.jar/darwin; fi
}

"$@"

#!/bin/bash

GIT_BRANCH=$1
GIT_COMMIT=$2

if [ ! -d ~/ray ]; then
  git clone https://github.com/ray-project/ray.git ~/ray
fi

pushd ~/ray || exit 1
git fetch
git checkout "$GIT_BRANCH"
git pull origin "$GIT_BRANCH"
git checkout "$GIT_COMMIT"

bash ~/ray/ci/install_bazel.sh
export PATH="/home/ray/bin:$PATH"

cd ~/ray/ci/asan_tests/ && bash ./run.sh --git-sha="${GIT_COMMIT}"
popd || true
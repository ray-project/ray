#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

# run this file before compile the targets
sh $ROOT_DIR/generate_deps.sh

echo "Compiling Java code."
pushd $ROOT_DIR/..
# compile all the targets
bazel build //java:all --verbose_failures

# bazel checkstyle for java
bazel test //java:all --test_tag_filters="checkstyle"

# The following are soft links
# TODO: remove this once cmake is removed
mkdir -p $ROOT_DIR/../build/java/
ln -sf $ROOT_DIR/../bazel-bin/java/* $ROOT_DIR/../build/java/
mkdir -p $ROOT_DIR/tutorial/target/
ln -sf $ROOT_DIR/../bazel-bin/java/org_ray_ray_tutorial_deploy.jar $ROOT_DIR/tutorial/target/ray-tutorial-0.1-SNAPSHOT.jar

echo "Running tests under cluster mode."
ENABLE_MULTI_LANGUAGE_TESTS=1 bazel test //java:all_tests --jvmopt="-Dray.home=$ROOT_DIR/.." --test_output="errors" || cluster_exit_code=$?

# exit_code == 2 means there are some tests skiped.
if [ $cluster_exit_code -eq 2 ] && [ $cluster_exit_code -eq 0 ] ; then
    exit $exit_code
fi

echo "Running tests under single-process mode."
#bazel test //java:all_tests --jvmopt="-Dray.run-mode=SINGLE_PROCESS -Dray.home=$ROOT_DIR/.." --test_output="errors" || single_exit_code=$?

# exit_code == 2 means there are some tests skiped.
if [ $single_exit_code -eq 2 ] && [ $single_exit_code -eq 0 ] ; then
    exit $exit_code
fi

popd

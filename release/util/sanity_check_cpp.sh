#!/usr/bin/env bash

# This script generate a ray C++ template an run example
set -e
rm -rf ray-template && mkdir ray-template
ray cpp --generate-bazel-project-template-to ray-template
pushd ray-template && sh run.sh

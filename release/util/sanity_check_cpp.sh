#!/usr/bin/env bash

# This script generate a ray C++ template and run example
set -e
rm -rf ray-template
ray cpp --generate-bazel-project-template-to ray-template
pushd ray-template && bash run.sh

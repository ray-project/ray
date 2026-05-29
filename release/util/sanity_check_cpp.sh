#!/usr/bin/env bash

# This script generate a ray C++ template and run example
set -e
rm -rf ray-template
ray cpp --generate-bazel-project-template-to ray-template
(
    cd ray-template

    # Our generated CPP template does not work with bazel 7.x ,
    # so pin the bazel version to 6
    USE_BAZEL_VERSION=6.5.0 bash run.sh
)

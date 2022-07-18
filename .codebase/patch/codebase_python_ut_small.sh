#!/usr/bin/env bash
source .codebase/patch/_codebase_prepare.sh
MINIMAL_INSTALL=1 PYTHON=3.9 INSTALL_BAZEL=1 NODE_VERSION=14 source ci/env/install-dependencies.sh
echo "build --config=ci" >> ~/.bazelrc
export PATH=/root/bin:$PATH
ci/ci.sh build
pip install -r python/requirements.txt
bazel test --config=ci --test_tag_filters=client_tests,small_size_python_tests,-test_debug_tools,-test_client_library_integration -- python/ray/tests/...
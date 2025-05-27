#!/usr/bin/env bash
source .codebase/patch/_codebase_prepare.sh
MINIMAL_INSTALL=1 PYTHON=3.9 INSTALL_BAZEL=1 source ci/env/install-dependencies.sh
echo "build --config=ci" >> ~/.bazelrc
export PATH=/root/bin:$PATH

cd .whl
ls | grep "cp39-cp39-manylinux2014" | xargs printf -- '%s[tune,data,train,serve,default]\n' | xargs python3 -m pip install -i https://bytedpypi.byted.org/simple
cd ..

bash copy_code_from_repo.sh
pip3 install -r .codebase/patch/requirements.txt

bazel test --config=ci --test_tag_filters=client_tests -- python/ray/tests/...
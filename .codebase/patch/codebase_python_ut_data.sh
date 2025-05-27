#!/usr/bin/env bash
source .codebase/patch/_codebase_prepare.sh
MINIMAL_INSTALL=1 PYTHON=3.9 INSTALL_BAZEL=1 source ci/env/install-dependencies.sh
echo "build --config=ci" >> ~/.bazelrc
export PATH=/root/bin:$PATH

cd .whl
ls | grep "cp39-cp39-manylinux2014" | xargs printf -- '%s[tune,data,train,serve,default]\n' | xargs python3 -m pip install -i https://bytedpypi.byted.org/simple
cd ..

cp -r python/ray/data/examples/ /opt/miniconda/lib/python3.9/site-packages/ray/data/
cp -r python/ray/data/tests/ /opt/miniconda/lib/python3.9/site-packages/ray/data/
cp -r python/ray/serve/tests/ /opt/miniconda/lib/python3.9/site-packages/ray/serve/
cp -r python/ray/tests/ /opt/miniconda/lib/python3.9/site-packages/ray/
cp -r python/ray/experimental/ /opt/miniconda/lib/python3.9/site-packages/ray/

pip install -r .codebase/patch/requirements.txt
pip install torch==1.13.0
pip install datasets==2.0.0
pip install tensorflow==2.11.0
pip install torchvision==0.14.0
pip install polars==0.14.21
pip install tensorflow_datasets
bazel test --config=ci --test_tag_filters=-data_integration,-ray_air,-bytedance_exclude,-large python/ray/data/...
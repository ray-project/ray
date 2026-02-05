#!/usr/bin/env bash
set -euo pipefail

apt-get update
apt-get install -y libopenblas-dev liblapack-dev gcc g++ gfortran pkg-config git \
                    make cmake ninja-build openssl libcurl4-openssl-dev

pip install meson ninja 'numpy<1.23' \
    Cython==0.29.37 'meson-python<0.15.0,>=0.12.1' \
    pybind11 'patchelf>=0.11.0' 'pythran<0.15.0,>=0.12.0' build \
    wheel setuptools_scm

git clone https://github.com/scipy/scipy
cd scipy
git checkout v1.11.4
git submodule update --init
pip install -e .
cd /home/ray


PYARROW_VER=19.0.1
echo "[prebuild-power] Building pyarrow==$PYARROW_VER ..."
git clone https://github.com/apache/arrow.git
cd arrow
git checkout "apache-arrow-${PYARROW_VER}"
git submodule update --init --recursive
cd cpp
mkdir -p release && cd release
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/usr/local \
      -DARROW_PYTHON=ON \
      -DARROW_PARQUET=ON \
      -DARROW_ORC=ON \
      -DARROW_FILESYSTEM=ON \
      -DARROW_WITH_LZ4=ON \
      -DARROW_WITH_ZSTD=ON \
      -DARROW_WITH_SNAPPY=ON \
      -DARROW_JSON=ON \
      -DARROW_CSV=ON \
      -DARROW_DATASET=ON \
      -DARROW_S3=ON \
      -DARROW_BUILD_TESTS=OFF \
      -DARROW_SUBSTRAIT=ON \
      -DProtobuf_SOURCE=BUNDLED \
      -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    ..
make -j"$(nproc)"
sudo make install
cd ../../python
export BUILD_TYPE=release
python setup.py build_ext --build-type=$BUILD_TYPE --bundle-arrow-cpp bdist_wheel
pip install dist/*.whl

# Cleanup Arrow sources and build artifacts
cd /home/ray
rm -rf arrow scipy

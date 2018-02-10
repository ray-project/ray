conda create -y -q -n pyarrow-dev \
      python=3.6 numpy six setuptools cython pandas pytest \
      cmake flatbuffers rapidjson boost-cpp thrift-cpp snappy zlib \
      gflags brotli jemalloc lz4-c zstd -c conda-forge
source activate pyarrow-dev

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
PARQUET_HOME=$TP_DIR/arrow/cpp/build/cpp-install

cd $TP_DIR/parquet-cpp
ARROW_HOME=$TP_DIR/arrow/cpp \
cmake -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_BUILD_TESTS=off \
      .

make -j4
make install

source deactivate
conda remove --name pyarrow-dev --all -y -q
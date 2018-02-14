if [ $TRAVIS_OS_NAME == "osx" ]; then
  brew update > /dev/null
  brew install boost
  brew install openssl
  brew install bison
  export OPENSSL_ROOT_DIR=/usr/local/opt/openssl
  export LD_LIBRARY_PATH=/usr/local/opt/openssl/lib:$LD_LIBRARY_PATH
  export PATH="/usr/local/opt/bison/bin:$PATH"
else
  # Use a C++11 compiler on Linux
  export CC="gcc-4.9"
  export CXX="g++-4.9"
fi

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

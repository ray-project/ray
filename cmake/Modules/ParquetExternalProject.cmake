# parquet external project
# target:
#  - parquet_ep
# depends:
#  - arrow_ep
# defines:
#  - PARQUET_HOME
#  - PARQUET_INCLUDE_DIR
#  - PARQUET_STATIC_LIB
#  - PARQUET_SHARED_LIB

include(ExternalProject)

set(parquet_URL https://github.com/apache/parquet-cpp.git)
set(parquet_TAG 63f41b00bddecb172bd5b3aa0366b4653f498811)

set(PARQUET_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/parquet-install)
set(PARQUET_HOME ${PARQUET_INSTALL_PREFIX})
set(PARQUET_INCLUDE_DIR ${PARQUET_INSTALL_PREFIX}/include)
set(PARQUET_STATIC_LIB ${PARQUET_INSTALL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}parquet.a)
set(PARQUET_SHARED_LIB ${PARQUET_INSTALL_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_SHARED_LIBRARY_SUFFIX})

ExternalProject_Add(parquet_ep
    PREFIX external/parquet
    DEPENDS arrow_ep
    GIT_REPOSITORY ${parquet_URL}
    GIT_TAG ${parquet_TAG}
    CMAKE_ARGS
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${PARQUET_INSTALL_PREFIX}
    -DARROW_HOME=${ARROW_HOME}
    -DBOOST_ROOT=${BOOST_ROOT}
    -DPARQUET_BUILD_BENCHMARKS=off
    -DPARQUET_BUILD_EXECUTABLES=off
    -DPARQUET_BUILD_TESTS=off
    )

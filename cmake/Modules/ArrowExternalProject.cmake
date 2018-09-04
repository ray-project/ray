# arrow external project
# target:
#  - arrow_ep
# depends:
#
# this module defines:
#  - ARROW_HOME
#  - ARROW_SOURCE_DIR
#  - ARROW_INCLUDE_DIR
#  - ARROW_SHARED_LIB
#  - ARROW_STATIC_LIB
#  - PLASMA_INCLUDE_DIR
#  - PLASMA_STATIC_LIB
#  - PLASMA_SHARED_LIB

set(arrow_URL https://github.com/apache/arrow.git)
set(arrow_TAG fda4b3dcfc773612b12973df5053193f236fc696)

set(ARROW_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/arrow-install)
set(ARROW_HOME ${ARROW_INSTALL_PREFIX})
set(ARROW_SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/external/arrow/src/arrow_ep)

# The following is needed because in CentOS, the lib directory is named lib64
if(EXISTS "/etc/redhat-release" AND CMAKE_SIZEOF_VOID_P EQUAL 8)
  set(LIB_SUFFIX 64)
endif()

set(ARROW_INCLUDE_DIR ${ARROW_HOME}/include)
set(ARROW_LIBRARY_DIR ${ARROW_HOME}/lib${LIB_SUFFIX})
set(ARROW_SHARED_LIB ${ARROW_LIBRARY_DIR}/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_STATIC_LIB ${ARROW_LIBRARY_DIR}/libarrow.a)

# plasma in arrow
set(PLASMA_INCLUDE_DIR ${ARROW_HOME}/include)
set(PLASMA_SHARED_LIB ${ARROW_LIBRARY_DIR}/libplasma${CMAKE_SHARED_LIBRARY_SUFFIX})
set(PLASMA_STATIC_LIB ${ARROW_LIBRARY_DIR}/libplasma.a)

set(ARROW_CMAKE_ARGS
    -DCMAKE_BUILD_TYPE:STRING=Release
    -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_PREFIX}
    -DCMAKE_C_FLAGS=-g -O3 ${EP_C_FLAGS}
    -DCMAKE_CXX_FLAGS=-g -O3 ${EP_CXX_FLAGS}
    -DARROW_BUILD_TESTS=off
    -DARROW_HDFS=on
    -DARROW_BOOST_USE_SHARED=off
    -DARROW_PYTHON=on
    -DARROW_PLASMA=on
    -DARROW_TENSORFLOW=on
    -DARROW_JEMALLOC=off
    -DARROW_WITH_BROTLI=off
    -DARROW_WITH_LZ4=off
    -DARROW_WITH_ZLIB=off
    -DARROW_WITH_ZSTD=off
    -DARROW_PLASMA_JAVA_CLIENT=ON
    -DFLATBUFFERS_HOME=${FLATBUFFERS_HOME}
    -DBOOST_ROOT=${BOOST_ROOT}
    )

ExternalProject_Add(arrow_ep
    PREFIX external/arrow
    DEPENDS flatbuffers_ep boost_ep
    GIT_REPOSITORY ${arrow_URL}
    GIT_TAG ${arrow_TAG}
    SOURCE_SUBDIR cpp
    BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${ARROW_STATIC_LIB}"
    CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    )

if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "YES")
  ExternalProject_Add_Step(arrow_ep arrow_ep_install_java_lib
    COMMAND cd ${ARROW_SOURCE_DIR}/java && mvn clean install -pl plasma -am -Dmaven.test.skip
    DEPENDEES build)

  # add install of library plasma_java, it is not configured in plasma CMakeLists.txt
  ExternalProject_Add_Step(arrow_ep arrow_ep_install_plasma_java
    COMMAND bash -c "cp ${CMAKE_CURRENT_BINARY_DIR}/external/arrow/src/arrow_ep-build/release/libplasma_java.* ${ARROW_LIBRARY_DIR}/"
    DEPENDEES install)
endif ()

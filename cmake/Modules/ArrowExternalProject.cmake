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
#  - ARROW_LIBRARY_DIR
#  - PLASMA_INCLUDE_DIR
#  - PLASMA_STATIC_LIB
#  - PLASMA_SHARED_LIB

set(arrow_URL https://github.com/pcmoritz/arrow.git)
# The PR for this commit is https://github.com/apache/arrow/pull/2826. We
# include the link here to make it easier to find the right commit because
# Arrow often rewrites git history and invalidates certain commits.
set(arrow_TAG 140e9509f99478d4d1e7aa52f7e13f81c3274251)

set(ARROW_INSTALL_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/external/arrow-install)
set(ARROW_HOME ${ARROW_INSTALL_PREFIX})
set(ARROW_SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/external/arrow/src/arrow_ep)

set(ARROW_INCLUDE_DIR ${ARROW_HOME}/include)
set(ARROW_LIBRARY_DIR ${ARROW_HOME}/lib${LIB_SUFFIX})
set(ARROW_SHARED_LIB ${ARROW_LIBRARY_DIR}/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX})
set(ARROW_STATIC_LIB ${ARROW_LIBRARY_DIR}/libarrow.a)

# plasma in arrow
set(PLASMA_INCLUDE_DIR ${ARROW_HOME}/include)
set(PLASMA_SHARED_LIB ${ARROW_LIBRARY_DIR}/libplasma${CMAKE_SHARED_LIBRARY_SUFFIX})
set(PLASMA_STATIC_LIB ${ARROW_LIBRARY_DIR}/libplasma.a)

find_package(PythonInterp REQUIRED)
message(STATUS "PYTHON_EXECUTABLE for arrow: ${PYTHON_EXECUTABLE}")

set(ARROW_CMAKE_ARGS
  -DCMAKE_BUILD_TYPE:STRING=Release
  -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_PREFIX}
  -DCMAKE_C_FLAGS=-g -O3 ${EP_C_FLAGS}
  -DCMAKE_CXX_FLAGS=-g -O3 ${EP_CXX_FLAGS}
  -DARROW_BUILD_TESTS=off
  -DARROW_HDFS=on
  -DARROW_BOOST_USE_SHARED=off
  -DPYTHON_EXECUTABLE:FILEPATH=${PYTHON_EXECUTABLE}
  -DARROW_PYTHON=on
  -DARROW_PLASMA=on
  -DARROW_TENSORFLOW=on
  -DARROW_JEMALLOC=off
  -DARROW_WITH_BROTLI=off
  -DARROW_WITH_LZ4=off
  -DARROW_WITH_ZSTD=off
  -DFLATBUFFERS_HOME=${FLATBUFFERS_HOME}
  -DBOOST_ROOT=${BOOST_ROOT}
  -DGLOG_HOME=${GLOG_HOME})

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  # PyArrow needs following settings.
  set(ARROW_CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    -DARROW_WITH_THRIFT=ON
    -DARROW_PARQUET=ON
    -DARROW_WITH_ZLIB=ON)
else()
  set(ARROW_CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    -DARROW_WITH_THRIFT=OFF
    -DARROW_PARQUET=OFF
    -DARROW_WITH_ZLIB=OFF)
endif ()
if (APPLE)
  set(ARROW_CMAKE_ARGS ${ARROW_CMAKE_ARGS}
    -DBISON_EXECUTABLE=/usr/local/opt/bison/bin/bison)
endif()

if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "YES")
  set(ARROW_CMAKE_ARGS ${ARROW_CMAKE_ARGS} -DARROW_PLASMA_JAVA_CLIENT=ON)
endif ()

message(STATUS "ARROW_CMAKE_ARGS: ${ARROW_CMAKE_ARGS}")

if (CMAKE_VERSION VERSION_GREATER "3.7")
  set(ARROW_CONFIGURE SOURCE_SUBDIR "cpp" CMAKE_ARGS ${ARROW_CMAKE_ARGS})
else()
  set(ARROW_CONFIGURE CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}"
    ${ARROW_CMAKE_ARGS} "${ARROW_SOURCE_DIR}/cpp")
endif()

ExternalProject_Add(arrow_ep
  PREFIX external/arrow
  DEPENDS flatbuffers boost glog
  GIT_REPOSITORY ${arrow_URL}
  GIT_TAG ${arrow_TAG}
  UPDATE_COMMAND ""
  ${ARROW_CONFIGURE}
  BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${ARROW_STATIC_LIB}")

if ("${CMAKE_RAY_LANG_JAVA}" STREQUAL "YES")
  set_property(DIRECTORY APPEND PROPERTY ADDITIONAL_MAKE_CLEAN_FILES "${ARROW_SOURCE_DIR}/java/target/")

  if(NOT EXISTS ${ARROW_SOURCE_DIR}/java/target/)
    ExternalProject_Add_Step(arrow_ep arrow_ep_install_java_lib
      COMMAND bash -c "cd ${ARROW_SOURCE_DIR}/java && mvn clean install -pl plasma -am -Dmaven.test.skip > /dev/null"
      DEPENDEES build)
  endif()

  # add install of library plasma_java, it is not configured in plasma CMakeLists.txt
  ExternalProject_Add_Step(arrow_ep arrow_ep_install_plasma_java
    COMMAND bash -c "cp -rf ${CMAKE_CURRENT_BINARY_DIR}/external/arrow/src/arrow_ep-build/release/libplasma_java.* ${ARROW_LIBRARY_DIR}/"
    DEPENDEES install)
endif ()

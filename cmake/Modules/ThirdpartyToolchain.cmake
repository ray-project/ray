set(GFLAGS_VERSION "2.2.0")
set(GTEST_VERSION "1.8.0")
set(GBENCHMARK_VERSION "1.1.0")

# Because we use the old C++ ABI to be compatible with TensorFlow,
# we have to turn it on for dependencies too
set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=0")

if(RAY_BUILD_TESTS OR RAY_BUILD_BENCHMARKS)
  add_custom_target(unittest ctest -L unittest)

  if(APPLE)
    set(GTEST_CMAKE_CXX_FLAGS "-fPIC -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
  elseif(NOT MSVC)
    set(GTEST_CMAKE_CXX_FLAGS "-fPIC")
  endif()
  if(CMAKE_BUILD_TYPE)
    string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
  endif()
  set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}} ${GTEST_CMAKE_CXX_FLAGS}")

  set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/googletest/src/googletest_ep")
  set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
  set(GTEST_STATIC_LIB
    "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(GTEST_MAIN_STATIC_LIB
    "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(GMOCK_MAIN_STATIC_LIB
    "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(GTEST_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                       -DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}
                       -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})
  if (MSVC AND NOT ARROW_USE_STATIC_CRT)
    set(GTEST_CMAKE_ARGS ${GTEST_CMAKE_ARGS} -Dgtest_force_shared_crt=ON)
  endif()

  set(GTEST_URL_MD5 "16877098823401d1bf2ed7891d7dce36")

  ExternalProject_Add(googletest_ep
    PREFIX external/googletest
    URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
    URL_MD5 ${GTEST_URL_MD5}
    BUILD_BYPRODUCTS ${GTEST_STATIC_LIB} ${GTEST_MAIN_STATIC_LIB} ${GMOCK_MAIN_STATIC_LIB}
    CMAKE_ARGS ${GTEST_CMAKE_ARGS}
    ${EP_LOG_OPTIONS})

  message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
  message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gtest
    STATIC_LIB ${GTEST_STATIC_LIB})
  ADD_THIRDPARTY_LIB(gtest_main
    STATIC_LIB ${GTEST_MAIN_STATIC_LIB})
  ADD_THIRDPARTY_LIB(gmock_main
    STATIC_LIB ${GMOCK_MAIN_STATIC_LIB})

  add_dependencies(gtest googletest_ep)
  add_dependencies(gtest_main googletest_ep)
  add_dependencies(gmock_main googletest_ep)
endif()

if(RAY_USE_GLOG)
  message(STATUS "Starting to build glog")
  set(GLOG_VERSION "0.3.5")
  # keep the url md5 equals with the version, `md5 v0.3.5.tar.gz`
  set(GLOG_URL_MD5 "5df6d78b81e51b90ac0ecd7ed932b0d4")
  set(GLOG_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
  if(APPLE)
    set(GLOG_CMAKE_CXX_FLAGS "${GLOG_CMAKE_CXX_FLAGS} -mmacosx-version-min=10.12")
  endif()

  set(GLOG_URL "https://github.com/google/glog/archive/v${GLOG_VERSION}.tar.gz")
  set(GLOG_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/glog/src/glog_ep")
  set(GLOG_HOME "${GLOG_PREFIX}")
  set(GLOG_INCLUDE_DIR "${GLOG_PREFIX}/include")
  set(GLOG_STATIC_LIB "${GLOG_PREFIX}/lib/libglog.a")


  set(GLOG_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                        -DCMAKE_INSTALL_PREFIX=${GLOG_PREFIX}
                        -DBUILD_SHARED_LIBS=OFF
                        -DBUILD_TESTING=OFF
                        -DWITH_GFLAGS=OFF
                        -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${GLOG_CMAKE_CXX_FLAGS}
                        -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
                        -DCMAKE_CXX_FLAGS=${GLOG_CMAKE_CXX_FLAGS})

  ExternalProject_Add(glog_ep
    PREFIX external/glog
    URL ${GLOG_URL}
    URL_MD5 ${GLOG_URL_MD5}
    ${EP_LOG_OPTIONS}
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "${GLOG_STATIC_LIB}"
    CMAKE_ARGS ${GLOG_CMAKE_ARGS})

  message(STATUS "GLog include dir: ${GLOG_INCLUDE_DIR}")
  message(STATUS "GLog static library: ${GLOG_STATIC_LIB}")
  include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(glog
    STATIC_LIB ${GLOG_STATIC_LIB})

  add_dependencies(glog glog_ep)
endif()

# boost
include(BoostExternalProject)

message(STATUS "Boost root: ${BOOST_ROOT}")
message(STATUS "Boost include dir: ${Boost_INCLUDE_DIR}")
message(STATUS "Boost system library: ${Boost_SYSTEM_LIBRARY}")
message(STATUS "Boost filesystem library: ${Boost_FILESYSTEM_LIBRARY}")
include_directories(${Boost_INCLUDE_DIR})

# flatbuffers
include(FlatBuffersExternalProject)

message(STATUS "Flatbuffers home: ${FLATBUFFERS_HOME}")
message(STATUS "Flatbuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
message(STATUS "Flatbuffers static library: ${FLATBUFFERS_STATIC_LIB}")
message(STATUS "Flatbuffers compiler: ${FLATBUFFERS_COMPILER}")
include_directories(SYSTEM ${FLATBUFFERS_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(flatbuffers STATIC_LIB ${FLATBUFFERS_STATIC_LIB})

add_dependencies(flatbuffers flatbuffers_ep)

# Apache Arrow, use FLATBUFFERS_HOME and BOOST_ROOT
include(ArrowExternalProject)

message(STATUS "Arrow home: ${ARROW_HOME}")
message(STATUS "Arrow source dir: ${ARROW_SOURCE_DIR}")
message(STATUS "Arrow include dir: ${ARROW_INCLUDE_DIR}")
message(STATUS "Arrow static library: ${ARROW_STATIC_LIB}")
message(STATUS "Arrow shared library: ${ARROW_SHARED_LIB}")
include_directories(SYSTEM ${ARROW_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(arrow STATIC_LIB ${ARROW_STATIC_LIB})

add_dependencies(arrow arrow_ep)

# Plasma, it is already built in arrow
message(STATUS "Plasma include dir: ${PLASMA_INCLUDE_DIR}")
message(STATUS "Plasma static library: ${PLASMA_STATIC_LIB}")
message(STATUS "Plasma shared library: ${PLASMA_SHARED_LIB}")
include_directories(SYSTEM ${PLASMA_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(plasma STATIC_LIB ${PLASMA_STATIC_LIB})

add_dependencies(plasma plasma_ep)

if ("${CMAKE_RAY_LANG_PYTHON}" STREQUAL "YES")
  # Apache parquet cpp
  include(ParquetExternalProject)

  message(STATUS "Parquet home: ${PARQUET_HOME}")
  message(STATUS "Parquet include dir: ${PARQUET_INCLUDE_DIR}")
  message(STATUS "Parquet static library: ${PARQUET_STATIC_LIB}")
  message(STATUS "Parquet shared library: ${PARQUET_SHARED_LIB}")
  include_directories(SYSTEM ${PARQUET_INCLUDE_DIR})

  ADD_THIRDPARTY_LIB(parquet STATIC_LIB ${PARQUET_STATIC_LIB})

  add_dependencies(parquet parquet_ep)

  # pyarrow
  find_package(PythonInterp REQUIRED)
  message(STATUS "PYTHON_EXECUTABLE: ${PYTHON_EXECUTABLE}")

  set(pyarrow_ENV
    "PKG_CONFIG_PATH=${ARROW_HOME}/lib/pkgconfig"
    "PYARROW_WITH_PLASMA=1"
    "PYARROW_WITH_TENSORFLOW=1"
    "PYARROW_BUNDLE_ARROW_CPP=1"
    "PARQUET_HOME=${PARQUET_HOME}"
    "PYARROW_WITH_PARQUET=1"
  )

  # here we use externalProject to process pyarrow building
  # add_custom_command would have problem with setup.py
  ExternalProject_Add(pyarrow_ext
    PREFIX external/pyarrow
    DEPENDS parquet_ep
    DOWNLOAD_COMMAND ""
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND cd ${ARROW_SOURCE_DIR}/python && ${CMAKE_COMMAND} -E env ${pyarrow_ENV} ${PYTHON_EXECUTABLE} setup.py build
    BUILD_COMMAND cd ${ARROW_SOURCE_DIR}/python && ${CMAKE_COMMAND} -E env ${pyarrow_ENV} ${PYTHON_EXECUTABLE} setup.py build_ext
    INSTALL_COMMAND bash -c "cp -r \$(find ${ARROW_SOURCE_DIR}/python/build/ -maxdepth 1 -type d -print | grep -m1 'lib')/pyarrow ${CMAKE_SOURCE_DIR}/python/ray/pyarrow_files/"
  )

endif ()

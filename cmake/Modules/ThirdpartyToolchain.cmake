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
  string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
  set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}} ${GTEST_CMAKE_CXX_FLAGS}")

  set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
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

  ExternalProject_Add(googletest_ep
    URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
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

  set(GFLAGS_CMAKE_CXX_FLAGS ${EP_CXX_FLAGS})

  set(GFLAGS_URL "https://github.com/gflags/gflags/archive/v${GFLAGS_VERSION}.tar.gz")
  set(GFLAGS_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gflags_ep-prefix/src/gflags_ep")
  set(GFLAGS_HOME "${GFLAGS_PREFIX}")
  set(GFLAGS_INCLUDE_DIR "${GFLAGS_PREFIX}/include")
  if(MSVC)
    set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/gflags_static.lib")
  else()
    set(GFLAGS_STATIC_LIB "${GFLAGS_PREFIX}/lib/libgflags.a")
  endif()
  set(GFLAGS_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                        -DCMAKE_INSTALL_PREFIX=${GFLAGS_PREFIX}
                        -DBUILD_SHARED_LIBS=OFF
                        -DBUILD_STATIC_LIBS=ON
                        -DBUILD_PACKAGING=OFF
                        -DBUILD_TESTING=OFF
                        -BUILD_CONFIG_TESTS=OFF
                        -DINSTALL_HEADERS=ON
                        -DCMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_CXX_FLAGS}
                        -DCMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}=${EP_C_FLAGS}
                        -DCMAKE_CXX_FLAGS=${GFLAGS_CMAKE_CXX_FLAGS})

  ExternalProject_Add(gflags_ep
    URL ${GFLAGS_URL}
    ${EP_LOG_OPTIONS}
    BUILD_IN_SOURCE 1
    BUILD_BYPRODUCTS "${GFLAGS_STATIC_LIB}"
    CMAKE_ARGS ${GFLAGS_CMAKE_ARGS})

  message(STATUS "GFlags include dir: ${GFLAGS_INCLUDE_DIR}")
  message(STATUS "GFlags static library: ${GFLAGS_STATIC_LIB}")
  include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
  ADD_THIRDPARTY_LIB(gflags
    STATIC_LIB ${GFLAGS_STATIC_LIB})

  add_dependencies(gflags gflags_ep)
endif()

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost COMPONENTS system filesystem REQUIRED)
include_directories(${Boost_INCLUDE_DIR})

if(RAY_USE_GLOG)
  message(STATUS "Starting to build glog")
  set(GLOG_VERSION "0.3.5")
  set(GLOG_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
  if(APPLE)
    set(GLOG_CMAKE_CXX_FLAGS "${GLOG_CMAKE_CXX_FLAGS} -mmacosx-version-min=10.12")
  endif()

  set(GLOG_URL "https://github.com/google/glog/archive/v${GLOG_VERSION}.tar.gz")
  set(GLOG_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/glog_ep-prefix/src/glog_ep")
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
    URL ${GLOG_URL}
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

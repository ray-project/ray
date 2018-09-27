# glog external project
# target:
#  - glog_ep
# defines:
#  - GLOG_HOME
#  - GLOG_INCLUDE_DIR
#  - GLOG_STATIC_LIB

if(DEFINED ENV{GLOG_HOME} AND EXISTS $ENV{GLOG_HOME})
  set(GLOG_HOME "$ENV{GLOG_HOME}")
  set(GLOG_INCLUDE_DIR "${GLOG_HOME}/include")
  set(GLOG_STATIC_LIB "${GLOG_HOME}/lib/libglog.a")

  add_custom_target(glog_ep)
else()
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
endif()

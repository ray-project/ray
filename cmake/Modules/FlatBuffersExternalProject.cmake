# flatbuffers external project
# target:
#  - flatbuffer_ep
# depends:
#
# defines:
#  - FLATBUFFERS_HOME
#  - FLATBUFFERS_INCLUDE_DIR
#  - FLATBUFFERS_STATIC_LIB
#  - FLATBUFFERS_COMPILER
#  - FBS_DEPENDS, to keep compatible

if(DEFINED ENV{FLATBUFFERS_HOME} AND EXISTS $ENV{FLATBUFFERS_HOME})
  set(FLATBUFFERS_HOME "$ENV{FLATBUFFERS_HOME}")
  set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_HOME}/include")
  set(FLATBUFFERS_STATIC_LIB "${FLATBUFFERS_HOME}/lib${LIB_SUFFIX}/libflatbuffers.a")
  set(FLATBUFFERS_COMPILER "${FLATBUFFERS_HOME}/bin/flatc")

  add_custom_target(flatbuffers_ep)
else()
  set(flatbuffers_VERSION "1.9.0")
  set(flatbuffers_URL "https://github.com/google/flatbuffers/archive/v${flatbuffers_VERSION}.tar.gz")
  set(flatbuffers_URL_MD5 "8be7513bf960034f6873326d09521a4b")

  set(FLATBUFFERS_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/flatbuffers-install")

  set(FLATBUFFERS_HOME "${FLATBUFFERS_INSTALL_PREFIX}")
  set(FLATBUFFERS_INCLUDE_DIR "${FLATBUFFERS_INSTALL_PREFIX}/include")
  set(FLATBUFFERS_STATIC_LIB "${FLATBUFFERS_INSTALL_PREFIX}/lib${LIB_SUFFIX}/libflatbuffers.a")
  set(FLATBUFFERS_COMPILER "${FLATBUFFERS_INSTALL_PREFIX}/bin/flatc")

  ExternalProject_Add(flatbuffers_ep
    PREFIX external/flatbuffers
    URL ${flatbuffers_URL}
    URL_MD5 ${flatbuffers_URL_MD5}
    CMAKE_ARGS
    "-DCMAKE_CXX_FLAGS=-fPIC"
    "-DCMAKE_INSTALL_PREFIX:PATH=${FLATBUFFERS_INSTALL_PREFIX}"
    "-DFLATBUFFERS_BUILD_TESTS=OFF"
    "-DCMAKE_BUILD_TYPE=RELEASE")
endif()

set(FBS_DEPENDS flatbuffers_ep)

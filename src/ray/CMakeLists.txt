cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++11")

# TODO(pcm): Remove this
option(RAY_BUILD_CORE "Build the ray core separately" OFF)
if (RAY_BUILD_CORE)
  set(ARROW_DIR "${CMAKE_CURRENT_LIST_DIR}/../thirdparty/arrow/"
      CACHE STRING "Path of the arrow source directory")
  include_directories("${ARROW_DIR}/cpp/src/")
  include_directories("${CMAKE_CURRENT_LIST_DIR}/../")

  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/../../cmake/Modules")
  option(RAY_BUILD_STATIC "Build the libray static libraries" ON)
  option(RAY_BUILD_SHARED "Build the libray shared libraries" ON)
  option(RAY_BUILD_TESTS "Build the Ray googletest unit tests" ON)
  include(ExternalProject)
  include(GNUInstallDirs)
  include(BuildUtils)
  enable_testing()
  include(ThirdpartyToolchain)
endif()
# TODO(pcm): End of "Remove this"

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../thirdparty/arrow/python/cmake_modules)
find_package(Plasma)
find_package(Arrow)

include_directories(${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/ae)

add_subdirectory(util)
add_subdirectory(gcs)

include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

set(AE_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/ae/ae.c
)

set(HIREDIS_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/hiredis/async.c
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/hiredis/dict.c
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/hiredis/hiredis.c
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/hiredis/net.c
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/hiredis/read.c
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/hiredis/sds.c
)

set(RAY_SRCS
  id.cc
  status.cc
  gcs/client.cc
  gcs/tables.cc
  gcs/redis_context.cc
)

install(FILES
  api.h
  id.h
  status.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/ray")

# pkg-config support
configure_file(ray.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/ray.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/ray.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

ADD_RAY_LIB(ray
    SOURCES ${RAY_SRCS} ${AE_SRCS} ${HIREDIS_SRCS}
    DEPENDENCIES gen_gcs_fbs
    SHARED_LINK_LIBS
    STATIC_LINK_LIBS ${ARROW_DIR}/cpp/build/release/libarrow.a ${PLASMA_STATIC_LIB})

set(RAY_TEST_LINK_LIBS ${ARROW_DIR}/cpp/build/release/libarrow.a ${PLASMA_STATIC_LIB} ray_static gtest gtest_main)

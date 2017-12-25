cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++11")

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
    SHARED_LINK_LIBS ""
    STATIC_LINK_LIBS "")

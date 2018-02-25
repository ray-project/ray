cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_FLAGS "-g -Wall -Werror -std=c++11")

include_directories(${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/ae)

add_subdirectory(util)
add_subdirectory(gcs)
add_subdirectory(om)
add_subdirectory(raylet)
# add_subdirectory(sandbox)

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

set(UTIL_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/sha256.c
  ${CMAKE_CURRENT_LIST_DIR}/../common/common_protocol.cc
)

set(RAY_SRCS
  id.cc
  status.cc
  gcs/client.cc
  gcs/tables.cc
  gcs/redis_context.cc
  gcs/asio.cc
  om/object_store_client.cc
  om/object_directory.cc
  om/object_manager.cc
  raylet/mock_gcs_client.cc
  raylet/Task.cc
  raylet/TaskExecutionSpecification.cc
  raylet/TaskSpecification.cc
  raylet/client_connection.cc
  raylet/Worker.cc
  raylet/WorkerPool.cc
  raylet/LsResources.cc
  raylet/ActorInformation.cc
  raylet/LsQueue.cc
  raylet/LsPolicy.cc
  raylet/task_dependency_manager.cc
  raylet/reconstruction_policy.cc
  raylet/local_scheduler.cc
  raylet/node_manager.cc
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
  SOURCES ${RAY_SRCS} ${AE_SRCS} ${HIREDIS_SRCS} ${UTIL_SRCS}
    DEPENDENCIES gen_gcs_fbs gen_om_fbs gen_nm_fbs
    SHARED_LINK_LIBS ""
    STATIC_LINK_LIBS ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB})

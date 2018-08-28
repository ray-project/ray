cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Werror -std=c++11")

include_directories(${CMAKE_CURRENT_LIST_DIR}/../common/thirdparty/ae)

add_subdirectory(util)
add_subdirectory(gcs)
add_subdirectory(object_manager)
add_subdirectory(raylet)

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
  gcs/task_table.cc
  gcs/redis_context.cc
  gcs/asio.cc
  util/logging.cc
  util/signal_handler.cc
  common/client_connection.cc
  object_manager/object_manager_client_connection.cc
  object_manager/connection_pool.cc
  object_manager/object_buffer_pool.cc
  object_manager/object_store_notification_manager.cc
  object_manager/object_directory.cc
  object_manager/object_manager.cc
  raylet/monitor.cc
  raylet/mock_gcs_client.cc
  raylet/task.cc
  raylet/task_execution_spec.cc
  raylet/task_spec.cc
  raylet/worker.cc
  raylet/worker_pool.cc
  raylet/scheduling_resources.cc
  raylet/actor_registration.cc
  raylet/scheduling_queue.cc
  raylet/scheduling_policy.cc
  raylet/task_dependency_manager.cc
  raylet/reconstruction_policy.cc
  raylet/node_manager.cc
  raylet/lineage_cache.cc
  raylet/raylet.cc
)

set(RAY_LIB_STATIC_LINK_LIBS ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB})
set(RAY_LIB_DEPENDENCIES gen_gcs_fbs gen_object_manager_fbs gen_node_manager_fbs gen_local_scheduler_fbs ${COMMON_FBS_OUTPUT_FILES})

if(RAY_USE_GLOG)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DRAY_USE_GLOG")
  set(RAY_LIB_STATIC_LINK_LIBS ${RAY_LIB_STATIC_LINK_LIBS} ${GLOG_STATIC_LIB})
  set(RAY_LIB_DEPENDENCIES ${RAY_LIB_DEPENDENCIES} glog)
endif()

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
    # TODO: When raylet replaces the old backend, please remove the dependency gen_local_scheduler_fbs.
    # TODO: When remove the included Task.h from table.h, please remove the dependency gen_common_python_fbs.
    DEPENDENCIES ${RAY_LIB_DEPENDENCIES}
    SHARED_LINK_LIBS ""
    STATIC_LINK_LIBS ${RAY_LIB_STATIC_LINK_LIBS})

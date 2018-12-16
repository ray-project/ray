cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Werror -std=c++11")

add_subdirectory(util)
add_subdirectory(gcs)
add_subdirectory(object_manager)
add_subdirectory(raylet)

include_directories(thirdparty/ae)

set(HIREDIS_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/async.c
  ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/dict.c
  ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/hiredis.c
  ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/net.c
  ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/read.c
  ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/sds.c
)

set(UTIL_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/thirdparty/sha256.c
  ${CMAKE_CURRENT_LIST_DIR}/common/common_protocol.cc
)

set(RAY_SRCS
  id.cc
  status.cc
  gcs/client.cc
  gcs/tables.cc
  gcs/redis_context.cc
  gcs/asio.cc
  util/logging.cc
  common/client_connection.cc
  common/common_protocol.cc
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

set(RAY_LIB_STATIC_LINK_LIBS ${PLASMA_STATIC_LIB} ${ARROW_STATIC_LIB} ${Boost_THREAD_LIBRARY})
set(RAY_LIB_DEPENDENCIES
    boost_thread
    arrow_ep
    gen_gcs_fbs
    gen_object_manager_fbs
    gen_node_manager_fbs)

if(RAY_USE_GLOG)
  add_definitions(-DRAY_USE_GLOG)
  set(RAY_LIB_STATIC_LINK_LIBS ${RAY_LIB_STATIC_LINK_LIBS} glog)
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
  SOURCES ${RAY_SRCS} ${HIREDIS_SRCS} ${UTIL_SRCS}
    DEPENDENCIES ${RAY_LIB_DEPENDENCIES}
    SHARED_LINK_LIBS ""
    STATIC_LINK_LIBS ${RAY_LIB_STATIC_LINK_LIBS})

add_custom_target(copy_redis ALL)
foreach(file "redis-cli" "redis-server")
add_custom_command(TARGET copy_redis POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E
                      copy ${CMAKE_CURRENT_LIST_DIR}/../../thirdparty/pkg/redis/src/${file}
                           ${CMAKE_BINARY_DIR}/src/ray/thirdparty/redis/src/${file})
endforeach()

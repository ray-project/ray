cmake_minimum_required(VERSION 2.8)

project(common)

include(${CMAKE_CURRENT_LIST_DIR}/cmake/Common.cmake)

add_subdirectory(redis_module)

include_directories(thirdparty/ae)

add_custom_target(
   hiredis
   COMMAND make
   WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis)

add_library(common STATIC
  event_loop.c
  common.c
  task.c
  io.c
  net.c
  logging.c
  state/redis.c
  state/table.c
  state/object_table.c
  state/task_table.c
  state/db_client_table.c
  state/actor_notification_table.c
  state/local_scheduler_table.c
  thirdparty/ae/ae.c
  thirdparty/sha256.c)

target_link_libraries(common "${CMAKE_CURRENT_LIST_DIR}/thirdparty/hiredis/libhiredis.a")

function(define_test test_name library)
  add_executable(${test_name} test/${test_name}.c ${ARGN})
  add_dependencies(${test_name} hiredis flatcc)
  target_link_libraries(${test_name} common ${FLATBUFFERS_STATIC_LIB} ${library})
  target_compile_options(${test_name} PUBLIC "-DPLASMA_TEST -DPHOTON_TEST -DCOMMON_TEST -DRAY_COMMON_LOG_LEVEL=4 -DRAY_TIMEOUT=50")
endfunction()

define_test(common_tests "")
define_test(db_tests "")
define_test(io_tests "")
define_test(task_tests "")
define_test(redis_tests "")
define_test(task_table_tests "")
define_test(object_table_tests "")

add_custom_target(
   redis
   COMMAND make
   WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/thirdparty/redis)

add_custom_target(copy_redis ALL)
foreach(file "redis-cli" "redis-server")
add_custom_command(TARGET copy_redis POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E
                      copy ${CMAKE_CURRENT_LIST_DIR}/thirdparty/redis/src/${file}
                           ${CMAKE_BINARY_DIR}/src/common/thirdparty/redis/src/${file})
endforeach()
add_dependencies(copy_redis redis)
